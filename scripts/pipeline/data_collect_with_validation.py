#!/usr/bin/env python3
"""
带GE验证的数据采集脚本

功能：
1. 采集数据
2. GE验证数据质量
3. 验证失败自动重试
4. 记录验证结果

使用方式:
    python scripts/pipeline/data_collect_with_validation.py --date 2026-04-24
    python scripts/pipeline/data_collect_with_validation.py --retry-failed
    python scripts/pipeline/data_collect_with_validation.py --codes 000001,000002 --concurrent 10
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import asyncio
import json

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import pandas as pd
from services.data_service.fetchers.async_kline_fetcher import AsyncKlineFetcher, AsyncConfig, FetchResult
from services.data_service.fetchers.stock_list_cache import StockListCacheManager
from services.data_service.quality.gx_validator import GreatExpectationsValidator, ValidationSuiteResult
from scripts.pipeline.progress_helper import ProgressReporter


# 全局单例验证器
_kline_validator: Optional[GreatExpectationsValidator] = None


def setup_logging(log_dir: Path = None) -> logging.Logger:
    """设置日志，同时输出到控制台和文件"""
    if log_dir is None:
        log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger("collect_with_validation")
    logger.setLevel(logging.INFO)
    
    # 清除已有处理器
    logger.handlers.clear()
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # 文件处理器
    log_file = log_dir / f"collect_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    logger.info(f"日志文件: {log_file}")
    return logger


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='带GE验证的数据采集')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='目标日期 YYYY-MM-DD（默认今天）'
    )
    parser.add_argument(
        '--codes',
        type=str,
        default=None,
        help='股票代码列表，逗号分隔（如：000001,000002）'
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='最大重试次数（默认3）'
    )
    parser.add_argument(
        '--min-success-rate',
        type=float,
        default=0.95,
        help='最小成功率（默认0.95）'
    )
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='仅重试之前失败的采集'
    )
    parser.add_argument(
        '--concurrent',
        type=int,
        default=10,
        help='并发采集数量（默认10）'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='每批处理的股票数（默认50）'
    )
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.5,
        help='请求间隔秒数（默认0.5）'
    )
    parser.add_argument(
        '--kline-dir',
        type=str,
        default='data/kline',
        help='K线数据保存目录（默认data/kline）'
    )
    parser.add_argument(
        '--auto-update-list',
        action='store_true',
        help='股票列表过期时自动更新（默认关闭）'
    )
    return parser.parse_args()


def get_kline_validator() -> GreatExpectationsValidator:
    """获取全局单例K线数据GE验证器"""
    global _kline_validator
    if _kline_validator is None:
        _kline_validator = GreatExpectationsValidator()
        
        # 基本列存在性验证（使用实际的列名 trade_date）
        _kline_validator.expect_column_to_exist('code')
        _kline_validator.expect_column_to_exist('trade_date')
        _kline_validator.expect_column_to_exist('open')
        _kline_validator.expect_column_to_exist('high')
        _kline_validator.expect_column_to_exist('low')
        _kline_validator.expect_column_to_exist('close')
        _kline_validator.expect_column_to_exist('volume')
        
        # 非空验证
        _kline_validator.expect_column_values_to_not_be_null('code')
        _kline_validator.expect_column_values_to_not_be_null('trade_date')
        _kline_validator.expect_column_values_to_not_be_null('open', mostly=0.99)
        _kline_validator.expect_column_values_to_not_be_null('close', mostly=0.99)
        
        # 数值范围验证
        _kline_validator.expect_column_values_to_be_between('open', min_value=0, max_value=10000)
        _kline_validator.expect_column_values_to_be_between('high', min_value=0, max_value=10000)
        _kline_validator.expect_column_values_to_be_between('low', min_value=0, max_value=10000)
        _kline_validator.expect_column_values_to_be_between('close', min_value=0, max_value=10000)
        _kline_validator.expect_column_values_to_be_between('volume', min_value=0, max_value=1e12)
        
        # OHLC逻辑验证
        _kline_validator.expect_ohlc_logic(mostly=0.99)
    
    return _kline_validator


def validate_stock_data(code: str, kline_dir: Path) -> Tuple[bool, Optional[ValidationSuiteResult]]:
    """
    验证单只股票的数据质量（使用全局单例验证器）
    
    Returns:
        (是否通过, 验证结果)
    """
    try:
        kline_file = kline_dir / f"{code}.parquet"
        if not kline_file.exists():
            return False, None
        
        # 读取数据
        df = pl.read_parquet(kline_file).to_pandas()
        
        if df.empty:
            return False, None
        
        # 执行GE验证（使用全局单例验证器）
        validator = get_kline_validator()
        result = validator.validate(df, suite_name=f"kline_{code}")
        
        return result.success, result
        
    except Exception as e:
        logger.error(f"验证 {code} 失败: {e}")
        return False, None


class StockCollector:
    """股票采集器（复用 fetcher 实例）"""
    
    def __init__(self, config: AsyncConfig, kline_dir: Path):
        self.config = config
        self.kline_dir = kline_dir
        self.fetcher = AsyncKlineFetcher(config)
        self.logger = logging.getLogger("collect_with_validation")
    
    async def collect_single_stock_with_retry(
        self,
        code: str,
        max_retries: int = 3,
        min_success_rate: float = 0.95
    ) -> Dict:
        """
        采集单只股票（带验证和重试）
        
        Returns:
            {
                'code': 股票代码,
                'success': 是否成功,
                'attempts': 尝试次数,
                'validation_passed': 验证是否通过,
                'rows': 数据行数
            }
        """
        for attempt in range(1, max_retries + 1):
            self.logger.info(f"🔄 {code}: 第 {attempt}/{max_retries} 次尝试采集")
            
            try:
                # 1. 采集数据（复用 self.fetcher）
                result = await self.fetcher.fetch_single_stock(code, self.kline_dir, days=365*3)
                
                if not result.success:
                    self.logger.warning(f"⚠️ {code}: 采集失败 - {result.status}")
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)  # 指数退避
                        continue
                    else:
                        return {
                            'code': code,
                            'success': False,
                            'attempts': attempt,
                            'validation_passed': False,
                            'rows': 0,
                            'error': result.error
                        }
                
                # 2. 验证数据质量
                validation_passed, validation_result = validate_stock_data(code, self.kline_dir)
                
                if validation_passed:
                    self.logger.info(f"✅ {code}: 采集并验证成功 ({result.rows} 行)")
                    return {
                        'code': code,
                        'success': True,
                        'attempts': attempt,
                        'validation_passed': True,
                        'rows': result.rows
                    }
                else:
                    # 验证失败，检查GE验证成功率
                    if validation_result:
                        ge_success_rate = validation_result.success_rate
                        self.logger.warning(f"⚠️ {code}: GE验证成功率 {ge_success_rate:.1%}")
                        
                        # GE验证成功率达标，视为成功（但标记验证未通过）
                        if ge_success_rate >= min_success_rate:
                            self.logger.info(f"✅ {code}: GE验证成功率达标，视为成功")
                            return {
                                'code': code,
                                'success': True,
                                'attempts': attempt,
                                'validation_passed': False,
                                'rows': result.rows,
                                'ge_success_rate': ge_success_rate
                            }
                    
                    # 需要重试
                    if attempt < max_retries:
                        self.logger.info(f"🔄 {code}: 验证不通过，准备重试...")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        return {
                            'code': code,
                            'success': False,
                            'attempts': attempt,
                            'validation_passed': False,
                            'rows': result.rows,
                            'error': '验证失败且重试次数用尽'
                        }
            
            except Exception as e:
                self.logger.error(f"❌ {code}: 采集异常 - {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return {
                        'code': code,
                        'success': False,
                        'attempts': attempt,
                        'validation_passed': False,
                        'rows': 0,
                        'error': str(e)
                    }
        
        return {
            'code': code,
            'success': False,
            'attempts': max_retries,
            'validation_passed': False,
            'rows': 0,
            'error': '重试次数用尽'
        }


async def collect_stocks_with_validation(
    codes: List[str],
    max_retries: int = 3,
    min_success_rate: float = 0.95,
    concurrent: int = 10,
    batch_size: int = 50,
    request_delay: float = 0.5,
    kline_dir: Path = None
) -> Dict:
    """
    批量采集股票（带验证和重试）- 并行版本
    
    Returns:
        采集统计
    """
    logger.info("=" * 60)
    logger.info("🚀 开始带GE验证的数据采集（并行模式）")
    logger.info("=" * 60)
    logger.info(f"股票数: {len(codes)}")
    logger.info(f"最大重试: {max_retries}")
    logger.info(f"最小成功率: {min_success_rate:.0%}")
    logger.info(f"并发数: {concurrent}")
    logger.info(f"批次大小: {batch_size}")
    
    if kline_dir is None:
        kline_dir = Path("data/kline")
    kline_dir.mkdir(exist_ok=True)
    
    # 创建采集器配置
    config = AsyncConfig(
        max_concurrent=concurrent,
        semaphore_value=concurrent,
        batch_size=batch_size,
        batch_pause=1.0,  # 批次间暂停1秒
        request_delay=request_delay,
        min_kline_rows=50
    )
    
    # 创建采集器（复用 fetcher）
    collector = StockCollector(config, kline_dir)
    
    results = []
    total = len(codes)
    
    # 批量并行处理
    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch_codes = codes[batch_start:batch_end]
        
        logger.info(f"\n📦 处理批次 {batch_start//batch_size + 1}/{(total-1)//batch_size + 1} [{batch_start}:{batch_end}]")
        
        # 创建并发任务
        tasks = [
            collector.collect_single_stock_with_retry(
                code, max_retries, min_success_rate
            )
            for code in batch_codes
        ]
        
        # 并行执行
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        for code, result in zip(batch_codes, batch_results):
            if isinstance(result, Exception):
                logger.error(f"❌ {code}: 任务异常 - {result}")
                results.append({
                    'code': code,
                    'success': False,
                    'attempts': 0,
                    'validation_passed': False,
                    'rows': 0,
                    'error': str(result)
                })
            else:
                results.append(result)
        
        # 输出进度
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"进度: {len(results)}/{total}, 成功: {success_count}/{len(results)}")
        
        # 批次间暂停（避免数据源限流）
        if batch_end < total:
            await asyncio.sleep(1.0)
    
    # 统计结果
    success_count = sum(1 for r in results if r['success'])
    validation_passed = sum(1 for r in results if r.get('validation_passed', False))
    total_rows = sum(r['rows'] for r in results)
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 采集完成")
    logger.info("=" * 60)
    logger.info(f"总股票: {len(codes)}")
    logger.info(f"✅ 成功: {success_count}")
    logger.info(f"✅ 验证通过: {validation_passed}")
    logger.info(f"❌ 失败: {len(codes) - success_count}")
    logger.info(f"📈 总行数: {total_rows}")
    
    return {
        'total': len(codes),
        'success': success_count,
        'validation_passed': validation_passed,
        'failed': len(codes) - success_count,
        'total_rows': total_rows,
        'details': results
    }


def load_failed_stocks() -> List[str]:
    """加载之前失败的采集记录"""
    failed_file = Path("data/failed_collections.json")
    if failed_file.exists():
        try:
            with open(failed_file, 'r') as f:
                data = json.load(f)
                return data.get('failed_codes', [])
        except Exception as e:
            logger.error(f"读取失败记录失败: {e}")
    return []


def save_failed_stocks(failed_codes: List[str]):
    """保存失败的采集记录"""
    failed_file = Path("data/failed_collections.json")
    failed_file.parent.mkdir(exist_ok=True)
    
    try:
        with open(failed_file, 'w') as f:
            json.dump({
                'failed_codes': failed_codes,
                'updated_at': datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"失败记录已保存: {failed_file}")
    except Exception as e:
        logger.error(f"保存失败记录失败: {e}")


def generate_report(results: Dict, target_date: str):
    """生成采集报告"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 GE验证数据采集报告")
    logger.info("=" * 60)
    
    report = {
        'target_date': target_date,
        'collection_time': datetime.now().isoformat(),
        'summary': {
            'total': results['total'],
            'success': results['success'],
            'validation_passed': results['validation_passed'],
            'failed': results['failed'],
            'total_rows': results['total_rows'],
            'success_rate': results['success'] / results['total'] if results['total'] > 0 else 0
        },
        'details': results['details']
    }
    
    # 保存报告
    report_path = Path("data/collection_validation_report.json")
    report_path.parent.mkdir(exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"📄 报告已保存: {report_path}")
    
    # 保存失败记录
    failed_codes = [r['code'] for r in results['details'] if not r['success']]
    if failed_codes:
        save_failed_stocks(failed_codes)


async def main():
    """主函数"""
    global logger
    
    args = parse_args()
    
    # 设置日志
    logger = setup_logging()
    reporter = ProgressReporter("data_fetch")
    reporter.start("启动数据采集", progress=0)
    
    # 确定目标日期
    if args.date:
        target_date = args.date
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    reporter.update(10, f"完成参数解析，目标日期 {target_date}")
    
    logger.info("=" * 60)
    logger.info("🚀 带GE验证的数据采集")
    logger.info("=" * 60)
    logger.info(f"目标日期: {target_date}")
    
    # 确定股票列表
    if args.retry_failed:
        # 重试之前失败的
        codes = load_failed_stocks()
        logger.info(f"🔄 重试模式: {len(codes)} 只")
    elif args.codes:
        # 指定股票
        codes = args.codes.split(',')
        logger.info(f"📋 指定股票: {len(codes)} 只")
    else:
        # 优先从Redis读取，带新鲜度检查和自动更新
        if args.auto_update_list:
            # 自动更新模式
            cache_manager = StockListCacheManager(auto_update=True)
            codes = cache_manager.get_codes_auto()
            logger.info(f"📋 自动更新模式，获取: {len(codes)} 只")
        else:
            # 普通模式
            cache_manager = StockListCacheManager()
            result = cache_manager.get_codes_with_freshness_check(use_redis=True)

            codes = result['codes']
            freshness = result['freshness']

            if codes:
                source_name = "Redis" if result['source'] == 'redis' else "Parquet"
                logger.info(f"📋 从{source_name}读取: {len(codes)} 只")
                logger.info(f"   数据年龄: {freshness['age_days']} 天")

                # 警告提示
                if freshness.get('warning'):
                    logger.warning(f"⚠️  {freshness['warning']}")
            else:
                logger.error("❌ 无法获取股票列表")
                reporter.fail("无法获取股票列表")
                return 1
    
    if not codes:
        logger.error("❌ 没有股票需要采集")
        reporter.fail("没有股票需要采集")
        return 1
    reporter.update(20, f"完成股票清单准备，共 {len(codes)} 只")
    
    # 执行采集
    kline_dir = Path(args.kline_dir)
    reporter.update(40, f"开始主采集，输出目录 {kline_dir}")
    results = await collect_stocks_with_validation(
        codes,
        max_retries=args.max_retries,
        min_success_rate=args.min_success_rate,
        concurrent=args.concurrent,
        batch_size=args.batch_size,
        request_delay=args.request_delay,
        kline_dir=kline_dir
    )
    reporter.update(
        80,
        "主采集完成，开始生成报告",
        extra={
            "total": results["total"],
            "success": results["success"],
            "failed": results["failed"],
        },
    )
    
    # 生成报告
    generate_report(results, target_date)
    
    # 判断是否成功
    success_rate = results['success'] / results['total'] if results['total'] > 0 else 0
    if success_rate >= 0.8:
        logger.info(f"✅ 采集成功，成功率 {success_rate:.1%}")
        reporter.complete(
            f"数据采集完成，成功率 {success_rate:.1%}",
            extra={
                "total": results["total"],
                "success": results["success"],
                "failed": results["failed"],
                "success_rate": success_rate,
            },
        )
        return 0
    else:
        logger.error(f"❌ 采集失败，成功率 {success_rate:.1%} < 80%")
        reporter.fail(
            f"数据采集失败，成功率 {success_rate:.1%}",
            extra={
                "total": results["total"],
                "success": results["success"],
                "failed": results["failed"],
                "success_rate": success_rate,
            },
        )
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
