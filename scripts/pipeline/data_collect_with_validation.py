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
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import asyncio
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("collect_with_validation")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import pandas as pd
from services.data_service.fetchers.async_kline_fetcher import AsyncKlineFetcher, AsyncConfig, FetchResult
from services.data_service.quality.gx_validator import GreatExpectationsValidator, ValidationSuiteResult


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
    return parser.parse_args()


def get_kline_validator() -> GreatExpectationsValidator:
    """创建K线数据GE验证器"""
    validator = GreatExpectationsValidator()
    
    # 基本列存在性验证
    validator.expect_column_to_exist('code')
    validator.expect_column_to_exist('date')
    validator.expect_column_to_exist('open')
    validator.expect_column_to_exist('high')
    validator.expect_column_to_exist('low')
    validator.expect_column_to_exist('close')
    validator.expect_column_to_exist('volume')
    
    # 非空验证
    validator.expect_column_values_to_not_be_null('code')
    validator.expect_column_values_to_not_be_null('date')
    validator.expect_column_values_to_not_be_null('open', mostly=0.99)
    validator.expect_column_values_to_not_be_null('close', mostly=0.99)
    
    # 数值范围验证
    validator.expect_column_values_to_be_between('open', min_value=0, max_value=10000)
    validator.expect_column_values_to_be_between('high', min_value=0, max_value=10000)
    validator.expect_column_values_to_be_between('low', min_value=0, max_value=10000)
    validator.expect_column_values_to_be_between('close', min_value=0, max_value=10000)
    validator.expect_column_values_to_be_between('volume', min_value=0, max_value=1e12)
    
    # OHLC逻辑验证
    validator.add_custom_expectation('ohlc_logic', lambda df: (
        (df['high'] >= df[['open', 'close']].max(axis=1)) &
        (df['low'] <= df[['open', 'close']].min(axis=1))
    ).all())
    
    return validator


def validate_stock_data(code: str, kline_dir: Path) -> Tuple[bool, ValidationSuiteResult]:
    """
    验证单只股票的数据质量
    
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
        
        # 执行GE验证
        validator = get_kline_validator()
        result = validator.validate(df, suite_name=f"kline_{code}")
        
        return result.success, result
        
    except Exception as e:
        logger.error(f"验证 {code} 失败: {e}")
        return False, None


async def collect_single_stock_with_retry(
    code: str,
    kline_dir: Path,
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
    kline_dir = Path(kline_dir)
    
    config = AsyncConfig(
        max_concurrent=1,
        semaphore_value=1,
        batch_size=1,
        batch_pause=0,
        request_delay=0.5,
        min_kline_rows=50
    )
    fetcher = AsyncKlineFetcher(config)
    
    for attempt in range(1, max_retries + 1):
        logger.info(f"🔄 {code}: 第 {attempt}/{max_retries} 次尝试采集")
        
        try:
            # 1. 采集数据
            result = await fetcher.fetch_single_stock(code, kline_dir, days=365*3)
            
            if not result.success:
                logger.warning(f"⚠️ {code}: 采集失败 - {result.status}")
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
            validation_passed, validation_result = validate_stock_data(code, kline_dir)
            
            if validation_passed:
                logger.info(f"✅ {code}: 采集并验证成功 ({result.rows} 行)")
                return {
                    'code': code,
                    'success': True,
                    'attempts': attempt,
                    'validation_passed': True,
                    'rows': result.rows
                }
            else:
                # 验证失败，检查成功率
                if validation_result:
                    success_rate = validation_result.success_rate
                    logger.warning(f"⚠️ {code}: 验证失败，成功率 {success_rate:.1%}")
                    
                    if success_rate >= min_success_rate:
                        # 成功率达标，视为成功
                        logger.info(f"✅ {code}: 成功率达标，视为成功")
                        return {
                            'code': code,
                            'success': True,
                            'attempts': attempt,
                            'validation_passed': False,
                            'rows': result.rows,
                            'success_rate': success_rate
                        }
                
                # 需要重试
                if attempt < max_retries:
                    logger.info(f"🔄 {code}: 准备重试...")
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
            logger.error(f"❌ {code}: 采集异常 - {e}")
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
    min_success_rate: float = 0.95
) -> Dict:
    """
    批量采集股票（带验证和重试）
    
    Returns:
        采集统计
    """
    logger.info("=" * 60)
    logger.info("🚀 开始带GE验证的数据采集")
    logger.info("=" * 60)
    logger.info(f"股票数: {len(codes)}")
    logger.info(f"最大重试: {max_retries}")
    logger.info(f"最小成功率: {min_success_rate:.0%}")
    
    kline_dir = Path("data/kline")
    kline_dir.mkdir(exist_ok=True)
    
    results = []
    
    # 串行处理（避免并发导致的问题）
    for i, code in enumerate(codes, 1):
        logger.info(f"\n[{i}/{len(codes)}] 处理 {code}")
        
        result = await collect_single_stock_with_retry(
            code, kline_dir, max_retries, min_success_rate
        )
        results.append(result)
        
        # 输出进度
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"进度: {i}/{len(codes)}, 成功: {success_count}/{i}")
    
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
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"📄 报告已保存: {report_path}")
    
    # 保存失败记录
    failed_codes = [r['code'] for r in results['details'] if not r['success']]
    if failed_codes:
        save_failed_stocks(failed_codes)


async def main():
    """主函数"""
    args = parse_args()
    
    # 确定目标日期
    if args.date:
        target_date = args.date
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
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
        # 从文件读取
        try:
            df = pl.read_parquet("data/stock_list.parquet")
            codes = df['code'].to_list()
            logger.info(f"📋 从文件读取: {len(codes)} 只")
        except Exception as e:
            logger.error(f"读取股票列表失败: {e}")
            return 1
    
    if not codes:
        logger.error("❌ 没有股票需要采集")
        return 1
    
    # 执行采集
    results = await collect_stocks_with_validation(
        codes,
        max_retries=args.max_retries,
        min_success_rate=args.min_success_rate
    )
    
    # 生成报告
    generate_report(results, target_date)
    
    # 判断是否成功
    success_rate = results['success'] / results['total'] if results['total'] > 0 else 0
    if success_rate >= 0.8:
        logger.info(f"✅ 采集成功，成功率 {success_rate:.1%}")
        return 0
    else:
        logger.error(f"❌ 采集失败，成功率 {success_rate:.1%} < 80%")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
