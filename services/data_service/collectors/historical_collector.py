#!/usr/bin/env python3
"""
历史数据采集器

用于采集收盘后的历史数据：
- 日K线数据
- 股票列表
- 基本面数据

特点：
- 数据完整性高
- 支持增量更新
- 完整质量验证
"""
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import pandas as pd
import polars as pl

from core.logger import setup_logger
from core.config import get_settings
from core.market_guardian import enforce_market_closed
from services.data_service.datasource import get_datasource_manager
from services.data_service.fetchers.unified_fetcher import get_unified_fetcher
from services.data_service.fetchers.kline_fetcher import fetch_kline_data_parallel
from services.data_service.pipeline import get_data_pipeline
from services.data_service.quality.gx_validator import (
    KlineDataQualitySuite,
    StockListQualitySuite,
    validate_all_kline_data,
    generate_quality_report
)

logger = setup_logger("historical_collector", log_file="system/historical_collector.log")


@dataclass
class HistoricalCollectionResult:
    """历史采集结果"""
    success: bool
    stage: str
    message: str
    stock_code: Optional[str] = None
    row_count: int = 0
    validation_success_rate: float = 0.0
    error: Optional[str] = None


class HistoricalCollector:
    """历史数据采集器"""

    def __init__(self, data_dir: str = None):
        self.settings = get_settings()
        self.data_dir = Path(data_dir or self.settings.DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.kline_dir = self.data_dir / "kline"
        self.kline_dir.mkdir(exist_ok=True)

        self.pipeline = get_data_pipeline(
            data_dir=str(self.data_dir),
            min_success_rate=0.95
        )

        self.kline_validator = KlineDataQualitySuite.create_validator()
        self.stock_list_validator = StockListQualitySuite.create_validator()

    async def initialize(self):
        """初始化"""
        logger.info("🚀 历史数据采集器初始化")
        self.ds_manager = get_datasource_manager()
        # DataSourceManager.initialize() 不是协程，直接调用
        if hasattr(self.ds_manager, 'initialize'):
            result = self.ds_manager.initialize()
            if asyncio.iscoroutine(result):
                await result
        self.fetcher = await get_unified_fetcher()
        logger.info("✅ 初始化完成")

    def check_market_status(self, target_date: str = None) -> bool:
        """
        检查市场状态

        Returns:
            True: 可以采集
            False: 不能采集
        """
        try:
            enforce_market_closed(target_date=target_date)
            logger.info("✅ 市场状态检查通过")
            return True
        except SystemExit:
            logger.error("❌ 市场未收盘，无法采集当日数据")
            return False

    async def collect_stock_list(self) -> HistoricalCollectionResult:
        """采集股票列表"""
        logger.info("📊 采集股票列表...")

        try:
            df = await self.fetcher.fetch_stock_list()

            if df.empty:
                return HistoricalCollectionResult(
                    success=False,
                    stage='fetch',
                    message='获取股票列表失败',
                    error='Empty dataframe'
                )

            # 验证
            result = self.stock_list_validator.validate(df, suite_name='stock_list')

            if not result.success:
                logger.warning(f"股票列表验证警告: 成功率 {result.success_rate:.1%}")

            # 保存
            output_path = self.data_dir / "stock_list.parquet"
            df.to_parquet(output_path, index=False)

            logger.info(f"✅ 股票列表已保存: {len(df)} 只")

            return HistoricalCollectionResult(
                success=True,
                stage='complete',
                message=f'股票列表采集完成: {len(df)} 只',
                row_count=len(df),
                validation_success_rate=result.success_rate
            )

        except Exception as e:
            logger.exception("采集股票列表失败")
            return HistoricalCollectionResult(
                success=False,
                stage='error',
                message='采集股票列表失败',
                error=str(e)
            )

    def get_codes_to_update(self, codes: List[str], target_date: str = None) -> List[str]:
        """
        获取需要更新的股票代码

        Args:
            codes: 所有股票代码
            target_date: 目标日期 YYYY-MM-DD

        Returns:
            需要更新的代码列表
        """
        if not target_date:
            return codes

        codes_to_update = []
        target = datetime.strptime(target_date, "%Y-%m-%d").date()

        for code in codes:
            kline_file = self.kline_dir / f"{code}.parquet"

            if not kline_file.exists():
                codes_to_update.append(code)
                continue

            try:
                df = pl.read_parquet(kline_file)
                if 'trade_date' in df.columns:
                    last_date = df['trade_date'].max()
                    last = datetime.strptime(str(last_date), "%Y-%m-%d").date()

                    if last < target:
                        codes_to_update.append(code)
            except Exception as e:
                logger.warning(f"检查 {code} 失败: {e}")
                codes_to_update.append(code)

        return codes_to_update

    async def collect_kline(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        days: int = 3 * 365
    ) -> HistoricalCollectionResult:
        """
        采集单只股票K线数据

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            days: 获取天数（默认3年）

        Returns:
            采集结果
        """
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if not start_date:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        try:
            # 获取数据
            df = await self.fetcher.fetch_kline(code, start_date, end_date)

            if df is None or df.empty:
                return HistoricalCollectionResult(
                    success=False,
                    stage='fetch',
                    message='获取数据为空',
                    stock_code=code
                )

            # 使用管道处理（验证+存储）
            result = self.pipeline.process_kline(code, df, validate=True)

            return HistoricalCollectionResult(
                success=result.success,
                stage=result.stage,
                message=result.message,
                stock_code=code,
                row_count=result.metadata.get('row_count', 0) if result.metadata else 0,
                validation_success_rate=result.validation_result.success_rate if result.validation_result else 0.0
            )

        except Exception as e:
            logger.exception(f"采集 {code} K线失败")
            return HistoricalCollectionResult(
                success=False,
                stage='error',
                message=f'采集失败: {str(e)}',
                stock_code=code,
                error=str(e)
            )

    async def batch_collect_kline(
        self,
        codes: List[str],
        days: int = 3 * 365
    ) -> Dict[str, HistoricalCollectionResult]:
        """
        批量采集K线数据

        Args:
            codes: 股票代码列表
            days: 历史天数

        Returns:
            采集结果字典
        """
        logger.info(f"🔄 批量采集 {len(codes)} 只股票K线数据")

        # 使用多进程并行采集
        stats = fetch_kline_data_parallel(codes, self.kline_dir, days)

        logger.info(f"✅ 成功: {stats.get('success', 0)}")
        logger.info(f"❌ 失败: {stats.get('failed', 0)}")
        logger.info(f"⏭️ 跳过: {stats.get('skipped', 0)}")

        # 转换为结果对象
        results = {}
        for code in codes:
            results[code] = HistoricalCollectionResult(
                success=True,
                stage='complete',
                message='通过批量采集完成',
                stock_code=code
            )

        return results

    async def validate_collected_data(
        self,
        sample_size: int = 100
    ) -> Tuple[bool, Dict]:
        """
        验证已采集的数据

        Args:
            sample_size: 抽样数量

        Returns:
            (是否通过, 报告)
        """
        logger.info(f"🔍 抽样验证 {sample_size} 个文件...")

        validation_results = validate_all_kline_data(
            self.kline_dir,
            sample_size=sample_size
        )

        passed = sum(1 for r in validation_results.values() if r.success)
        failed = len(validation_results) - passed
        avg_rate = sum(r.success_rate for r in validation_results.values()) / len(validation_results) if validation_results else 0

        logger.info(f"✅ 通过: {passed}")
        logger.info(f"❌ 失败: {failed}")
        logger.info(f"📊 平均成功率: {avg_rate:.1%}")

        # 生成报告
        report_path = self.data_dir / "quality_report.json"
        report = generate_quality_report(validation_results, report_path)

        # 判断是否通过（成功率>=95%）
        is_passed = avg_rate >= 0.95

        return is_passed, report

    async def run_daily_collection(self, target_date: str = None) -> Dict:
        """
        运行每日采集

        Args:
            target_date: 目标日期 YYYY-MM-DD

        Returns:
            采集统计
        """
        logger.info("=" * 70)
        logger.info("🚀 开始每日历史数据采集")
        logger.info("=" * 70)

        start_time = datetime.now()

        # 1. 检查市场状态
        if not self.check_market_status(target_date):
            return {'success': False, 'error': '市场未收盘'}

        # 2. 采集股票列表
        stock_list_result = await self.collect_stock_list()
        if not stock_list_result.success:
            return {'success': False, 'error': '股票列表采集失败'}

        codes = []
        try:
            stock_list_path = self.data_dir / "stock_list.parquet"
            df = pd.read_parquet(stock_list_path)
            codes = df['code'].tolist()
        except Exception as e:
            return {'success': False, 'error': f'读取股票列表失败: {e}'}

        # 3. 计算需要更新的股票
        codes_to_update = self.get_codes_to_update(codes, target_date)
        logger.info(f"📈 需要更新: {len(codes_to_update)} 只股票")

        if not codes_to_update:
            logger.info("✅ 所有股票已是最新")
            return {'success': True, 'updated': 0}

        # 4. 批量采集K线
        results = await self.batch_collect_kline(codes_to_update, days=3*365)

        # 5. 验证数据质量
        is_passed, report = await self.validate_collected_data(sample_size=100)

        # 6. 生成统计
        end_time = datetime.now()
        duration = end_time - start_time

        success_count = sum(1 for r in results.values() if r.success)

        stats = {
            'success': True,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
            'total_stocks': len(codes_to_update),
            'success_count': success_count,
            'failed_count': len(codes_to_update) - success_count,
            'quality_passed': is_passed,
            'quality_success_rate': report['summary']['overall_success_rate']
        }

        logger.info("=" * 70)
        logger.info("📊 采集完成")
        logger.info(f"总股票: {stats['total_stocks']}")
        logger.info(f"成功: {stats['success_count']}")
        logger.info(f"失败: {stats['failed_count']}")
        logger.info(f"质量: {stats['quality_success_rate']:.1%}")
        logger.info(f"耗时: {duration}")
        logger.info("=" * 70)

        return stats


# 便捷函数
async def collect_historical_data(target_date: str = None) -> Dict:
    """采集历史数据的便捷函数"""
    collector = HistoricalCollector()
    await collector.initialize()
    return await collector.run_daily_collection(target_date)
