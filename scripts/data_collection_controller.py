#!/usr/bin/env python3
"""
数据采集主控器

统一的数据采集入口，协调各组件完成：
1. 股票列表更新
2. K线数据增量更新
3. 实时数据获取
4. 数据质量验证
5. 采集报告生成

使用方式:
    python scripts/data_collection_controller.py --mode daily
    python scripts/data_collection_controller.py --mode full
    python scripts/data_collection_controller.py --mode incremental --codes 000001,000002
"""
import sys
import argparse
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import json

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.logger import setup_logger
from core.config import get_settings
from core.market_guardian import enforce_market_closed
from services.data_service.datasource import get_datasource_manager
from services.data_service.fetchers.unified_fetcher import UnifiedFetcher, get_unified_fetcher
from services.data_service.fetchers.kline_fetcher import fetch_kline_data_parallel, Config as KlineConfig
from services.data_service.fetchers.async_kline_fetcher import fetch_kline_data_async, AsyncKlineFetcher, AsyncConfig
from services.data_service.pipeline import get_data_pipeline, PipelineResult
from services.data_service.quality.gx_validator import validate_all_kline_data, generate_quality_report

# 设置日志
logger = setup_logger("collection_controller", log_file="system/collection_controller.log")


@dataclass
class CollectionStats:
    """采集统计"""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_stocks: int = 0
    success_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    validation_passed: int = 0
    validation_failed: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def duration(self) -> timedelta:
        """采集耗时"""
        end = self.end_time or datetime.now()
        return end - self.start_time

    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_stocks == 0:
            return 0.0
        return self.success_count / self.total_stocks

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration.total_seconds(),
            'total_stocks': self.total_stocks,
            'success_count': self.success_count,
            'failed_count': self.failed_count,
            'skipped_count': self.skipped_count,
            'validation_passed': self.validation_passed,
            'validation_failed': self.validation_failed,
            'success_rate': self.success_rate,
            'errors': self.errors
        }


class DataCollectionController:
    """数据采集主控器"""

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
        self.stats = CollectionStats()

    async def initialize(self):
        """初始化"""
        logger.info("=" * 70)
        logger.info("🚀 数据采集主控器初始化")
        logger.info("=" * 70)

        # 初始化数据源管理器
        self.ds_manager = get_datasource_manager()
        self.ds_manager.initialize()  # 不是异步方法

        # 初始化统一获取器
        self.fetcher = await get_unified_fetcher()

        logger.info("✅ 初始化完成")

    async def phase1_prepare(self, target_date: str = None) -> List[str]:
        """
        Phase 1: 准备阶段

        Returns:
            需要更新的股票代码列表
        """
        logger.info("\n" + "=" * 70)
        logger.info("📋 Phase 1: 准备阶段")
        logger.info("=" * 70)

        # 1. 检查市场状态
        try:
            enforce_market_closed(target_date=target_date)
            logger.info("✅ 市场状态检查通过")
        except SystemExit:
            logger.error("❌ 市场未收盘，无法采集当日数据")
            raise

        # 2. 获取/更新股票列表
        logger.info("\n📊 获取股票列表...")
        stock_list = await self.fetcher.fetch_stock_list()

        if stock_list.empty:
            raise ValueError("获取股票列表失败")

        # 保存股票列表
        stock_list_path = self.data_dir / "stock_list.parquet"
        stock_list.to_parquet(stock_list_path, index=False)
        logger.info(f"✅ 股票列表已保存: {len(stock_list)} 只")

        # 3. 计算需要更新的股票
        codes = stock_list['code'].tolist()
        codes_to_update = self._get_codes_to_update(codes, target_date)

        self.stats.total_stocks = len(codes_to_update)
        logger.info(f"📈 需要更新: {len(codes_to_update)} 只股票")

        return codes_to_update

    def _get_codes_to_update(self, codes: List[str], target_date: str = None) -> List[str]:
        """获取需要更新的股票代码"""
        if not target_date:
            # 默认更新所有
            return codes

        # 检查每个股票的最后更新日期
        codes_to_update = []
        target = datetime.strptime(target_date, "%Y-%m-%d").date()

        for code in codes:
            kline_file = self.kline_dir / f"{code}.parquet"

            if not kline_file.exists():
                codes_to_update.append(code)
                continue

            try:
                import polars as pl
                df = pl.read_parquet(kline_file)
                # 兼容两种列名: trade_date 和 date
                date_col = None
                if 'trade_date' in df.columns:
                    date_col = 'trade_date'
                elif 'date' in df.columns:
                    date_col = 'date'

                if date_col:
                    last_date = df[date_col].max()
                    last = datetime.strptime(str(last_date), "%Y-%m-%d").date()

                    if last < target:
                        codes_to_update.append(code)
            except Exception as e:
                logger.warning(f"检查 {code} 失败: {e}")
                codes_to_update.append(code)

        return codes_to_update

    async def phase2_collect(self, codes: List[str], days: int = 3 * 365, use_async: bool = True) -> Dict[str, PipelineResult]:
        """
        Phase 2: 采集阶段

        Args:
            codes: 股票代码列表
            days: 获取历史天数
            use_async: 是否使用异步采集（默认True）

        Returns:
            采集结果字典
        """
        logger.info("\n" + "=" * 70)
        logger.info("📥 Phase 2: 采集阶段")
        logger.info("=" * 70)

        results = {}

        if use_async:
            # 使用异步采集
            logger.info(f"🔄 启动异步采集 (并发数: 10)")

            batch_results = await fetch_kline_data_async(
                codes=codes,
                kline_dir=self.kline_dir,
                days=days,
                max_concurrent=10,
                filter_delisted=True
            )

            # 更新统计
            self.stats.success_count += batch_results.get('success', 0)
            self.stats.failed_count += batch_results.get('failed', 0)
            self.stats.skipped_count += batch_results.get('skipped', 0)

            logger.info(f"   ✅ 成功: {batch_results.get('success', 0)}")
            logger.info(f"   ❌ 失败: {batch_results.get('failed', 0)}")
            logger.info(f"   ⏭️  跳过: {batch_results.get('skipped', 0)}")
        else:
            # 使用多进程并行采集（旧方式，可能不稳定）
            logger.info(f"🔄 启动多进程采集 (进程数: {KlineConfig.max_workers})")

            # 分批处理
            batch_size = KlineConfig.batch_size
            batches = [codes[i:i+batch_size] for i in range(0, len(codes), batch_size)]

            for i, batch in enumerate(batches, 1):
                logger.info(f"\n📦 处理批次 {i}/{len(batches)} ({len(batch)} 只股票)")

                # 使用现有的 kline_fetcher 进行批量采集
                batch_results = fetch_kline_data_parallel(
                    codes=batch,
                    kline_dir=self.kline_dir,
                    days=days
                )

                # 更新统计
                self.stats.success_count += batch_results.get('success', 0)
                self.stats.failed_count += batch_results.get('failed', 0)
                self.stats.skipped_count += batch_results.get('skipped', 0)

                logger.info(f"   ✅ 成功: {batch_results.get('success', 0)}")
                logger.info(f"   ❌ 失败: {batch_results.get('failed', 0)}")
                logger.info(f"   ⏭️  跳过: {batch_results.get('skipped', 0)}")

        return results

    async def phase3_validate(self, sample_size: int = 100) -> Dict:
        """
        Phase 3: 验证阶段

        Args:
            sample_size: 抽样验证数量

        Returns:
            验证报告
        """
        logger.info("\n" + "=" * 70)
        logger.info("🔍 Phase 3: 验证阶段")
        logger.info("=" * 70)

        # 抽样验证K线数据
        logger.info(f"🎯 抽样验证 {sample_size} 个文件...")
        validation_results = validate_all_kline_data(
            self.kline_dir,
            sample_size=sample_size
        )

        # 统计验证结果
        passed = sum(1 for r in validation_results.values() if r.success)
        failed = len(validation_results) - passed

        self.stats.validation_passed = passed
        self.stats.validation_failed = failed

        logger.info(f"✅ 验证通过: {passed}")
        logger.info(f"❌ 验证失败: {failed}")

        # 生成质量报告
        report_path = self.data_dir / "quality_report.json"
        report = generate_quality_report(validation_results, report_path)

        logger.info(f"📄 质量报告已保存: {report_path}")
        logger.info(f"📊 总体成功率: {report['summary']['overall_success_rate']:.1%}")

        return report

    async def phase4_finalize(self, report: Dict):
        """
        Phase 4: 收尾阶段
        """
        logger.info("\n" + "=" * 70)
        logger.info("📝 Phase 4: 收尾阶段")
        logger.info("=" * 70)

        self.stats.end_time = datetime.now()

        # 生成采集报告
        collection_report = {
            'collection_time': datetime.now().isoformat(),
            'statistics': self.stats.to_dict(),
            'quality_summary': report.get('summary', {})
        }

        # 保存报告
        report_path = self.data_dir / "collection_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(collection_report, f, ensure_ascii=False, indent=2)

        logger.info(f"📄 采集报告已保存: {report_path}")

        # 输出总结
        logger.info("\n" + "=" * 70)
        logger.info("📊 采集总结")
        logger.info("=" * 70)
        logger.info(f"总股票数: {self.stats.total_stocks}")
        logger.info(f"成功: {self.stats.success_count} ✅")
        logger.info(f"失败: {self.stats.failed_count} ❌")
        logger.info(f"跳过: {self.stats.skipped_count} ⏭️")
        logger.info(f"成功率: {self.stats.success_rate:.1%}")
        logger.info(f"耗时: {self.stats.duration}")
        logger.info(f"验证通过: {self.stats.validation_passed}/{self.stats.validation_passed + self.stats.validation_failed}")
        logger.info("=" * 70)

        # 如果有错误，记录到日志
        if self.stats.errors:
            logger.warning(f"⚠️ 采集过程中有 {len(self.stats.errors)} 个错误")
            for error in self.stats.errors[:5]:  # 只显示前5个
                logger.warning(f"   - {error}")

    async def run_daily_collection(self, target_date: str = None):
        """运行每日采集"""
        try:
            # Phase 1: 准备
            codes = await self.phase1_prepare(target_date)

            if not codes:
                logger.info("✅ 所有股票已是最新，无需更新")
                return

            # Phase 2: 采集
            await self.phase2_collect(codes, days=3*365)

            # Phase 3: 验证
            report = await self.phase3_validate(sample_size=100)

            # Phase 4: 收尾
            await self.phase4_finalize(report)

            logger.info("\n🎉 每日采集完成！")

        except Exception as e:
            logger.exception("采集过程出错")
            raise

    async def run_full_collection(self):
        """运行全量采集"""
        logger.info("🔄 启动全量采集模式")

        # 强制重新采集所有数据
        # 删除现有K线数据（可选）
        # import shutil
        # shutil.rmtree(self.kline_dir, ignore_errors=True)
        # self.kline_dir.mkdir(exist_ok=True)

        await self.run_daily_collection()

    async def run_incremental_collection(self, codes: List[str]):
        """运行指定股票的增量采集"""
        logger.info(f"🔄 启动增量采集模式: {len(codes)} 只股票")

        self.stats.total_stocks = len(codes)

        # Phase 2: 采集
        await self.phase2_collect(codes, days=30)  # 只采集最近30天

        # Phase 3: 验证
        report = await self.phase3_validate(sample_size=min(len(codes), 50))

        # Phase 4: 收尾
        await self.phase4_finalize(report)


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据采集主控器')
    parser.add_argument(
        '--mode',
        choices=['daily', 'full', 'incremental'],
        default='daily',
        help='采集模式: daily=每日增量, full=全量重新采集, incremental=指定股票增量'
    )
    parser.add_argument(
        '--codes',
        type=str,
        help='指定股票代码，逗号分隔（仅incremental模式）'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='目标日期 YYYY-MM-DD（默认今天）'
    )
    parser.add_argument(
        '--skip-market-check',
        action='store_true',
        help='跳过市场状态检查（仅测试使用）'
    )

    args = parser.parse_args()

    # 创建控制器
    controller = DataCollectionController()
    await controller.initialize()

    # 根据模式执行
    if args.mode == 'daily':
        await controller.run_daily_collection(target_date=args.date)
    elif args.mode == 'full':
        await controller.run_full_collection()
    elif args.mode == 'incremental':
        if not args.codes:
            print("❌ incremental模式需要指定 --codes 参数")
            sys.exit(1)
        codes = [c.strip() for c in args.codes.split(',')]
        await controller.run_incremental_collection(codes)


if __name__ == "__main__":
    asyncio.run(main())
