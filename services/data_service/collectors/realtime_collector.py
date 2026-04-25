#!/usr/bin/env python3
"""
实时数据采集器

用于定时采集实时行情数据：
- 实时行情快照
- 涨停池数据
- 资金流向数据

特点：
- 定时触发（5分钟/15分钟）
- 轻量级验证
- 增量追加存储
"""
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass
import pandas as pd
import json

from core.logger import setup_logger
from core.config import get_settings
from services.data_service.datasource import get_datasource_manager
from services.data_service.fetchers.unified_fetcher import get_unified_fetcher
from services.data_service.storage.parquet_manager import ParquetManager

logger = setup_logger("realtime_collector", log_file="system/realtime_collector.log")


@dataclass
class RealtimeCollectionResult:
    """实时采集结果"""
    success: bool
    data_type: str
    message: str
    row_count: int = 0
    timestamp: str = None
    error: Optional[str] = None


class RealtimeCollector:
    """实时数据采集器"""

    def __init__(self, data_dir: str = None):
        self.settings = get_settings()
        self.data_dir = Path(data_dir or self.settings.DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 创建实时数据目录
        self.realtime_dir = self.data_dir / "realtime"
        self.realtime_dir.mkdir(exist_ok=True)
        self.limitup_dir = self.data_dir / "limitup"
        self.limitup_dir.mkdir(exist_ok=True)
        self.fundflow_dir = self.data_dir / "fundflow"
        self.fundflow_dir.mkdir(exist_ok=True)

        self.storage = ParquetManager(str(self.data_dir))

    async def initialize(self):
        """初始化"""
        logger.info("🚀 实时数据采集器初始化")
        self.ds_manager = get_datasource_manager()
        await self.ds_manager.initialize()
        self.fetcher = await get_unified_fetcher()
        logger.info("✅ 初始化完成")

    def basic_validation(self, df: pd.DataFrame, required_cols: List[str]) -> tuple:
        """
        基础数据验证

        Args:
            df: DataFrame
            required_cols: 必需列

        Returns:
            (是否通过, 错误信息)
        """
        if df is None or df.empty:
            return False, "数据为空"

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少列: {missing_cols}"

        # 检查空值比例
        for col in required_cols:
            null_ratio = df[col].isnull().sum() / len(df)
            if null_ratio > 0.1:  # 空值超过10%
                return False, f"列 {col} 空值比例过高: {null_ratio:.1%}"

        return True, "OK"

    async def collect_realtime_quotes(self) -> RealtimeCollectionResult:
        """
        采集实时行情

        Returns:
            采集结果
        """
        logger.info("📊 采集实时行情...")
        timestamp = datetime.now()

        try:
            # 获取实时行情
            df = await self.fetcher.fetch_stock_list()

            if df.empty:
                return RealtimeCollectionResult(
                    success=False,
                    data_type='realtime_quotes',
                    message='获取实时行情失败',
                    timestamp=timestamp.isoformat()
                )

            # 基础验证
            required_cols = ['code', 'name']
            is_valid, error_msg = self.basic_validation(df, required_cols)

            if not is_valid:
                logger.warning(f"实时行情验证失败: {error_msg}")
                # 实时数据验证失败不阻断，继续保存

            # 添加时间戳
            df['collect_time'] = timestamp

            # 保存
            date_str = timestamp.strftime('%Y%m%d')
            time_str = timestamp.strftime('%H%M%S')
            filename = f"{date_str}_{time_str}.parquet"

            output_path = self.realtime_dir / filename
            df.to_parquet(output_path, index=False)

            logger.info(f"✅ 实时行情已保存: {len(df)} 条 -> {filename}")

            return RealtimeCollectionResult(
                success=True,
                data_type='realtime_quotes',
                message=f'实时行情采集完成: {len(df)} 条',
                row_count=len(df),
                timestamp=timestamp.isoformat()
            )

        except Exception as e:
            logger.exception("采集实时行情失败")
            return RealtimeCollectionResult(
                success=False,
                data_type='realtime_quotes',
                message='采集实时行情失败',
                timestamp=timestamp.isoformat(),
                error=str(e)
            )

    async def collect_limit_up_pool(self) -> RealtimeCollectionResult:
        """
        采集涨停池数据

        Returns:
            采集结果
        """
        logger.info("📊 采集涨停池...")
        timestamp = datetime.now()

        try:
            # 这里需要实现涨停池获取
            # 暂时使用模拟数据
            from services.data_service.fetchers.limitup import LimitUpFetcher

            fetcher = LimitUpFetcher()
            stocks = await fetcher.fetch_limit_up_pool()

            if not stocks:
                return RealtimeCollectionResult(
                    success=False,
                    data_type='limit_up_pool',
                    message='获取涨停池失败',
                    timestamp=timestamp.isoformat()
                )

            # 转换为DataFrame
            df = pd.DataFrame([{
                'code': s.code,
                'name': s.name,
                'change_pct': s.change_pct,
                'limit_time': s.limit_time,
                'seal_amount': s.seal_amount,
                'open_count': s.open_count,
                'continuous_limit': s.continuous_limit,
                'sector': s.sector,
                'collect_time': timestamp
            } for s in stocks])

            # 基础验证
            required_cols = ['code', 'name', 'change_pct']
            is_valid, error_msg = self.basic_validation(df, required_cols)

            if not is_valid:
                logger.warning(f"涨停池验证失败: {error_msg}")

            # 保存
            date_str = timestamp.strftime('%Y%m%d')
            filename = f"{date_str}.parquet"

            output_path = self.limitup_dir / filename

            # 如果文件存在，合并数据
            if output_path.exists():
                try:
                    existing_df = pd.read_parquet(output_path)
                    df = pd.concat([existing_df, df], ignore_index=True)
                    df = df.drop_duplicates(subset=['code', 'collect_time'], keep='last')
                except Exception as e:
                    logger.warning(f"合并现有数据失败: {e}")

            df.to_parquet(output_path, index=False)

            logger.info(f"✅ 涨停池已保存: {len(df)} 条 -> {filename}")

            return RealtimeCollectionResult(
                success=True,
                data_type='limit_up_pool',
                message=f'涨停池采集完成: {len(df)} 条',
                row_count=len(df),
                timestamp=timestamp.isoformat()
            )

        except Exception as e:
            logger.exception("采集涨停池失败")
            return RealtimeCollectionResult(
                success=False,
                data_type='limit_up_pool',
                message='采集涨停池失败',
                timestamp=timestamp.isoformat(),
                error=str(e)
            )

    async def collect_fund_flow(self) -> RealtimeCollectionResult:
        """
        采集资金流向数据

        Returns:
            采集结果
        """
        logger.info("📊 采集资金流向...")
        timestamp = datetime.now()

        try:
            # 这里需要实现资金流向获取
            # 暂时返回成功，实际实现需要根据数据源

            logger.info("⏭️ 资金流向采集暂未实现")

            return RealtimeCollectionResult(
                success=True,
                data_type='fund_flow',
                message='资金流向采集暂未实现',
                row_count=0,
                timestamp=timestamp.isoformat()
            )

        except Exception as e:
            logger.exception("采集资金流向失败")
            return RealtimeCollectionResult(
                success=False,
                data_type='fund_flow',
                message='采集资金流向失败',
                timestamp=timestamp.isoformat(),
                error=str(e)
            )

    async def run_collection(self, data_types: List[str] = None) -> Dict:
        """
        运行实时采集

        Args:
            data_types: 数据类型列表 ['quotes', 'limitup', 'fundflow']

        Returns:
            采集结果统计
        """
        if data_types is None:
            data_types = ['quotes', 'limitup']

        logger.info("=" * 70)
        logger.info("🚀 开始实时数据采集")
        logger.info(f"数据类型: {data_types}")
        logger.info("=" * 70)

        start_time = datetime.now()
        results = {}

        # 采集实时行情
        if 'quotes' in data_types:
            results['quotes'] = await self.collect_realtime_quotes()

        # 采集涨停池
        if 'limitup' in data_types:
            results['limitup'] = await self.collect_limit_up_pool()

        # 采集资金流向
        if 'fundflow' in data_types:
            results['fundflow'] = await self.collect_fund_flow()

        end_time = datetime.now()
        duration = end_time - start_time

        # 统计
        success_count = sum(1 for r in results.values() if r.success)
        total_count = len(results)

        stats = {
            'success': success_count == total_count,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
            'total_tasks': total_count,
            'success_count': success_count,
            'failed_count': total_count - success_count,
            'results': {
                k: {
                    'success': v.success,
                    'message': v.message,
                    'row_count': v.row_count
                } for k, v in results.items()
            }
        }

        logger.info("=" * 70)
        logger.info("📊 实时采集完成")
        logger.info(f"任务数: {stats['total_tasks']}")
        logger.info(f"成功: {stats['success_count']}")
        logger.info(f"失败: {stats['failed_count']}")
        logger.info(f"耗时: {duration}")
        logger.info("=" * 70)

        return stats

    async def run_scheduled(self, interval_minutes: int = 5):
        """
        定时运行采集

        Args:
            interval_minutes: 采集间隔（分钟）
        """
        logger.info(f"🔄 启动定时采集，间隔: {interval_minutes}分钟")

        while True:
            try:
                await self.run_collection(['quotes'])
                logger.info(f"⏳ 等待 {interval_minutes} 分钟...")
                await asyncio.sleep(interval_minutes * 60)
            except Exception as e:
                logger.exception("定时采集出错")
                await asyncio.sleep(60)  # 出错后等待1分钟再试


# 便捷函数
async def collect_realtime_data(data_types: List[str] = None) -> Dict:
    """采集实时数据的便捷函数"""
    collector = RealtimeCollector()
    await collector.initialize()
    return await collector.run_collection(data_types)


async def run_realtime_scheduler(interval_minutes: int = 5):
    """运行实时数据定时采集"""
    collector = RealtimeCollector()
    await collector.initialize()
    await collector.run_scheduled(interval_minutes)
