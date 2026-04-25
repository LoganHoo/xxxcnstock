#!/usr/bin/env python3
"""
盘中数据采集器

用于交易时段内高频采集实时数据：
- 实时报价（tick）
- 委托队列
- 成交明细

特点：
- 秒级/逐笔频率
- 轻量验证
- 内存缓存 + 批量写入

⚠️ 重要：盘中不采集K线数据（数据不完整）
"""
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Deque
from dataclasses import dataclass, field
from collections import deque
import pandas as pd
import json

from core.logger import setup_logger
from core.config import get_settings
from core.market_guardian import is_trading_time

logger = setup_logger("intraday_collector", log_file="system/intraday_collector.log")


@dataclass
class TickData:
    """Tick数据"""
    code: str
    time: datetime
    price: float
    volume: int
    bid1: float
    ask1: float
    bid1_vol: int
    ask1_vol: int


@dataclass
class IntradayCollectionResult:
    """盘中采集结果"""
    success: bool
    data_type: str
    message: str
    count: int = 0
    timestamp: str = None
    error: Optional[str] = None


class IntradayCollector:
    """盘中数据采集器"""

    def __init__(self, data_dir: str = None, buffer_size: int = 1000):
        self.settings = get_settings()
        self.data_dir = Path(data_dir or self.settings.DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 创建盘中数据目录
        self.tick_dir = self.data_dir / "intraday" / "ticks"
        self.tick_dir.mkdir(parents=True, exist_ok=True)
        self.trade_dir = self.data_dir / "intraday" / "trades"
        self.trade_dir.mkdir(parents=True, exist_ok=True)

        # 内存缓存
        self.buffer_size = buffer_size
        self.tick_buffer: Deque[TickData] = deque(maxlen=buffer_size)
        self.trade_buffer: Deque[Dict] = deque(maxlen=buffer_size)

        # 运行状态
        self.is_running = False
        self.collection_stats = {
            'ticks_collected': 0,
            'trades_collected': 0,
            'buffer_flushes': 0
        }

    def check_trading_hours(self) -> bool:
        """
        检查是否在交易时段

        Returns:
            True: 在交易时段
            False: 不在交易时段
        """
        now = datetime.now()

        # 检查是否是交易日（简化版，实际需要检查节假日）
        if now.weekday() >= 5:  # 周六日
            return False

        # 检查交易时间
        morning_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        morning_end = now.replace(hour=11, minute=30, second=0, microsecond=0)
        afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        afternoon_end = now.replace(hour=15, minute=0, second=0, microsecond=0)

        is_morning = morning_start <= now <= morning_end
        is_afternoon = afternoon_start <= now <= afternoon_end

        return is_morning or is_afternoon

    async def collect_tick(self, code: str) -> Optional[TickData]:
        """
        采集单只股票Tick数据

        Args:
            code: 股票代码

        Returns:
            TickData or None
        """
        try:
            # 这里需要接入实际的行情API
            # 暂时使用模拟数据
            import random

            tick = TickData(
                code=code,
                time=datetime.now(),
                price=random.uniform(10, 100),
                volume=random.randint(100, 10000),
                bid1=random.uniform(9.9, 99.9),
                ask1=random.uniform(10.1, 100.1),
                bid1_vol=random.randint(100, 5000),
                ask1_vol=random.randint(100, 5000)
            )

            # 添加到缓存
            self.tick_buffer.append(tick)
            self.collection_stats['ticks_collected'] += 1

            return tick

        except Exception as e:
            logger.error(f"采集 {code} Tick失败: {e}")
            return None

    async def flush_tick_buffer(self):
        """将Tick缓存写入磁盘"""
        if not self.tick_buffer:
            return

        try:
            # 转换为DataFrame
            data = []
            for tick in list(self.tick_buffer):
                data.append({
                    'code': tick.code,
                    'time': tick.time,
                    'price': tick.price,
                    'volume': tick.volume,
                    'bid1': tick.bid1,
                    'ask1': tick.ask1,
                    'bid1_vol': tick.bid1_vol,
                    'ask1_vol': tick.ask1_vol
                })

            df = pd.DataFrame(data)

            # 保存
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"ticks_{date_str}.parquet"
            output_path = self.tick_dir / filename

            # 追加模式
            if output_path.exists():
                try:
                    existing_df = pd.read_parquet(output_path)
                    df = pd.concat([existing_df, df], ignore_index=True)
                except Exception as e:
                    logger.warning(f"读取现有Tick数据失败: {e}")

            df.to_parquet(output_path, index=False)

            # 清空缓存
            self.tick_buffer.clear()
            self.collection_stats['buffer_flushes'] += 1

            logger.debug(f"✅ Tick缓存已写入: {len(df)} 条")

        except Exception as e:
            logger.exception("写入Tick缓存失败")

    async def collect_trades(self, code: str) -> List[Dict]:
        """
        采集成交明细

        Args:
            code: 股票代码

        Returns:
            成交明细列表
        """
        try:
            # 这里需要接入实际的成交明细API
            # 暂时返回空列表
            return []

        except Exception as e:
            logger.error(f"采集 {code} 成交明细失败: {e}")
            return []

    async def run_tick_collection(self, codes: List[str], interval_seconds: int = 5):
        """
        运行Tick采集

        Args:
            codes: 股票代码列表
            interval_seconds: 采集间隔（秒）
        """
        logger.info(f"🔄 启动Tick采集: {len(codes)} 只股票, 间隔: {interval_seconds}秒")

        self.is_running = True

        while self.is_running:
            try:
                # 检查是否在交易时段
                if not self.check_trading_hours():
                    logger.info("⏸️ 不在交易时段，暂停采集")
                    await asyncio.sleep(60)
                    continue

                # 采集每只股票的Tick
                for code in codes:
                    await self.collect_tick(code)

                    # 如果缓存满了，写入磁盘
                    if len(self.tick_buffer) >= self.buffer_size * 0.8:
                        await self.flush_tick_buffer()

                    # 短暂延迟，避免请求过快
                    await asyncio.sleep(0.1)

                # 等待下一次采集
                await asyncio.sleep(interval_seconds)

            except Exception as e:
                logger.exception("Tick采集出错")
                await asyncio.sleep(5)

    async def run_collection(self, mode: str = 'tick', codes: List[str] = None) -> IntradayCollectionResult:
        """
        运行盘中采集

        Args:
            mode: 采集模式 ('tick', 'trade')
            codes: 股票代码列表

        Returns:
            采集结果
        """
        timestamp = datetime.now()

        # 检查是否在交易时段
        if not self.check_trading_hours():
            return IntradayCollectionResult(
                success=False,
                data_type=mode,
                message='不在交易时段',
                timestamp=timestamp.isoformat()
            )

        if not codes:
            # 默认采集部分股票
            codes = ['000001', '000002', '600000']

        logger.info("=" * 70)
        logger.info("🚀 开始盘中数据采集")
        logger.info(f"模式: {mode}, 股票数: {len(codes)}")
        logger.info("=" * 70)

        try:
            if mode == 'tick':
                # 运行一段时间的Tick采集
                await asyncio.wait_for(
                    self.run_tick_collection(codes, interval_seconds=5),
                    timeout=60  # 采集1分钟
                )
            elif mode == 'trade':
                # 采集成交明细
                for code in codes:
                    trades = await self.collect_trades(code)
                    self.trade_buffer.extend(trades)

            # 刷新缓存
            await self.flush_tick_buffer()

            return IntradayCollectionResult(
                success=True,
                data_type=mode,
                message=f'盘中采集完成',
                count=self.collection_stats['ticks_collected'],
                timestamp=timestamp.isoformat()
            )

        except asyncio.TimeoutError:
            # 正常超时
            await self.flush_tick_buffer()

            return IntradayCollectionResult(
                success=True,
                data_type=mode,
                message=f'盘中采集完成（超时）',
                count=self.collection_stats['ticks_collected'],
                timestamp=timestamp.isoformat()
            )

        except Exception as e:
            logger.exception("盘中采集失败")
            return IntradayCollectionResult(
                success=False,
                data_type=mode,
                message='盘中采集失败',
                timestamp=timestamp.isoformat(),
                error=str(e)
            )

    def stop(self):
        """停止采集"""
        logger.info("🛑 停止盘中采集")
        self.is_running = False


# 便捷函数
async def collect_intraday_data(mode: str = 'tick', codes: List[str] = None) -> Dict:
    """采集盘中数据的便捷函数"""
    collector = IntradayCollector()
    result = await collector.run_collection(mode, codes)
    return {
        'success': result.success,
        'data_type': result.data_type,
        'message': result.message,
        'count': result.count
    }


async def run_intraday_collector(codes: List[str] = None, duration_minutes: int = 60):
    """
    运行盘中采集器一段时间

    Args:
        codes: 股票代码列表
        duration_minutes: 运行时长（分钟）
    """
    collector = IntradayCollector()

    if not codes:
        codes = ['000001', '000002', '600000']

    # 启动采集
    task = asyncio.create_task(
        collector.run_tick_collection(codes, interval_seconds=5)
    )

    # 运行指定时间
    await asyncio.sleep(duration_minutes * 60)

    # 停止
    collector.stop()
    await task

    logger.info(f"✅ 盘中采集完成: {collector.collection_stats}")
