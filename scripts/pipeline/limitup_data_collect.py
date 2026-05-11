#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
涨停数据采集脚本

功能：
1. 盘中实时涨停数据采集（每30秒）
2. 涨停数据质量检查（字段完整性、合理性验证）
3. Redis 热数据存储（TTL 24小时）
4. Parquet 冷数据存储（按日期分区）
5. 炸板事件检测和记录
6. 炸板回封跟踪

Author: AI Assistant
Date: 2026-04-27
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Set
import pandas as pd
import numpy as np
import json

project_root = Path(__file__).parent.parent.parent

# 加载环境变量
try:
    from dotenv import load_dotenv
    load_dotenv(project_root / '.env')
except ImportError:
    pass

sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            project_root / 'logs' / f'limitup_collect_{datetime.now().strftime("%Y%m%d")}.log',
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)

# 服务层导入 - 使用真实涨停数据获取器替代 mock 数据
from services.data_service.fetchers.limitup import LimitUpFetcher
from services.data_service.fetchers.stock_list_cache import StockListCacheManager


class LimitUpDataCollector:
    """涨停数据采集器 - 通过服务层获取真实数据"""

    def __init__(self):
        self.data_dir = project_root / 'data' / 'limitup'
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.redis_client = None
        self._init_redis()

        # 服务层实例
        self._limitup_fetcher = LimitUpFetcher()
        self._stock_cache_mgr = StockListCacheManager()

        self.limit_up_cache = {}  # 缓存上一轮的涨停股票
        self.broken_board_records = []  # 炸板记录

    def _init_redis(self):
        """初始化Redis连接"""
        try:
            import redis
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_password = os.getenv('REDIS_PASSWORD', None)

            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            self.redis_client.ping()
            logger.info(f"Redis connected: {redis_host}:{redis_port}")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}, using local cache only")
            self.redis_client = None

    def load_stock_list(self) -> pd.DataFrame:
        """加载股票列表 - 通过 StockListCacheManager 服务层"""
        try:
            # 优先从服务层获取
            codes = self._stock_cache_mgr.get_codes(use_redis=True)
            if codes:
                # 获取带详情的数据
                df = self._stock_cache_mgr.load_from_parquet()
                if df is not None and not df.empty:
                    logger.info(f"Loaded stock list via service layer: {len(df)} stocks")
                    return df
        except Exception as e:
            logger.warning(f"Service layer stock list failed: {e}")

        # 降级: 直接读文件
        try:
            stock_list_path = project_root / 'data' / 'stock_list.parquet'
            if stock_list_path.exists():
                return pd.read_parquet(stock_list_path)

            stock_list_path = project_root / 'data' / 'stock_list.csv'
            if stock_list_path.exists():
                return pd.read_csv(stock_list_path)
        except Exception as e:
            logger.error(f"Failed to load stock list from file: {e}")

        logger.warning("Stock list unavailable from all sources")
        return pd.DataFrame(columns=['code', 'name', 'industry'])

    def fetch_limit_up_data(self) -> pd.DataFrame:
        """
        获取涨停数据 - 通过 LimitUpFetcher 服务层获取真实数据

        Returns:
            涨停股票 DataFrame，若服务层失败则返回空 DataFrame
        """
        logger.info("Fetching limit up data via service layer...")

        try:
            # 使用 asyncio 调用异步的 fetch_limit_up_pool
            loop = None
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(
                        asyncio.run,
                        self._limitup_fetcher.fetch_limit_up_pool()
                    )
                    stocks = future.result(timeout=30)
            else:
                stocks = asyncio.run(self._limitup_fetcher.fetch_limit_up_pool())

            if not stocks:
                logger.info("No limit up stocks found for today")
                return pd.DataFrame()

            # 将 LimitUpStock 对象转为 DataFrame
            limit_up_data = []
            for s in stocks:
                limit_up_data.append({
                    'code': s.code,
                    'name': s.name,
                    'limit_up_time': s.limit_time,
                    'limit_up_price': 0.0,  # 涨停价需从行情获取
                    'volume': 0,
                    'amount': float(s.seal_amount) * 10000,  # 封单资金(万) -> 元
                    'buy_lock_volume': 0,
                    'sell_lock_volume': 0,
                    'limit_up_type': '连板' if s.continuous_limit > 1 else '首板',
                    'consecutive_days': s.continuous_limit,
                    'industry': s.sector,
                    'change_pct': s.change_pct,
                    'open_count': s.open_count,
                })

            df = pd.DataFrame(limit_up_data)
            logger.info(f"Fetched {len(df)} real limit up stocks via service layer")
            return df

        except Exception as e:
            logger.error(f"Service layer fetch failed: {e}")
            logger.warning("Returning empty DataFrame - no mock data in production")
            return pd.DataFrame()

    def validate_limit_up_data(self, df: pd.DataFrame) -> bool:
        """验证涨停数据质量 - 空数据视为有效（当天可能无涨停股）"""
        if df.empty:
            logger.info("Limit up data is empty (no limit up stocks today)")
            return True

        required_columns = ['code', 'name', 'limit_up_time', 'limit_up_price', 'volume']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return False

        for _, row in df.iterrows():
            if row['volume'] <= 0:
                logger.warning(f"Invalid volume for {row['code']}: {row['volume']}")
                return False
            if row['limit_up_price'] <= 0:
                logger.warning(f"Invalid price for {row['code']}: {row['limit_up_price']}")
                return False

        logger.info(f"Data validation passed: {len(df)} limit up stocks")
        return True

    def detect_broken_board(self, current_df: pd.DataFrame) -> List[Dict]:
        """检测炸板事件"""
        if not self.limit_up_cache:
            return []

        current_codes = set(current_df['code'].tolist())
        previous_codes = set(self.limit_up_cache.keys())

        broken_board_codes = previous_codes - current_codes

        broken_board_events = []
        for code in broken_board_codes:
            previous_data = self.limit_up_cache[code]
            event = {
                'code': code,
                'name': previous_data['name'],
                'broken_time': datetime.now().strftime('%H:%M:%S'),
                'limit_up_time': previous_data['limit_up_time'],
                'limit_up_price': previous_data['limit_up_price'],
                'event_type': '炸板'
            }
            broken_board_events.append(event)
            logger.warning(f"炸板 detected: {code} {previous_data['name']}")

        return broken_board_events

    def detect_reseal(self, current_df: pd.DataFrame) -> List[Dict]:
        """检测回封事件"""
        if not self.limit_up_cache:
            return []

        previous_codes = set(self.limit_up_cache.keys())

        reseal_events = []
        for _, row in current_df.iterrows():
            code = row['code']
            if code not in previous_codes and row.get('limit_up_type') == '炸板回封':
                event = {
                    'code': code,
                    'name': row['name'],
                    'reseal_time': datetime.now().strftime('%H:%M:%S'),
                    'limit_up_price': row['limit_up_price'],
                    'event_type': '回封'
                }
                reseal_events.append(event)
                logger.info(f"回封 detected: {code} {row['name']}")

        return reseal_events

    def save_to_redis(self, df: pd.DataFrame, trade_date: str):
        """保存到Redis热数据"""
        if self.redis_client is None:
            logger.warning("Redis not available, skipping hot data storage")
            return

        try:
            key = f"limitup:{trade_date}"
            data = df.to_dict('records')

            self.redis_client.setex(
                key,
                timedelta(hours=24),
                json.dumps(data, ensure_ascii=False)
            )

            for _, row in df.iterrows():
                stock_key = f"limitup:{trade_date}:{row['code']}"
                self.redis_client.setex(
                    stock_key,
                    timedelta(hours=24),
                    json.dumps(row.to_dict(), ensure_ascii=False)
                )

            logger.info(f"Saved to Redis: {len(df)} stocks, key={key}")
        except Exception as e:
            logger.error(f"Failed to save to Redis: {e}")

    def save_to_parquet(self, df: pd.DataFrame, trade_date: str):
        """保存到Parquet冷数据"""
        try:
            date_dir = self.data_dir / trade_date
            date_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%H%M%S')
            file_path = date_dir / f"limitup_{timestamp}.parquet"

            df.to_parquet(file_path, index=False)
            logger.info(f"Saved to Parquet: {file_path}, {len(df)} stocks")

            daily_file = date_dir / "limitup_daily.parquet"
            if daily_file.exists():
                existing_df = pd.read_parquet(daily_file)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['code'], keep='last')
            else:
                combined_df = df

            combined_df.to_parquet(daily_file, index=False)
            logger.info(f"Updated daily file: {daily_file}, {len(combined_df)} stocks")

        except Exception as e:
            logger.error(f"Failed to save to Parquet: {e}")

    def save_broken_board_records(self, events: List[Dict], trade_date: str):
        """保存炸板记录"""
        if not events:
            return

        try:
            date_dir = self.data_dir / trade_date
            date_dir.mkdir(parents=True, exist_ok=True)

            broken_board_file = date_dir / "broken_board.parquet"

            new_records = pd.DataFrame(events)

            if broken_board_file.exists():
                existing_records = pd.read_parquet(broken_board_file)
                combined_records = pd.concat([existing_records, new_records], ignore_index=True)
                combined_records = combined_records.drop_duplicates(subset=['code', 'broken_time'], keep='last')
            else:
                combined_records = new_records

            combined_records.to_parquet(broken_board_file, index=False)
            logger.info(f"Saved broken board records: {len(events)} events")

        except Exception as e:
            logger.error(f"Failed to save broken board records: {e}")

    def collect_once(self, trade_date: str = None) -> pd.DataFrame:
        """执行一次采集"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"=" * 60)
        logger.info(f"涨停数据采集: {trade_date} {datetime.now().strftime('%H:%M:%S')}")
        logger.info(f"=" * 60)

        df = self.fetch_limit_up_data()

        if not self.validate_limit_up_data(df):
            logger.error("Data validation failed")
            return df

        broken_board_events = self.detect_broken_board(df)
        if broken_board_events:
            self.save_broken_board_records(broken_board_events, trade_date)
            self.broken_board_records.extend(broken_board_events)

        reseal_events = self.detect_reseal(df)
        if reseal_events:
            logger.info(f"Reseal events: {len(reseal_events)}")

        self.save_to_redis(df, trade_date)
        self.save_to_parquet(df, trade_date)

        self.limit_up_cache = {row['code']: row.to_dict() for _, row in df.iterrows()}

        logger.info(f"Collection complete: {len(df)} limit up stocks")
        return df

    def collect_realtime(self, duration_minutes: int = 240, interval_seconds: int = 30):
        """
        实时采集（盘中运行）

        Args:
            duration_minutes: 运行时长（分钟）
            interval_seconds: 采集间隔（秒）
        """
        trade_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"=" * 60)
        logger.info(f"实时涨停数据采集启动")
        logger.info(f"日期: {trade_date}")
        logger.info(f"时长: {duration_minutes}分钟")
        logger.info(f"间隔: {interval_seconds}秒")
        logger.info(f"=" * 60)

        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)

        iteration = 0
        while datetime.now() < end_time:
            iteration += 1
            logger.info(f"\n--- 第{iteration}轮采集 ---")

            try:
                self.collect_once(trade_date)
            except Exception as e:
                logger.error(f"Collection error: {e}")

            next_time = datetime.now() + timedelta(seconds=interval_seconds)
            sleep_seconds = (next_time - datetime.now()).total_seconds()

            if sleep_seconds > 0:
                logger.info(f"Sleeping for {sleep_seconds:.1f} seconds...")
                import time
                time.sleep(sleep_seconds)

        logger.info(f"\n实时采集结束，共执行{iteration}轮")


def main():
    parser = argparse.ArgumentParser(description='涨停数据采集')
    parser.add_argument('--mode', type=str, choices=['once', 'realtime'],
                        default='once',
                        help='采集模式: once=单次, realtime=实时')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--duration', type=int, default=240,
                        help='实时采集时长（分钟，默认240=4小时）')
    parser.add_argument('--interval', type=int, default=30,
                        help='实时采集间隔（秒，默认30）')

    args = parser.parse_args()

    collector = LimitUpDataCollector()

    if args.mode == 'once':
        collector.collect_once(args.date)
    else:
        collector.collect_realtime(args.duration, args.interval)

    return 0


if __name__ == '__main__':
    sys.exit(main())
