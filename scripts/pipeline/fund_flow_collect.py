#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
资金流向数据采集脚本

采集个股资金流向数据，用于资金共振策略和尾盘突袭策略。
数据来源：Tushare Pro moneyflow 接口

采集字段：
- 主力净流入/流出
- 散户净流入/流出
- 大单/小单资金流向
- 资金流入占比

Author: AI Assistant
Date: 2026-04-27
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
import yaml

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.market_guardian import enforce_market_closed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            project_root / 'logs' / f'fund_flow_{datetime.now().strftime("%Y%m%d")}.log',
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)


class FundFlowCollector:
    """资金流向数据收集器"""

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.pro = None
        self.data_dir = Path(self.config.get('data', {}).get('fund_flow_dir', 'data/fund_flow'))
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _load_config(self, config_path: str = None) -> dict:
        """加载配置文件"""
        if config_path is None:
            config_path = project_root / 'config' / 'limitup_config.yaml'

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Failed to load config: {e}, using defaults")
            return {
                'data': {'fund_flow_dir': 'data/fund_flow'},
                'collection': {'rate_limit': 200},
                'tushare': {'token_env': 'TUSHARE_TOKEN'}
            }

    def connect(self) -> bool:
        """连接Tushare Pro"""
        try:
            import tushare as ts
            token = os.environ.get(
                self.config.get('tushare', {}).get('token_env', 'TUSHARE_TOKEN')
            )
            if not token:
                logger.error("TUSHARE_TOKEN not set")
                return False
            ts.set_token(token)
            self.pro = ts.pro_api()
            logger.info("Connected to Tushare Pro")
            return True
        except ImportError:
            logger.error("Tushare not installed. Install with: pip install tushare")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Tushare: {e}")
            return False

    def fetch_fund_flow(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的资金流向数据

        Args:
            trade_date: 交易日期 YYYY-MM-DD

        Returns:
            DataFrame with columns: code, name, buy_elg_amount, sell_elg_amount,
            net_mf_amount, trade_amount, etc.
        """
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()

        try:
            date_str = trade_date.replace('-', '')

            df = self.pro.moneyflow(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"No fund flow data for {trade_date}")
                return pd.DataFrame()

            df = df.rename(columns={
                'ts_code': 'code',
                'buy_sm_amount': 'buy_small_amount',
                'sell_sm_amount': 'sell_small_amount',
                'buy_md_amount': 'buy_medium_amount',
                'sell_md_amount': 'sell_medium_amount',
                'buy_lg_amount': 'buy_large_amount',
                'sell_lg_amount': 'sell_large_amount',
                'buy_elg_amount': 'buy_extra_large_amount',
                'sell_elg_amount': 'sell_extra_large_amount',
                'net_mf_amount': 'net_main_force_amount',
                'trade_amount': 'total_trade_amount'
            })

            df['trade_date'] = trade_date

            df['small_net_amount'] = df['buy_small_amount'] - df['sell_small_amount']
            df['medium_net_amount'] = df['buy_medium_amount'] - df['sell_medium_amount']
            df['large_net_amount'] = df['buy_large_amount'] - df['sell_large_amount']
            df['extra_large_net_amount'] = df['buy_extra_large_amount'] - df['sell_extra_large_amount']

            df['main_force_in_ratio'] = (
                df['net_main_force_amount'] / df['total_trade_amount'] * 100
            ).round(2)

            df['retail_in_ratio'] = (
                (df['small_net_amount'] + df['medium_net_amount']) /
                df['total_trade_amount'] * 100
            ).round(2)

            logger.info(f"Fetched {len(df)} fund flow records for {trade_date}")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch fund flow for {trade_date}: {e}")
            return pd.DataFrame()

    def fetch_stock_fund_flow_history(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取个股历史资金流向数据

        Args:
            ts_code: 股票代码 (如 '000001.SZ')
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD

        Returns:
            DataFrame with historical fund flow data
        """
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()

        try:
            start_str = start_date.replace('-', '')
            end_str = end_date.replace('-', '')

            df = self.pro.moneyflow(
                ts_code=ts_code,
                start_date=start_str,
                end_date=end_str
            )

            if df is None or df.empty:
                logger.warning(f"No fund flow history for {ts_code}")
                return pd.DataFrame()

            df['trade_date'] = pd.to_datetime(df['trade_date'] if 'trade_date' in df.columns else df['date']).dt.strftime('%Y-%m-%d')

            return df

        except Exception as e:
            logger.error(f"Failed to fetch fund flow history for {ts_code}: {e}")
            return pd.DataFrame()

    def save_fund_flow(self, df: pd.DataFrame, trade_date: str) -> bool:
        """保存资金流向数据到Parquet文件"""
        if df.empty:
            logger.warning(f"Empty fund flow data for {trade_date}, skipping save")
            return False

        try:
            output_file = self.data_dir / f"fund_flow_{trade_date}.parquet"
            df.to_parquet(output_file, index=False, compression='zstd')
            logger.info(f"Saved {len(df)} records to {output_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to save fund flow data: {e}")
            return False

    def load_fund_flow(self, trade_date: str) -> pd.DataFrame:
        """从Parquet文件加载资金流向数据"""
        try:
            input_file = self.data_dir / f"fund_flow_{trade_date}.parquet"
            if not input_file.exists():
                logger.warning(f"Fund flow file not found: {input_file}")
                return pd.DataFrame()

            df = pd.read_parquet(input_file)
            logger.info(f"Loaded {len(df)} records from {input_file}")
            return df
        except Exception as e:
            logger.error(f"Failed to load fund flow data: {e}")
            return pd.DataFrame()

    def collect_daily(self, trade_date: str = None) -> bool:
        """
        采集每日资金流向数据

        Args:
            trade_date: 交易日期，默认为昨天

        Returns:
            bool: 是否成功
        """
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        logger.info(f"=" * 60)
        logger.info(f"开始采集资金流向数据: {trade_date}")
        logger.info(f"=" * 60)

        df = self.fetch_fund_flow(trade_date)

        if df.empty:
            logger.error(f"Failed to fetch fund flow for {trade_date}")
            return False

        success = self.save_fund_flow(df, trade_date)

        if success:
            self._generate_summary(df, trade_date)

        return success

    def _generate_summary(self, df: pd.DataFrame, trade_date: str):
        """生成资金流向汇总报告"""
        logger.info(f"\n{'='*60}")
        logger.info(f"资金流向汇总 - {trade_date}")
        logger.info(f"{'='*60}")

        total_main_in = df[df['net_main_force_amount'] > 0]['net_main_force_amount'].sum()
        total_main_out = df[df['net_main_force_amount'] < 0]['net_main_force_amount'].sum()

        logger.info(f"主力资金净流入: {total_main_in/1e8:.2f} 亿")
        logger.info(f"主力资金净流出: {total_main_out/1e8:.2f} 亿")
        logger.info(f"主力资金净额: {(total_main_in + total_main_out)/1e8:.2f} 亿")

        top_in = df.nlargest(10, 'net_main_force_amount')[
            ['code', 'net_main_force_amount', 'main_force_in_ratio']
        ]
        logger.info(f"\n主力资金流入TOP10:")
        for _, row in top_in.iterrows():
            logger.info(f"  {row['code']}: {row['net_main_force_amount']/1e8:.2f}亿 "
                       f"({row['main_force_in_ratio']:.1f}%)")

        top_out = df.nsmallest(10, 'net_main_force_amount')[
            ['code', 'net_main_force_amount', 'main_force_in_ratio']
        ]
        logger.info(f"\n主力资金流出TOP10:")
        for _, row in top_out.iterrows():
            logger.info(f"  {row['code']}: {row['net_main_force_amount']/1e8:.2f}亿 "
                       f"({row['main_force_in_ratio']:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='资金流向数据采集')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--history', action='store_true', help='采集历史数据')
    parser.add_argument('--start-date', type=str, help='历史数据开始日期')
    parser.add_argument('--end-date', type=str, help='历史数据结束日期')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--check-only', action='store_true', help='仅检查不采集')

    args = parser.parse_args()

    collector = FundFlowCollector(config_path=args.config)

    if args.check_only:
        logger.info("检查模式 - 验证配置和连接")
        if collector.connect():
            logger.info("✅ Tushare连接正常")
            return 0
        else:
            logger.error("❌ Tushare连接失败")
            return 1

    if args.history:
        start_date = args.start_date or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = args.end_date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"采集历史数据: {start_date} 至 {end_date}")

        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        success_count = 0
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            if collector.collect_daily(date_str):
                success_count += 1
            current += timedelta(days=1)

        logger.info(f"历史数据采集完成: {success_count} 天成功")
        return 0 if success_count > 0 else 1

    trade_date = args.date
    if trade_date is None:
        trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    target_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
    today = datetime.now().date()

    if target_date == today:
        enforce_market_closed(target_date=trade_date)

    success = collector.collect_daily(trade_date)
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
