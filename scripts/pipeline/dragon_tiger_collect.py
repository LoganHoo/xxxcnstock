#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
龙虎榜数据采集脚本

采集每日龙虎榜数据，用于分析主力资金动向和游资行为。
数据来源：Tushare Pro top_list 接口

采集字段：
- 上榜股票列表
- 买卖营业部明细
- 净买入金额
- 机构/游资参与情况

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
            project_root / 'logs' / f'dragon_tiger_{datetime.now().strftime("%Y%m%d")}.log',
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)


class DragonTigerCollector:
    """龙虎榜数据收集器"""

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.pro = None
        self.data_dir = Path(self.config.get('data', {}).get('dragon_tiger_dir', 'data/dragon_tiger'))
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
                'data': {'dragon_tiger_dir': 'data/dragon_tiger'},
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

    def fetch_top_list(self, trade_date: str) -> pd.DataFrame:
        """
        获取龙虎榜列表数据

        Args:
            trade_date: 交易日期 YYYY-MM-DD

        Returns:
            DataFrame with top list data
        """
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()

        try:
            date_str = trade_date.replace('-', '')

            df = self.pro.top_list(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"No top list data for {trade_date}")
                return pd.DataFrame()

            df = df.rename(columns={
                'ts_code': 'code',
                'name': 'name',
                'close': 'close_price',
                'pct_change': 'price_change_pct',
                'turnover_rate': 'turnover_ratio',
                'amount': 'trade_amount',
                'l_buy': 'buy_amount',
                'l_sell': 'sell_amount',
                'net_amount': 'net_buy_amount',
                'buy_amount': 'total_buy_amount',
                'sell_amount': 'total_sell_amount'
            })

            df['trade_date'] = trade_date

            logger.info(f"Fetched {len(df)} top list records for {trade_date}")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch top list for {trade_date}: {e}")
            return pd.DataFrame()

    def fetch_top_inst(self, trade_date: str) -> pd.DataFrame:
        """
        获取龙虎榜机构交易明细

        Args:
            trade_date: 交易日期 YYYY-MM-DD

        Returns:
            DataFrame with institution trading details
        """
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()

        try:
            date_str = trade_date.replace('-', '')

            df = self.pro.top_inst(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"No institution data for {trade_date}")
                return pd.DataFrame()

            df = df.rename(columns={
                'ts_code': 'code',
                'exalter': 'broker_name',
                'buy': 'buy_amount',
                'sell': 'sell_amount',
                'net_buy': 'net_buy_amount',
                'side': 'trade_side'
            })

            df['trade_date'] = trade_date

            df['is_institution'] = df['broker_name'].str.contains(
                '机构专用', na=False
            )

            logger.info(f"Fetched {len(df)} institution records for {trade_date}")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch institution data for {trade_date}: {e}")
            return pd.DataFrame()

    def fetch_broker_list(self) -> pd.DataFrame:
        """获取营业部列表"""
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()

        try:
            df = self.pro.broker_list()

            if df is None or df.empty:
                logger.warning("No broker list data")
                return pd.DataFrame()

            logger.info(f"Fetched {len(df)} brokers")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch broker list: {e}")
            return pd.DataFrame()

    def analyze_broker_patterns(
        self,
        top_list_df: pd.DataFrame,
        inst_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        分析营业部交易模式

        Args:
            top_list_df: 龙虎榜列表数据
            inst_df: 机构交易明细

        Returns:
            DataFrame with broker pattern analysis
        """
        if top_list_df.empty or inst_df.empty:
            return pd.DataFrame()

        analysis = []

        for code in top_list_df['code'].unique():
            stock_inst = inst_df[inst_df['code'] == code]
            stock_info = top_list_df[top_list_df['code'] == code].iloc[0]

            inst_buy = stock_inst[
                (stock_inst['is_institution']) &
                (stock_inst['net_buy_amount'] > 0)
            ]['net_buy_amount'].sum()

            inst_sell = stock_inst[
                (stock_inst['is_institution']) &
                (stock_inst['net_buy_amount'] < 0)
            ]['net_buy_amount'].sum()

            top_brokers = stock_inst.nlargest(3, 'net_buy_amount')['broker_name'].tolist()

            analysis.append({
                'code': code,
                'name': stock_info.get('name', ''),
                'inst_net_buy': inst_buy + inst_sell,
                'inst_buy_count': len(stock_inst[
                    (stock_inst['is_institution']) &
                    (stock_inst['net_buy_amount'] > 0)
                ]),
                'inst_sell_count': len(stock_inst[
                    (stock_inst['is_institution']) &
                    (stock_inst['net_buy_amount'] < 0)
                ]),
                'top_brokers': ','.join(top_brokers),
                'total_brokers': stock_inst['broker_name'].nunique(),
                'trade_date': stock_info.get('trade_date', '')
            })

        return pd.DataFrame(analysis)

    def save_data(
        self,
        top_list_df: pd.DataFrame,
        inst_df: pd.DataFrame,
        trade_date: str
    ) -> bool:
        """保存龙虎榜数据"""
        success = True

        if not top_list_df.empty:
            try:
                output_file = self.data_dir / f"top_list_{trade_date}.parquet"
                top_list_df.to_parquet(output_file, index=False, compression='zstd')
                logger.info(f"Saved top list: {len(top_list_df)} records to {output_file}")
            except Exception as e:
                logger.error(f"Failed to save top list: {e}")
                success = False

        if not inst_df.empty:
            try:
                output_file = self.data_dir / f"top_inst_{trade_date}.parquet"
                inst_df.to_parquet(output_file, index=False, compression='zstd')
                logger.info(f"Saved institution data: {len(inst_df)} records to {output_file}")
            except Exception as e:
                logger.error(f"Failed to save institution data: {e}")
                success = False

        return success

    def load_data(self, trade_date: str) -> tuple:
        """加载龙虎榜数据"""
        top_list_df = pd.DataFrame()
        inst_df = pd.DataFrame()

        try:
            top_list_file = self.data_dir / f"top_list_{trade_date}.parquet"
            if top_list_file.exists():
                top_list_df = pd.read_parquet(top_list_file)
                logger.info(f"Loaded top list: {len(top_list_df)} records")
        except Exception as e:
            logger.error(f"Failed to load top list: {e}")

        try:
            inst_file = self.data_dir / f"top_inst_{trade_date}.parquet"
            if inst_file.exists():
                inst_df = pd.read_parquet(inst_file)
                logger.info(f"Loaded institution data: {len(inst_df)} records")
        except Exception as e:
            logger.error(f"Failed to load institution data: {e}")

        return top_list_df, inst_df

    def collect_daily(self, trade_date: str = None) -> bool:
        """
        采集每日龙虎榜数据

        Args:
            trade_date: 交易日期，默认为昨天

        Returns:
            bool: 是否成功
        """
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        logger.info(f"=" * 60)
        logger.info(f"开始采集龙虎榜数据: {trade_date}")
        logger.info(f"=" * 60)

        top_list_df = self.fetch_top_list(trade_date)
        inst_df = self.fetch_top_inst(trade_date)

        if top_list_df.empty and inst_df.empty:
            logger.error(f"No dragon tiger data for {trade_date}")
            return False

        success = self.save_data(top_list_df, inst_df, trade_date)

        if success and not top_list_df.empty:
            self._generate_summary(top_list_df, inst_df, trade_date)

        return success

    def _generate_summary(
        self,
        top_list_df: pd.DataFrame,
        inst_df: pd.DataFrame,
        trade_date: str
    ):
        """生成龙虎榜汇总报告"""
        logger.info(f"\n{'='*60}")
        logger.info(f"龙虎榜汇总 - {trade_date}")
        logger.info(f"{'='*60}")

        logger.info(f"上榜股票数: {len(top_list_df)}")

        if 'net_buy_amount' in top_list_df.columns:
            total_net_buy = top_list_df['net_buy_amount'].sum()
            logger.info(f"总净买入: {total_net_buy/1e8:.2f} 亿")

            top_net_buy = top_list_df.nlargest(5, 'net_buy_amount')
            logger.info(f"\n净买入TOP5:")
            for _, row in top_net_buy.iterrows():
                logger.info(f"  {row['code']} {row.get('name', '')}: "
                           f"{row['net_buy_amount']/1e8:.2f}亿")

        if not inst_df.empty:
            inst_buy = inst_inst[
                (inst_df['is_institution']) &
                (inst_df['net_buy_amount'] > 0)
            ]['net_buy_amount'].sum()

            inst_sell = inst_df[
                (inst_df['is_institution']) &
                (inst_df['net_buy_amount'] < 0)
            ]['net_buy_amount'].sum()

            logger.info(f"\n机构参与:")
            logger.info(f"  机构买入: {inst_buy/1e8:.2f} 亿")
            logger.info(f"  机构卖出: {inst_sell/1e8:.2f} 亿")
            logger.info(f"  机构净额: {(inst_buy + inst_sell)/1e8:.2f} 亿")


def main():
    parser = argparse.ArgumentParser(description='龙虎榜数据采集')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--history', action='store_true', help='采集历史数据')
    parser.add_argument('--start-date', type=str, help='历史数据开始日期')
    parser.add_argument('--end-date', type=str, help='历史数据结束日期')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--check-only', action='store_true', help='仅检查不采集')
    parser.add_argument('--analyze', action='store_true', help='分析营业部模式')

    args = parser.parse_args()

    collector = DragonTigerCollector(config_path=args.config)

    if args.check_only:
        logger.info("检查模式 - 验证配置和连接")
        if collector.connect():
            logger.info("✅ Tushare连接正常")
            return 0
        else:
            logger.error("❌ Tushare连接失败")
            return 1

    if args.analyze:
        trade_date = args.date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        top_list_df, inst_df = collector.load_data(trade_date)

        if not top_list_df.empty and not inst_df.empty:
            analysis = collector.analyze_broker_patterns(top_list_df, inst_df)
            logger.info(f"\n营业部模式分析:")
            logger.info(analysis.head(10).to_string())
        return 0

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
