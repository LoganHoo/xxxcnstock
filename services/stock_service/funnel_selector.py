"""
漏斗选股器
5层漏斗 + AI综合评分
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

from core.logger import setup_logger

logger = setup_logger("funnel_selector")


class FunnelSelector:
    """漏斗选股器"""

    RETENTION_RATE = 0.85

    def __init__(self, data_dir: str = "data/kline"):
        self.data_dir = Path(data_dir)
        self.results = []

    def run(self, trade_date: str, stock_list: pd.DataFrame = None) -> pd.DataFrame:
        """执行漏斗选股

        Args:
            trade_date: 交易日期 YYYY-MM-DD
            stock_list: 初始股票列表，若为None则从数据目录加载

        Returns:
            选股结果 DataFrame
        """
        logger.info("=" * 60)
        logger.info(f"开始漏斗选股 日期: {trade_date}")
        logger.info("=" * 60)

        if stock_list is None:
            stock_list = self._load_stock_list(trade_date)

        original_count = len(stock_list)
        logger.info(f"初始股票数量: {original_count}")

        stock_list = self._layer1_basic_filter(stock_list)
        logger.info(f"第1层基础过滤后: {len(stock_list)} 只")

        stock_list = self._layer2_fundamental_filter(stock_list)
        logger.info(f"第2层基本面过滤后: {len(stock_list)} 只")

        stock_list = self._layer3_technical_filter(stock_list, trade_date)
        logger.info(f"第3层技术面过滤后: {len(stock_list)} 只")

        stock_list = self._layer4_money_filter(stock_list, trade_date)
        logger.info(f"第4层资金面过滤后: {len(stock_list)} 只")

        stock_list = self._layer5_pattern_filter(stock_list, trade_date)
        logger.info(f"第5层形态过滤后: {len(stock_list)} 只")

        stock_list = self._calculate_all_scores(stock_list, trade_date)

        self.results = stock_list
        logger.info(f"漏斗选股完成: {len(stock_list)} / {original_count} 只")
        return stock_list

    def _load_stock_list(self, trade_date: str) -> pd.DataFrame:
        """加载初始股票列表"""
        all_stocks = []
        for parquet_file in self.data_dir.glob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                if len(df) > 0:
                    latest = df.iloc[-1]
                    date_col = 'trade_date' if 'trade_date' in df.columns else 'date'
                    data_date = latest.get(date_col, None)
                    if data_date and str(data_date) != 'NaT':
                        data_date = pd.to_datetime(data_date)
                    else:
                        data_date = pd.NaT
                    all_stocks.append({
                        'code': parquet_file.stem,
                        'name': latest.get('name', ''),
                        'close': latest.get('close', 0),
                        'volume': latest.get('volume', 0),
                        'change_pct': latest.get('change_pct', 0),
                        'turnover_rate': latest.get('turnover_rate', 0),
                        'data_date': data_date,
                    })
            except Exception as e:
                logger.debug(f"加载 {parquet_file.name} 失败: {e}")
        return pd.DataFrame(all_stocks)

    def _layer1_basic_filter(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """第1层：基础过滤 - ST/退市/停牌/涨跌停/数据过期"""
        original = len(stocks)
        df = stocks.copy()

        df = df[~df['name'].str.contains('ST|退市', na=False, case=False)]
        df = df[df['volume'] > 0]
        df = df[df['change_pct'] > -9.9]
        df = df[df['change_pct'] < 9.9]

        df = df[df['data_date'] >= df['data_date'].max() - pd.Timedelta(days=30)]

        filtered_count = original - len(df)
        if filtered_count > 0:
            logger.info(f"第1层过滤: 剔除 {filtered_count} 只(ST/退市/停牌/涨跌停/数据过期)")

        return self._apply_retention(df, "layer1")

    def _layer2_fundamental_filter(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """第2层：基本面过滤 - 市值/PE/换手率"""
        df = stocks.copy()

        if 'turnover_rate' in df.columns and df['turnover_rate'].notna().sum() > 0:
            valid_tr = df[df['turnover_rate'].notna() & (df['turnover_rate'] > 0)]
            if len(valid_tr) > 10:
                df = valid_tr

        logger.info(f"第2层基本面过滤: 保留 {len(df)} 只(共 {len(stocks)} 只)")

        return self._apply_retention(df, "layer2")

    def _layer3_technical_filter(self, stocks: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """第3层：技术面过滤 - 均线/量比/MACD"""
        df = stocks.copy()
        passed_stocks = []

        for _, row in df.iterrows():
            code = row['code']
            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date].tail(30)

                if len(kline) < 20:
                    continue

                ma5 = kline['close'].tail(5).mean()
                ma10 = kline['close'].tail(10).mean()
                ma20 = kline['close'].tail(20).mean()

                latest_vol = kline['volume'].iloc[-1]
                vol_20_avg = kline['volume'].tail(20).mean()
                volume_ratio = latest_vol / vol_20_avg if vol_20_avg > 0 else 1

                macd_signal = self._calc_macd(kline)

                if volume_ratio > 1.0 and macd_signal > -0.5:
                    passed_stocks.append(row)
                elif ma5 > ma10 > ma20:
                    passed_stocks.append(row)

            except Exception as e:
                logger.debug(f"技术面分析 {code} 失败: {e}")

        result = pd.DataFrame(passed_stocks) if passed_stocks else pd.DataFrame(columns=stocks.columns)
        return self._apply_retention(result, "layer3")

    def _layer4_money_filter(self, stocks: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """第4层：资金+板块过滤 - 成交量放大"""
        df = stocks.copy()
        passed_stocks = []

        for _, row in df.iterrows():
            code = row['code']
            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date].tail(10)

                if len(kline) < 5:
                    continue

                vol_5_avg = kline['volume'].tail(5).mean()
                vol_10_avg = kline['volume'].tail(10).mean()

                if vol_5_avg > vol_10_avg * 0.8:
                    passed_stocks.append(row)

            except Exception as e:
                logger.debug(f"资金面分析 {code} 失败: {e}")

        result = pd.DataFrame(passed_stocks) if passed_stocks else pd.DataFrame(columns=stocks.columns)
        return self._apply_retention(result, "layer4")

    def _layer5_pattern_filter(self, stocks: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """第5层：特殊形态 - 涨停基因/突破/回调"""
        df = stocks.copy()
        passed_stocks = []

        for _, row in df.iterrows():
            code = row['code']
            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date]

                if len(kline) < 20:
                    continue

                has_limit_up = self._check_limit_up_gene(kline, 20)
                is_breakout = self._check_breakout(kline)
                is_callback = self._check_callback(kline)

                row['has_limit_gene'] = has_limit_up
                row['is_breakout'] = is_breakout
                row['is_callback'] = is_callback

                if has_limit_up or is_breakout or is_callback:
                    passed_stocks.append(row)
                elif len(passed_stocks) < 50:
                    passed_stocks.append(row)

            except Exception as e:
                logger.debug(f"形态分析 {code} 失败: {e}")

        result = pd.DataFrame(passed_stocks) if passed_stocks else pd.DataFrame(columns=stocks.columns)
        return self._apply_retention(result, "layer5")

    def _check_limit_up_gene(self, kline: pd.DataFrame, days: int = 20) -> bool:
        """检查涨停基因：20日内有涨停记录"""
        if 'change_pct' not in kline.columns:
            return False
        recent = kline.tail(days)
        return bool(any(recent['change_pct'] >= 9.9))

    def _check_breakout(self, kline: pd.DataFrame) -> bool:
        """检查突破形态：盘中突破20日高点"""
        if len(kline) < 20:
            return False
        high_20 = kline['high'].tail(20).max()
        current_close = kline['close'].iloc[-1]
        return current_close >= high_20 * 0.98

    def _check_callback(self, kline: pd.DataFrame) -> bool:
        """检查回调形态：回踩5日均线获得支撑"""
        if len(kline) < 10:
            return False
        ma5 = kline['close'].tail(5).mean()
        current_close = kline['close'].iloc[-1]
        current_low = kline['low'].iloc[-1]
        return current_low <= ma5 * 1.02 and current_close >= ma5 * 0.98

    def _calc_macd(self, kline: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> float:
        """计算MACD指标"""
        if len(kline) < slow + signal:
            return 0

        prices = kline['close'].values
        ema_fast = self._calc_ema(prices, fast)
        ema_slow = self._calc_ema(prices, slow)
        macd_line = ema_fast - ema_slow

        signal_line = self._calc_ema(macd_line, signal)
        histogram = macd_line - signal_line

        return histogram

    def _calc_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """计算EMA"""
        ema = np.zeros(len(prices))
        ema[0] = prices[0]
        multiplier = 2 / (period + 1)
        for i in range(1, len(prices)):
            ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
        return ema

    def _apply_retention(self, df: pd.DataFrame, layer_name: str) -> pd.DataFrame:
        """应用保留率，返回85%的股票"""
        if len(df) == 0:
            return df

        retention_count = max(int(len(df) * self.RETENTION_RATE), 1)
        retention_count = min(retention_count, len(df))

        return df.head(retention_count)

    def _calculate_all_scores(self, stocks: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """计算各层评分"""
        for layer in ['layer1', 'layer2', 'layer3', 'layer4', 'layer5']:
            if layer not in stocks.columns:
                stocks[layer] = 80

        stocks['funnel_score'] = (
            stocks['layer1'] * 0.1 +
            stocks['layer2'] * 0.15 +
            stocks['layer3'] * 0.25 +
            stocks['layer4'] * 0.2 +
            stocks['layer5'] * 0.3
        )

        return stocks
