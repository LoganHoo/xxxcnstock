"""
漏斗选股 AI 评分模块
生成持仓建议和关键价位
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List
from datetime import datetime

from core.logger import setup_logger

logger = setup_logger("funnel_ai_scorer")


class FunnelAIScorer:
    """漏斗选股 AI 评分器"""

    def __init__(self, data_dir: str = "data/kline"):
        self.data_dir = Path(data_dir)

    def score_and_recommend(self, stocks: pd.DataFrame, trade_date: str, top_n: int = 50) -> pd.DataFrame:
        """
        AI综合评分并生成持仓建议

        Args:
            stocks: 漏斗筛选后的股票 DataFrame
            trade_date: 交易日期
            top_n: 最终推荐数量

        Returns:
            带AI评分和持仓建议的 DataFrame
        """
        logger.info(f"开始AI综合评分, 输入股票数: {len(stocks)}")

        results = []

        for _, row in stocks.iterrows():
            code = row['code']
            try:
                analysis = self._analyze_stock(code, trade_date)
                if analysis:
                    recommendation = self._generate_recommendation(row, analysis)
                    results.append(recommendation)
            except Exception as e:
                logger.debug(f"AI分析 {code} 失败: {e}")

        if not results:
            logger.warning("AI评分为空")
            return pd.DataFrame()

        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values('ai_score', ascending=False)
        df_result = df_result.head(top_n)

        logger.info(f"AI评分完成, 推荐股票数: {len(df_result)}")
        return df_result

    def _analyze_stock(self, code: str, trade_date: str) -> Dict:
        """深度分析单只股票"""
        file_path = self.data_dir / f"{code}.parquet"
        if not file_path.exists():
            return None

        kline = pd.read_parquet(file_path)
        date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
        kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
        kline = kline[kline[date_col] <= trade_date].tail(60)

        if len(kline) < 30:
            return None

        latest = kline.iloc[-1]
        prev = kline.iloc[-2] if len(kline) >= 2 else latest

        close = latest.get('close', 0)
        prev_close = prev.get('close', close)
        change_pct = ((close - prev_close) / prev_close * 100) if prev_close > 0 else 0

        volume_5_avg = kline['volume'].tail(5).mean()
        volume_20_avg = kline['volume'].tail(20).mean()
        volume_ratio = latest['volume'] / volume_20_avg if volume_20_avg > 0 else 1

        turnover_rate = latest.get('turnover_rate', 0) * 100

        ma5 = kline['close'].tail(5).mean()
        ma10 = kline['close'].tail(10).mean()
        ma20 = kline['close'].tail(20).mean()
        ma60 = kline['close'].tail(60).mean() if len(kline) >= 60 else ma20

        high_20 = kline['high'].tail(20).max()
        low_20 = kline['low'].tail(20).min()

        volatility = kline['close'].tail(20).std() / ma20 if ma20 > 0 else 0

        limit_up_count = len(kline[kline['change_pct'] >= 9.9])
        limit_down_count = len(kline[kline['change_pct'] <= -9.9])

        macd = self._calc_macd_signal(kline)

        return {
            'close': close,
            'change_pct': change_pct,
            'volume_ratio': volume_ratio,
            'turnover_rate': turnover_rate,
            'ma5': ma5,
            'ma10': ma10,
            'ma20': ma20,
            'ma60': ma60,
            'high_20': high_20,
            'low_20': low_20,
            'volatility': volatility,
            'limit_up_count': limit_up_count,
            'limit_down_count': limit_down_count,
            'macd': macd,
        }

    def _calc_macd_signal(self, kline: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> float:
        """计算MACD"""
        if len(kline) < slow + signal:
            return 0

        prices = kline['close'].values
        ema_fast = self._calc_ema(prices, fast)
        ema_slow = self._calc_ema(prices, slow)
        macd_line = ema_fast - ema_slow
        signal_line = self._calc_ema(macd_line, signal)

        return macd_line[-1] - signal_line[-1]

    def _calc_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """计算EMA"""
        ema = np.zeros(len(prices))
        ema[0] = prices[0]
        multiplier = 2 / (period + 1)
        for i in range(1, len(prices)):
            ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
        return ema

    def _generate_recommendation(self, stock: pd.Series, analysis: Dict) -> Dict:
        """生成持仓建议"""
        close = analysis['close']

        entry_price = close
        stoploss_price = round(close * 0.95, 2)
        take_profit_1 = round(close * 1.10, 2)
        take_profit_2 = round(close * 1.20, 2)

        support_price = round(analysis['ma20'] * 0.98, 2)
        resistance_price = round(analysis['high_20'] * 1.02, 2)

        ai_score = self._calculate_ai_score(stock, analysis)

        return {
            'code': stock['code'],
            'name': stock.get('name', ''),
            'trade_date': stock.get('trade_date', ''),
            'funnel_score': stock.get('funnel_score', 0),
            'layer1_score': stock.get('layer1', 0),
            'layer2_score': stock.get('layer2', 0),
            'layer3_score': stock.get('layer3', 0),
            'layer4_score': stock.get('layer4', 0),
            'layer5_score': stock.get('layer5', 0),
            'ai_score': ai_score,
            'entry_price': entry_price,
            'stoploss_price': stoploss_price,
            'take_profit_1': take_profit_1,
            'take_profit_2': take_profit_2,
            'support_price': support_price,
            'resistance_price': resistance_price,
            'current_price': close,
            'change_pct': analysis['change_pct'],
            'volume_ratio': analysis['volume_ratio'],
            'turnover_rate': analysis['turnover_rate'],
            'limit_up_count': analysis['limit_up_count'],
            'volatility': round(analysis['volatility'] * 100, 2),
            'trend': '上涨' if analysis['ma5'] > analysis['ma20'] else '震荡',
        }

    def _calculate_ai_score(self, stock: pd.Series, analysis: Dict) -> float:
        """计算AI综合评分"""
        score = 0

        funnel_score = stock.get('funnel_score', 50)
        score += funnel_score * 0.3

        if analysis['change_pct'] >= 5:
            score += 20
        elif analysis['change_pct'] >= 3:
            score += 15
        elif analysis['change_pct'] >= 0:
            score += 10

        if analysis['volume_ratio'] >= 2:
            score += 15
        elif analysis['volume_ratio'] >= 1.5:
            score += 10

        if analysis['turnover_rate'] >= 5:
            score += 10
        elif analysis['turnover_rate'] >= 3:
            score += 5

        if analysis['ma5'] > analysis['ma10'] > analysis['ma20']:
            score += 15

        if analysis['macd'] > 0:
            score += 10

        score += min(analysis['limit_up_count'] * 3, 15)

        if analysis['volatility'] < 0.05:
            score += 5

        return min(score, 100)
