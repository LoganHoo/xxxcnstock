"""
策略引擎
实现资金行为学系统的策略层逻辑
"""
import polars as pl
from typing import Dict, Any, List
import logging

from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
from core.fund_behavior_config import config_manager

logger = logging.getLogger(__name__)


class FundBehaviorStrategyEngine:
    """资金行为学策略引擎"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.FundBehaviorStrategyEngine")
        self.indicator_engine = FundBehaviorIndicatorEngine()
    
    def select_trend_stocks(self, data: pl.DataFrame) -> List[str]:
        """
        选择波段趋势股票
        选股：主线板块且处于MA5之上的个股
        
        Args:
            data: 包含因子数据的DataFrame
        
        Returns:
            选中的股票代码列表
        """
        # 筛选MA5之上的股票
        trend_stocks = data.filter(
            (pl.col("factor_ma5_bias") > 0)
        )
        
        # 按MA5偏差排序，选择前N只
        trend_stocks = trend_stocks.sort("factor_ma5_bias", descending=True)
        
        # 获取股票代码
        selected_codes = trend_stocks["code"].unique().to_list()
        
        self.logger.info(f"波段趋势选股: {len(selected_codes)} 只股票")
        return selected_codes
    
    def select_short_term_stocks(self, data: pl.DataFrame, upward_pivot: bool) -> List[str]:
        """
        选择短线打板股票
        选股：情绪先锋、热点龙回头
        
        Args:
            data: 包含因子数据的DataFrame
            upward_pivot: 10点定基调是否向上
        
        Returns:
            选中的股票代码列表
        """
        if not upward_pivot:
            self.logger.info("10点定基调向下，不进行短线选股")
            return []
        
        # 筛选情绪得分高的股票
        short_term_stocks = data.filter(
            (pl.col("factor_limit_up_score") > 0)
        )
        
        # 按情绪得分排序，选择前N只
        short_term_stocks = short_term_stocks.sort("factor_limit_up_score", descending=True)
        
        # 获取股票代码
        selected_codes = short_term_stocks["code"].unique().to_list()
        
        self.logger.info(f"短线打板选股: {len(selected_codes)} 只股票")
        return selected_codes
    
    def calculate_position_size(self, total_capital: float) -> Dict[str, float]:
        """
        计算仓位大小
        
        Args:
            total_capital: 总资金
        
        Returns:
            各轨道的资金分配
        """
        # 从配置中获取仓位比例
        position_config = config_manager.get('strategy.position', {
            'trend': 0.5,
            'short_term': 0.4,
            'cash': 0.1
        })
        
        return {
            "trend": total_capital * position_config.get('trend', 0.5),
            "short_term": total_capital * position_config.get('short_term', 0.4),
            "cash": total_capital * position_config.get('cash', 0.1)
        }
    
    def four_step_exit_strategy(self, stock_data: Dict[str, Any], current_time: str) -> Dict[str, float]:
        """
        四步取关法
        
        Args:
            stock_data: 股票数据
            current_time: 当前时间（HH:MM格式）
        
        Returns:
            减仓比例
        """
        exit_ratio = 0.0
        action = "hold"
        
        # 09:26：未封一字，撤1/4
        if current_time == "09:26":
            if not stock_data.get("is_limit_up_open", False):
                exit_ratio += 0.25
                action = "sell_25"
        
        # 盘中：破黄线，撤1/4
        elif "10:00" < current_time < "14:56":
            if stock_data.get("price") < stock_data.get("vwap", 0):
                exit_ratio += 0.25
                action = "sell_25_on_break"
        
        # 10:00：未涨停/炸板，撤1/4
        elif current_time == "10:00":
            if not stock_data.get("is_limit_up", False):
                exit_ratio += 0.25
                action = "sell_25_at_10am"
        
        # 14:56：未封板，清仓
        elif current_time == "14:56":
            if not stock_data.get("is_limit_up", False) or stock_data.get("price") < stock_data.get("yesterday_close", 0):
                exit_ratio = 1.0  # 清仓
                action = "clear_all"
        
        return {"exit_ratio": exit_ratio, "action": action}
    
    def calculate_hedge_effect(self, data: pl.DataFrame) -> bool:
        """
        计算筹码与动能对冲效果
        
        Args:
            data: 包含因子数据的DataFrame
        
        Returns:
            对冲效果（True/False）
        """
        # 从配置中获取阈值
        hedge_config = config_manager.get('indicators.hedge', {
            'support_level': 4067,
            'v_total_threshold': 1800  # 亿（实际数据约1600-1900亿）
        })
        
        # 计算市场平均指标（v_total已经是每日市场总成交额，每只股票值相同）
        market_avg = data.group_by("trade_date").agg([
            pl.mean("factor_v_ratio10").alias("avg_v_ratio10"),
            pl.mean("factor_v_total").alias("avg_v_total"),
            pl.mean("close").alias("avg_close")
        ]).to_dict(as_series=False)

        # 获取最新数据
        if not market_avg.get("avg_v_ratio10") or not market_avg.get("avg_close") or not market_avg.get("avg_v_total"):
            return False

        v_ratio10 = market_avg["avg_v_ratio10"][-1]
        v_total = market_avg["avg_v_total"][-1]
        price = market_avg["avg_close"][-1]
        
        # 放量有效性检查（早盘持续放量）
        # 量能定心丸：V_total ≥ 2.8万亿
        # 逻辑对冲公式：Effect = (V_10am_ratio > 1.1) + (Price > Support_4067)
        effect = (v_ratio10 > 1.1) and (price > hedge_config['support_level']) and (v_total >= hedge_config['v_total_threshold'])
        
        return effect

    def execute_strategy(self, data: pl.DataFrame, total_capital: float, current_time: str) -> Dict[str, Any]:
        """
        执行完整策略
        
        Args:
            data: 包含所有因子数据的DataFrame
            total_capital: 总资金
            current_time: 当前时间
        
        Returns:
            策略执行结果
        """
        # 计算指标
        indicators = self.indicator_engine.calculate_all_indicators(data)
        
        # 获取10点定基调信号
        upward_pivot = False
        if indicators["10am_pivot"].get("upward_pivot"):
            upward_pivot = indicators["10am_pivot"]["upward_pivot"][-1] if indicators["10am_pivot"]["upward_pivot"] else False
        
        # 计算对冲效果
        hedge_effect = self.calculate_hedge_effect(data)
        
        # 选股
        trend_stocks = self.select_trend_stocks(data)
        short_term_stocks = self.select_short_term_stocks(data, upward_pivot)
        
        # 计算仓位
        position_size = self.calculate_position_size(total_capital)
        
        # 计算减仓信号
        exit_signals = {}
        for code in short_term_stocks:
            # 模拟股票数据
            stock_data = {
                "is_limit_up_open": False,  # 实际需要从实时数据获取
                "price": 0.0,  # 实际需要从实时数据获取
                "vwap": 0.0,  # 实际需要从实时数据获取
                "is_limit_up": False,  # 实际需要从实时数据获取
                "yesterday_close": 0.0  # 实际需要从实时数据获取
            }
            exit_signals[code] = self.four_step_exit_strategy(stock_data, current_time)
        
        # 筹码荣枯线判定
        cost_peak = 0.0
        cost_peak_list = []
        v_total_list = []
        sentiment_temp = 0.0
        delta_temp = 0.0

        if indicators["market_sentiment"].get("avg_cost_peak"):
            cost_peak_list = indicators["market_sentiment"]["avg_cost_peak"]
            cost_peak = cost_peak_list[-1] if cost_peak_list else 0.0

        if indicators["market_sentiment"].get("avg_v_total"):
            v_total_list = indicators["market_sentiment"]["avg_v_total"]

        if indicators["market_sentiment"].get("sentiment_temperature"):
            sentiment_temp = indicators["market_sentiment"]["sentiment_temperature"][-1]

        if indicators["market_sentiment"].get("delta_temperature"):
            delta_temp = indicators["market_sentiment"]["delta_temperature"][-1]

        # 动态计算筹码荣枯线（使用最近的筹码峰位值）
        current_price = data["close"].to_numpy()[-1] if len(data) > 0 else 0
        is_strong_region = cost_peak > 0 and current_price > cost_peak * 0.995

        result = {
            "market_state": indicators["market_sentiment"].get("market_state", []),
            "upward_pivot": upward_pivot,
            "hedge_effect": hedge_effect,
            "is_strong_region": is_strong_region,
            "trend_stocks": trend_stocks,
            "short_term_stocks": short_term_stocks,
            "position_size": position_size,
            "exit_signals": exit_signals,
            # 真实数据
            "cost_peak": cost_peak,
            "current_price": current_price,
            "v_total": v_total_list[-1] if v_total_list else 0.0,
            "sentiment_temperature": sentiment_temp,
            "delta_temperature": delta_temp,
            "market_sentiment_indicators": {
                "avg_cost_peak": cost_peak_list[-1] if cost_peak_list else 0.0,
                "avg_v_total": v_total_list[-1] if v_total_list else 0.0,
                "avg_limit_up_score": indicators["market_sentiment"].get("avg_limit_up_score", [0])[-1],
                "consecutive_height": indicators["market_sentiment"].get("consecutive_height", [0])[-1],
                "total_limit_up": indicators["market_sentiment"].get("total_limit_up", [0])[-1],
                "inertia_signal": indicators["market_sentiment"].get("inertia_signal", ["Normal"])[-1]
            }
        }

        return result
