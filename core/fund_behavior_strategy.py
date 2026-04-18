"""
策略引擎
实现资金行为学系统的策略层逻辑
"""
import polars as pl
from typing import Dict, Any, List, Tuple
import logging

from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
from core.unified_config import config, StrategyConfig, FactorConfig

logger = logging.getLogger(__name__)


class FundBehaviorStrategyEngine:
    """资金行为学策略引擎"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.FundBehaviorStrategyEngine")
        self.indicator_engine = FundBehaviorIndicatorEngine()
    
    def select_trend_stocks(self, data: pl.DataFrame, max_stocks: int = 50) -> Tuple[List[str], pl.DataFrame]:
        """
        选择波段趋势股票（精选版）
        选股：多因子综合评分，精选优质趋势股

        Args:
            data: 包含因子数据的DataFrame
            max_stocks: 最大选股数量，默认50只

        Returns:
            (选中的股票代码列表, 选股结果DataFrame包含composite_score)
        """
        # 基础筛选条件（过滤掉688开头的科创板股票）
        # 将code列转换为字符串以进行字符串操作
        # 计算MA5斜率（使用close价格的5日变化率作为替代）
        if "ma5_slope" not in data.columns:
            data = data.with_columns([
                ((pl.col("close") - pl.col("close").shift(5).over("code")) /
                 pl.col("close").shift(5).over("code") * 100).alias("ma5_slope")
            ])

        trend_stocks = data.with_columns([
            pl.col("code").cast(pl.Utf8).alias("code_str")
        ]).filter(
            (pl.col("factor_ma5_bias") > 0) &  # MA5之上
            (pl.col("ma5_slope") > 0) &   # MA5趋势向上
            (pl.col("factor_v_ratio10") > 1.0) &  # 成交量放大
            (~pl.col("code_str").str.starts_with("688"))  # 过滤科创板
        ).drop("code_str")

        # 计算综合评分（多因子加权）
        trend_stocks = trend_stocks.with_columns([
            (
                pl.col("factor_ma5_bias") * 0.3 +
                pl.col("ma5_slope") * 0.2 +
                pl.col("factor_v_ratio10") * 0.2 +
                pl.col("factor_limit_up_score") * 0.15 +
                pl.col("factor_cost_peak") * 0.15
            ).alias("composite_score")
        ])

        # 按综合评分排序，选择前N只
        trend_stocks = trend_stocks.sort("composite_score", descending=True)

        # 获取股票代码（限制数量）
        selected_codes = trend_stocks["code"].unique().to_list()[:max_stocks]

        self.logger.info(f"波段趋势选股: {len(selected_codes)} 只股票（精选前{max_stocks}）")
        return selected_codes, trend_stocks
    
    def select_short_term_stocks(self, data: pl.DataFrame, upward_pivot: bool, max_stocks: int = 20) -> Tuple[List[str], pl.DataFrame]:
        """
        选择短线打板股票（改进版）
        选股：情绪先锋、热点龙回头，即使10点定基调向下也选少量强势股

        Args:
            data: 包含因子数据的DataFrame
            upward_pivot: 10点定基调是否向上
            max_stocks: 最大选股数量，默认20只

        Returns:
            (选中的股票代码列表, 选股结果DataFrame包含short_term_score)
        """
        # 筛选情绪得分高的股票（过滤掉688开头的科创板股票，优先选择涨停股）
        # 将code列转换为字符串以进行字符串操作
        short_term_stocks = data.with_columns([
            pl.col("code").cast(pl.Utf8).alias("code_str")
        ]).filter(
            ((pl.col("factor_limit_up_score") > 0) |
            (pl.col("factor_pioneer_status") > 0)) &
            (~pl.col("code_str").str.starts_with("688"))  # 过滤科创板
        ).drop("code_str")

        # 如果10点定基调向下，进一步筛选最强壮的股
        if not upward_pivot:
            self.logger.info("10点定基调向下，精选最强短线股")
            short_term_stocks = short_term_stocks.filter(
                (pl.col("factor_ma5_bias") > 0) &  # 仍在MA5之上
                (pl.col("factor_v_ratio10") > 1.5)  # 成交量明显放大
            )
            # 限制数量更少
            max_stocks = min(max_stocks, 10)

        # 计算综合得分：涨停优先 + 情绪得分 + 涨幅
        # 如果is_limit_up列不存在，使用factor_limit_up_score > 10作为涨停判断
        if "is_limit_up" not in short_term_stocks.columns:
            short_term_stocks = short_term_stocks.with_columns([
                (pl.col("factor_limit_up_score") > 10).cast(pl.Int64).alias("is_limit_up")
            ])

        short_term_stocks = short_term_stocks.with_columns([
            (
                pl.col("is_limit_up") * 100 +  # 涨停加100分（最高优先级）
                pl.col("factor_limit_up_score") * 10 +  # 情绪得分
                pl.col("factor_ma5_bias").clip(0, 10)  # MA5偏差（限制最大10分）
            ).alias("short_term_score")
        ])

        # 按综合得分排序（涨停优先）
        short_term_stocks = short_term_stocks.sort("short_term_score", descending=True)

        # 获取股票代码（限制数量）
        selected_codes = short_term_stocks["code"].unique().to_list()[:max_stocks]

        self.logger.info(f"短线打板选股: {len(selected_codes)} 只股票")
        return selected_codes, short_term_stocks
    
    def calculate_position_size(self, total_capital: float) -> Dict[str, float]:
        """
        计算仓位大小
        
        Args:
            total_capital: 总资金
        
        Returns:
            各轨道的资金分配
        """
        # 从配置中获取仓位比例
        position_config = StrategyConfig.get_position_weights()
        
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
        hedge_config = StrategyConfig.get_hedge_params()
        
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

    def _calculate_defense_signals(
        self,
        indicators: Dict[str, Any],
        v_total: float,
        current_price: float
    ) -> Dict[str, Any]:
        """
        计算防守信号 - 什么情况下绝对不能买
        
        Args:
            indicators: 技术指标
            v_total: 成交量
            current_price: 当前价格
        
        Returns:
            防守信号字典
        """
        # 获取防守配置
        defense_config = StrategyConfig.get_defense_params()
        
        # 关键位配置
        hedge_config = StrategyConfig.get_hedge_params()
        support_level = hedge_config.get('support_level', 4067)
        # 确保 support_levels 是列表
        if isinstance(support_level, int):
            support_levels = [support_level]
        else:
            support_levels = support_level
        key_levels_config = {
            'support_levels': support_levels,
            'resistance_levels': hedge_config.get('resistance_levels', [4117, 4140])
        }
        
        signals = {
            'action': 'BUY',  # 默认买入
            'reasons': [],
            'details': {}
        }
        
        # 1. 成交量萎缩防守
        avg_v_total = indicators.get("market_sentiment", {}).get("avg_v_total", [0])[-1]
        if avg_v_total > 0 and v_total / avg_v_total < defense_config['volume_shrink_threshold']:
            signals['action'] = 'DEFENSE'
            signals['reasons'].append('成交量萎缩60%以上，触发防守')
            signals['details']['volume_shrink'] = True
        
        # 2. 开盘金额不足防守
        # 需要从实时数据获取开盘金额，这里用v_total模拟
        # 实际应该从09:25集合竞价数据获取
        if v_total < defense_config['open_auction_min']:
            signals['action'] = 'DEFENSE'
            signals['reasons'].append(f'开盘金额不足{v_total}亿，低于{defense_config["open_auction_min"]}亿阈值')
            signals['details']['open_auction_low'] = True
        
        # 3. 跌破支撑位防守
        support_levels = key_levels_config.get('support_levels', [4067])
        for support in support_levels:
            if current_price < support * (1 + defense_config['support_break_threshold']):
                signals['action'] = 'DEFENSE'
                signals['reasons'].append(f'跌破关键支撑位{int(support)}，触发防守')
                signals['details']['support_break'] = True
                break
        
        # 4. 情绪过热防守
        sentiment_temp = indicators.get("market_sentiment", {}).get("sentiment_temperature", [0])[-1]
        if sentiment_temp > 85:
            signals['action'] = 'CAUTION'
            signals['reasons'].append(f'情绪温度{sentiment_temp}°过高，注意风险')
            signals['details']['sentiment_overheat'] = True
        
        # 5. 判断当前处于关键位附近
        near_support = False
        near_resistance = False
        for support in support_levels:
            if abs(current_price - support) / support < 0.01:
                near_support = True
                break
        resistance_levels = key_levels_config.get('resistance_levels', [4200])
        for resistance in resistance_levels:
            if abs(current_price - resistance) / resistance < 0.01:
                near_resistance = True
                break
        
        if near_support:
            signals['details']['near_support'] = True
        if near_resistance:
            signals['details']['near_resistance'] = True
        
        return signals
    
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
        trend_stocks, trend_stocks_df = self.select_trend_stocks(data)
        short_term_stocks, short_term_stocks_df = self.select_short_term_stocks(data, upward_pivot)

        # 计算仓位
        position_size = self.calculate_position_size(total_capital)

        # 获取选股详细信息（用于报告展示和数据库保存）
        trend_stocks_detail = self._get_stocks_detail_from_selection(trend_stocks_df, trend_stocks)
        short_term_stocks_detail = self._get_stocks_detail_from_selection(short_term_stocks_df, short_term_stocks)

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

        # 计算防守信号
        defense_signals = self._calculate_defense_signals(
            indicators=indicators,
            v_total=v_total_list[-1] if v_total_list else 0.0,
            current_price=current_price
        )

        result = {
            "market_state": indicators["market_sentiment"].get("market_state", []),
            "upward_pivot": upward_pivot,
            "hedge_effect": hedge_effect,
            "is_strong_region": is_strong_region,
            "trend_stocks": trend_stocks,
            "short_term_stocks": short_term_stocks,
            "trend_stocks_detail": trend_stocks_detail,
            "short_term_stocks_detail": short_term_stocks_detail,
            "position_size": position_size,
            "exit_signals": exit_signals,
            # 防守信号
            "defense_signals": defense_signals,
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

    def _get_stocks_detail_from_selection(self, selection_df: pl.DataFrame, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        从选股结果DataFrame获取股票详细信息

        Args:
            selection_df: 选股结果DataFrame（包含composite_score或short_term_score）
            stock_codes: 股票代码列表

        Returns:
            股票代码到详细信息的字典
        """
        if not stock_codes or selection_df.is_empty():
            return {}

        # 加载股票名称映射
        stock_name_map = self._load_stock_name_map()

        # 获取每只股票的最新数据（按trade_date排序取第一条）
        latest_data = selection_df.sort("trade_date", descending=True).group_by("code").first()

        # 筛选出选中的股票
        selected_data = latest_data.filter(pl.col("code").is_in(stock_codes))

        # 构建详细信息字典
        stocks_detail = {}
        for row in selected_data.to_dicts():
            code = row.get("code", "")
            if code:
                # 根据选股类型确定score字段名
                if "composite_score" in row:
                    score = row.get("composite_score", 0.0)
                elif "short_term_score" in row:
                    score = row.get("short_term_score", 0.0)
                else:
                    score = 0.0

                # 从映射表获取股票名称，如果没有则使用空字符串
                name = stock_name_map.get(code, "")

                stocks_detail[code] = {
                    "name": name,
                    "close": row.get("close", 0.0),
                    "volume": row.get("volume", 0.0),
                    "v_ratio10": row.get("factor_v_ratio10", 0.0),
                    "v_total": row.get("factor_v_total", 0.0),
                    "cost_peak": row.get("factor_cost_peak", 0.0),
                    "limit_up_score": row.get("factor_limit_up_score", 0.0),
                    "pioneer_status": row.get("factor_pioneer_status", 0.0),
                    "ma5_bias": row.get("factor_ma5_bias", 0.0),
                    "score": score
                }

        return stocks_detail

    def _load_stock_name_map(self) -> Dict[str, str]:
        """
        加载股票名称映射表

        Returns:
            股票代码到名称的字典
        """
        from pathlib import Path

        stock_list_path = Path(__file__).parent.parent / "data" / "stock_list.parquet"
        name_map = {}

        if stock_list_path.exists():
            try:
                stock_list_df = pl.read_parquet(stock_list_path)
                # 确保code列是字符串类型
                if 'code' in stock_list_df.columns and 'name' in stock_list_df.columns:
                    stock_list_df = stock_list_df.with_columns([
                        pl.col("code").cast(pl.Utf8).alias("code")
                    ])
                    for row in stock_list_df.to_dicts():
                        code = row.get("code", "")
                        name = row.get("name", "")
                        if code:
                            name_map[code] = name
                    self.logger.debug(f"已加载 {len(name_map)} 只股票名称映射")
            except Exception as e:
                self.logger.warning(f"加载股票名称映射失败: {e}")
        else:
            self.logger.warning(f"股票列表文件不存在: {stock_list_path}")

        return name_map
