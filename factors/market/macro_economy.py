"""
宏观经济因子
模拟宏观经济指标对股票的影响
"""
import polars as pl
import numpy as np
from core.factor_library import BaseFactor, register_factor


@register_factor("macro_interest_rate")
class MacroInterestRateFactor(BaseFactor):
    """市场利率因子 - 模拟利率变化对股票的影响"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "macro_interest_rate",
            category=category or "macro",
            params=params or {"period": 20},
            description=description or "市场利率变化因子"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算利率因子 - 使用市场波动率作为利率变化的代理"""
        period = self.params.get("period", 20)
        
        # 使用市场波动率作为利率环境的代理
        # 高波动 = 紧张的市场环境 = 类似高利率环境
        data = data.with_columns([
            pl.col("close").rolling_std(window_size=period).over("code").alias("volatility")
        ])
        
        # 计算每日市场平均波动率
        daily_vol = data.group_by("trade_date").agg([
            pl.mean("volatility").alias("market_volatility")
        ])
        
        # 计算波动率变化 (作为利率变化的代理)
        daily_vol = daily_vol.with_columns([
            pl.col("market_volatility").shift(1).alias("market_vol_lag1")
        ])
        
        daily_vol = daily_vol.with_columns([
            ((pl.col("market_volatility") - pl.col("market_vol_lag1")) / 
             (pl.col("market_vol_lag1") + 0.001)).alias("interest_rate_change")
        ])
        
        # 合并回原始数据
        data = data.join(daily_vol.select(["trade_date", "interest_rate_change"]), 
                        on="trade_date", how="left")
        
        # 利率变化对股票的影响：
        # 利率上升(波动率上升)对成长股不利，对价值股影响较小
        data = data.with_columns([
            pl.col("interest_rate_change").fill_null(0).alias("factor_interest_rate")
        ])
        
        return data.drop(["volatility"])


@register_factor("macro_market_liquidity")
class MacroMarketLiquidityFactor(BaseFactor):
    """市场流动性因子 - 模拟市场流动性环境"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "macro_market_liquidity",
            category=category or "macro",
            params=params or {"period": 20},
            description=description or "市场流动性因子"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算流动性因子"""
        period = self.params.get("period", 20)
        
        # 使用成交量变化作为流动性代理
        data = data.with_columns([
            pl.col("volume").rolling_mean(window_size=period).over("code").alias("vol_ma")
        ])
        
        # 计算每日市场总成交量
        daily_volume = data.group_by("trade_date").agg([
            pl.sum("volume").alias("total_market_volume"),
            pl.sum("vol_ma").alias("total_market_vol_ma")
        ])
        
        # 计算流动性指标
        daily_volume = daily_volume.with_columns([
            (pl.col("total_market_volume") / (pl.col("total_market_vol_ma") + 1)).alias("liquidity_ratio")
        ])
        
        # 标准化流动性指标
        mean_liq = daily_volume.select(pl.mean("liquidity_ratio")).to_numpy()[0][0]
        std_liq = daily_volume.select(pl.std("liquidity_ratio")).to_numpy()[0][0]
        
        daily_volume = daily_volume.with_columns([
            ((pl.col("liquidity_ratio") - mean_liq) / (std_liq + 0.001)).alias("liquidity_score")
        ])
        
        # 合并回原始数据
        data = data.join(daily_volume.select(["trade_date", "liquidity_score"]), 
                        on="trade_date", how="left")
        
        data = data.with_columns([
            pl.col("liquidity_score").fill_null(0).alias("factor_market_liquidity")
        ])
        
        return data.drop(["vol_ma"])


@register_factor("macro_industry_cycle")
class MacroIndustryCycleFactor(BaseFactor):
    """行业周期因子 - 模拟行业景气度"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "macro_industry_cycle",
            category=category or "macro",
            params=params or {"period": 60},
            description=description or "行业周期景气度因子"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算行业周期因子"""
        period = self.params.get("period", 60)
        
        # 如果没有行业信息，使用代码前缀作为行业代理
        if "industry" not in data.columns:
            data = data.with_columns([
                pl.col("code").str.slice(0, 2).alias("industry")
            ])
        
        # 计算行业收益率
        data = data.with_columns([
            pl.col("close").pct_change().over("code").alias("daily_return")
        ])
        
        # 计算行业平均收益率
        industry_return = data.group_by(["trade_date", "industry"]).agg([
            pl.mean("daily_return").alias("industry_avg_return")
        ])
        
        # 计算行业动量 (60日累计收益)
        industry_return = industry_return.with_columns([
            pl.col("industry_avg_return").rolling_sum(window_size=period)
            .over(["industry"]).alias("industry_momentum")
        ])
        
        # 合并回原始数据
        data = data.join(industry_return.select(["trade_date", "industry", "industry_momentum"]), 
                        on=["trade_date", "industry"], how="left")
        
        data = data.with_columns([
            pl.col("industry_momentum").fill_null(0).alias("factor_industry_cycle")
        ])
        
        return data.drop(["daily_return", "industry_momentum"])


@register_factor("macro_market_sentiment")
class MacroMarketSentimentFactor(BaseFactor):
    """市场情绪因子 - 综合多种情绪指标"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "macro_market_sentiment",
            category=category or "macro",
            params=params or {"period": 20},
            description=description or "市场情绪综合因子"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算市场情绪因子"""
        period = self.params.get("period", 20)
        
        # 计算涨跌家数比
        data = data.with_columns([
            pl.col("close").pct_change().over("code").alias("daily_return")
        ])
        
        data = data.with_columns([
            (pl.col("daily_return") > 0).cast(pl.Int64).alias("is_up")
        ])
        
        daily_sentiment = data.group_by("trade_date").agg([
            pl.sum("is_up").alias("up_count"),
            pl.count("code").alias("total_count")
        ])
        
        daily_sentiment = daily_sentiment.with_columns([
            (pl.col("up_count").cast(pl.Float64) / pl.col("total_count")).alias("advance_ratio")
        ])
        
        # 计算情绪动量
        daily_sentiment = daily_sentiment.with_columns([
            pl.col("advance_ratio").rolling_mean(window_size=period).alias("sentiment_ma")
        ])
        
        daily_sentiment = daily_sentiment.with_columns([
            (pl.col("advance_ratio") - pl.col("sentiment_ma")).alias("sentiment_change")
        ])
        
        # 合并回原始数据
        data = data.join(daily_sentiment.select(["trade_date", "sentiment_change"]), 
                        on="trade_date", how="left")
        
        data = data.with_columns([
            pl.col("sentiment_change").fill_null(0).alias("factor_market_sentiment_macro")
        ])
        
        return data.drop(["daily_return", "is_up", "sentiment_change"])


@register_factor("macro_risk_appetite")
class MacroRiskAppetiteFactor(BaseFactor):
    """风险偏好因子 - 衡量市场风险偏好变化"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "macro_risk_appetite",
            category=category or "macro",
            params=params or {"period": 20},
            description=description or "市场风险偏好因子"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算风险偏好因子"""
        period = self.params.get("period", 20)
        
        # 使用波动率和收益率的关系衡量风险偏好
        data = data.with_columns([
            pl.col("close").pct_change().over("code").alias("daily_return")
        ])
        
        # 计算个股波动率
        data = data.with_columns([
            pl.col("daily_return").rolling_std(window_size=period).over("code").alias("stock_volatility")
        ])
        
        # 按日期聚合计算市场风险偏好
        # 高波动 + 正收益 = 高风险偏好
        # 高波动 + 负收益 = 恐慌
        daily_risk = data.group_by("trade_date").agg([
            pl.mean("stock_volatility").alias("avg_volatility"),
            pl.mean("daily_return").alias("avg_return")
        ])
        
        daily_risk = daily_risk.with_columns([
            (pl.col("avg_return") / (pl.col("avg_volatility") + 0.001)).alias("risk_appetite")
        ])
        
        # 标准化
        mean_ra = daily_risk.select(pl.mean("risk_appetite")).to_numpy()[0][0]
        std_ra = daily_risk.select(pl.std("risk_appetite")).to_numpy()[0][0]
        
        daily_risk = daily_risk.with_columns([
            ((pl.col("risk_appetite") - mean_ra) / (std_ra + 0.001)).alias("risk_appetite_score")
        ])
        
        # 合并回原始数据
        data = data.join(daily_risk.select(["trade_date", "risk_appetite_score"]), 
                        on="trade_date", how="left")
        
        data = data.with_columns([
            pl.col("risk_appetite_score").fill_null(0).alias("factor_risk_appetite")
        ])
        
        return data.drop(["daily_return", "stock_volatility", "risk_appetite_score"])
