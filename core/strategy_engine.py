"""
策略引擎
负责组合因子和执行选股
"""
import yaml
import polars as pl
from pathlib import Path
from typing import List
import logging

from core.factor_engine import FactorEngine

logger = logging.getLogger(__name__)


class StrategyEngine:
    """策略引擎"""
    
    def __init__(self, strategy_config: str, factor_engine: FactorEngine = None):
        self.config_path = Path(strategy_config)
        self.config = self._load_config()
        self.factor_engine = factor_engine or FactorEngine()
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self) -> dict:
        """加载策略配置"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    @property
    def strategy_name(self) -> str:
        """策略名称"""
        return self.config["strategy"]["name"]
    
    @property
    def factors(self) -> List[dict]:
        """因子配置"""
        return self.config["strategy"]["factors"]
    
    @property
    def filters(self) -> List[dict]:
        """筛选条件"""
        return self.config["strategy"].get("filters", [])
    
    @property
    def output_config(self) -> dict:
        """输出配置"""
        return self.config["strategy"]["output"]
    
    def calculate_factor_scores(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算所有因子得分"""
        df = data.clone()
        
        for factor_config in self.factors:
            factor_name = factor_config["name"]
            params = factor_config.get("params")
            
            self.logger.debug(f"计算因子: {factor_name}")
            df = self.factor_engine.calculate_factor(df, factor_name, params)
        
        return df
    
    def calculate_weighted_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算加权综合得分"""
        score_expr = pl.lit(0.0)
        
        for factor_config in self.factors:
            factor_name = factor_config["name"]
            weight = factor_config["weight"]
            threshold = factor_config.get("threshold", 0)
            
            factor_col = f"factor_{factor_name}"
            
            score_expr = score_expr + pl.when(
                pl.col(factor_col) >= threshold
            ).then(
                pl.col(factor_col) * weight
            ).otherwise(
                pl.lit(0.0)
            )
        
        return df.with_columns([
            score_expr.alias("strategy_score")
        ])
    
    def apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用筛选条件"""
        for f in self.filters:
            filter_type = f["type"]
            
            if filter_type == "price":
                df = df.filter(
                    (pl.col("close") >= f["min"]) & 
                    (pl.col("close") <= f["max"])
                )
            elif filter_type == "change_pct":
                if "change_pct" in df.columns:
                    df = df.filter(
                        (pl.col("change_pct") >= f["min"]) & 
                        (pl.col("change_pct") <= f["max"])
                    )
            elif filter_type == "market_cap":
                pass
        
        return df
    
    def select_stocks(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        执行选股
        
        Args:
            data: K线数据 DataFrame
        
        Returns:
            选中的股票 DataFrame
        """
        self.logger.info(f"开始执行策略: {self.strategy_name}")
        
        df = self.calculate_factor_scores(data)
        
        df = self.calculate_weighted_score(df)
        
        df = self.apply_filters(df)
        
        min_score = self.output_config.get("min_score", 0)
        df = df.filter(pl.col("strategy_score") >= min_score)
        
        top_n = self.output_config.get("top_n", 20)
        df = df.sort("strategy_score", descending=True).head(top_n)
        
        self.logger.info(f"选出 {len(df)} 只股票")
        
        return df
    
    def get_strategy_info(self) -> dict:
        """获取策略信息"""
        return {
            "name": self.strategy_name,
            "description": self.config["strategy"].get("description", ""),
            "version": self.config["strategy"].get("version", "1.0"),
            "factors": [
                {"name": f["name"], "weight": f["weight"]}
                for f in self.factors
            ],
            "filters": self.filters,
            "output": self.output_config
        }
