"""
因子计算引擎
负责加载因子配置和计算因子值
"""
import yaml
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import importlib
import logging

from core.factor_library import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


class FactorEngine:
    """因子计算引擎"""
    
    def __init__(self, config_dir: str = "config/factors"):
        self.config_dir = Path(config_dir)
        self.factor_configs: Dict[str, dict] = {}
        self._load_factor_configs()
    
    def _load_factor_configs(self):
        """加载所有因子配置"""
        if not self.config_dir.exists():
            logger.warning(f"因子配置目录不存在: {self.config_dir}")
            return
        
        for config_file in self.config_dir.glob("*.yaml"):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    
                    if config is None:
                        continue
                    
                    factors_data = config.get("factors", {})
                    
                    if isinstance(factors_data, list):
                        for factor_config in factors_data:
                            name = factor_config["name"]
                            self.factor_configs[name] = factor_config
                            logger.debug(f"加载因子配置: {name}")
                    elif isinstance(factors_data, dict):
                        for category, factors in factors_data.items():
                            if isinstance(factors, list):
                                for factor_config in factors:
                                    name = factor_config["name"]
                                    factor_config["category"] = category
                                    self.factor_configs[name] = factor_config
                                    logger.debug(f"加载因子配置: {name} ({category})")
            except Exception as e:
                logger.error(f"加载配置文件失败 {config_file}: {e}")
        
        logger.info(f"加载了 {len(self.factor_configs)} 个因子配置")
    
    def get_factor(self, name: str, params: Dict[str, Any] = None) -> Optional[BaseFactor]:
        """
        获取因子实例
        
        Args:
            name: 因子名称
            params: 自定义参数 (覆盖默认参数)
        
        Returns:
            因子实例
        """
        factor_class = FactorRegistry.get(name)
        
        if factor_class:
            config = self.factor_configs.get(name, {})
            merged_params = {**config.get("params", {}), **(params or {})}
            return factor_class(
                name=name,
                category=config.get("category", "unknown"),
                params=merged_params,
                description=config.get("description", "")
            )
        
        config = self.factor_configs.get(name)
        if config:
            category = config.get("category", "technical")
            try:
                module_path = f"factors.{category}.{name}"
                module = importlib.import_module(module_path)
                factor_class = getattr(module, f"{self._to_class_name(name)}Factor")
                
                merged_params = {**config.get("params", {}), **(params or {})}
                return factor_class(
                    name=name,
                    category=category,
                    params=merged_params,
                    description=config.get("description", "")
                )
            except (ImportError, AttributeError) as e:
                logger.warning(f"动态导入因子失败 {name}: {e}")
        
        logger.warning(f"因子 {name} 未找到")
        return None
    
    def _to_class_name(self, name: str) -> str:
        """将因子名转换为类名"""
        parts = name.split("_")
        return "".join(p.capitalize() for p in parts)
    
    def calculate_factor(
        self, 
        data: pl.DataFrame, 
        factor_name: str, 
        params: Dict[str, Any] = None
    ) -> pl.DataFrame:
        """
        计算单个因子
        
        Args:
            data: K线数据
            factor_name: 因子名称
            params: 自定义参数
        
        Returns:
            添加了因子列的 DataFrame
        """
        factor = self.get_factor(factor_name, params)
        
        if factor is None:
            logger.warning(f"因子 {factor_name} 不存在，返回默认值 50")
            return data.with_columns([
                pl.lit(50.0).alias(f"factor_{factor_name}")
            ])
        
        return factor.calculate(data)
    
    def calculate_all_factors(
        self, 
        data: pl.DataFrame,
        factor_names: List[str] = None,
        enabled_only: bool = True
    ) -> pl.DataFrame:
        """
        计算多个因子
        
        Args:
            data: K线数据
            factor_names: 指定因子列表 (None 则计算所有)
            enabled_only: 是否只计算启用的因子
        
        Returns:
            添加了所有因子列的 DataFrame
        """
        df = data.clone()
        
        if factor_names is None:
            factor_names = list(self.factor_configs.keys())
        
        for name in factor_names:
            config = self.factor_configs.get(name, {})
            
            if enabled_only and not config.get("enabled", True):
                continue
            
            df = self.calculate_factor(df, name)
        
        return df
    
    def list_factors(self, category: str = None, enabled_only: bool = False) -> List[dict]:
        """
        列出因子
        
        Args:
            category: 按类别筛选
            enabled_only: 只列出启用的因子
        
        Returns:
            因子配置列表
        """
        factors = []
        
        for name, config in self.factor_configs.items():
            if category and config.get("category") != category:
                continue
            if enabled_only and not config.get("enabled", True):
                continue
            
            factors.append({
                "name": name,
                "category": config.get("category"),
                "description": config.get("description"),
                "weight": config.get("weight", 0),
                "enabled": config.get("enabled", True)
            })
        
        return factors
    
    def get_factor_info(self, name: str) -> Optional[dict]:
        """获取因子详细信息"""
        return self.factor_configs.get(name)
