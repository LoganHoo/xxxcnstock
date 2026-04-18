#!/usr/bin/env python3
"""
因子和过滤器配置管理器

统一管理 factors_config.yaml 和 filters_config.yaml
提供统一的配置读取、修改、优化接口
"""
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class FactorConfig:
    """因子配置"""
    name: str
    category: str
    description: str
    enabled: bool
    params: Dict[str, Any] = field(default_factory=dict)
    optimization_range: Dict[str, List] = field(default_factory=dict)


@dataclass
class FilterConfig:
    """过滤器配置"""
    name: str
    category: str
    description: str
    enabled: bool
    params: Dict[str, Any] = field(default_factory=dict)
    optimization_range: Dict[str, List] = field(default_factory=dict)


@dataclass
class FactorCombination:
    """因子组合"""
    name: str
    description: str
    factors: List[Dict[str, Any]]


@dataclass
class FilterCombination:
    """过滤器组合"""
    name: str
    description: str
    filters: List[str]


class FactorFilterConfigManager:
    """因子和过滤器配置管理器"""
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_dir: str = "config"):
        if self._initialized:
            return
        
        self.config_dir = Path(config_dir)
        self.factors_config_path = self.config_dir / "factors_config.yaml"
        self.filters_config_path = self.config_dir / "filters_config.yaml"
        
        self._factors_config: Dict = {}
        self._filters_config: Dict = {}
        
        self._load_configs()
        self._initialized = True
    
    def _load_configs(self):
        """加载配置文件"""
        # 加载因子配置
        if self.factors_config_path.exists():
            with open(self.factors_config_path, 'r', encoding='utf-8') as f:
                self._factors_config = yaml.safe_load(f) or {}
            logger.info(f"已加载因子配置: {self.factors_config_path}")
        else:
            logger.warning(f"因子配置文件不存在: {self.factors_config_path}")
        
        # 加载过滤器配置
        if self.filters_config_path.exists():
            with open(self.filters_config_path, 'r', encoding='utf-8') as f:
                self._filters_config = yaml.safe_load(f) or {}
            logger.info(f"已加载过滤器配置: {self.filters_config_path}")
        else:
            logger.warning(f"过滤器配置文件不存在: {self.filters_config_path}")
    
    def reload(self):
        """重新加载配置"""
        self._load_configs()
    
    def save(self):
        """保存配置到文件"""
        with open(self.factors_config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._factors_config, f, allow_unicode=True, sort_keys=False)
        
        with open(self.filters_config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._filters_config, f, allow_unicode=True, sort_keys=False)
        
        logger.info("配置已保存")
    
    # ==================== 因子配置接口 ====================
    
    def get_all_factors(self) -> Dict[str, FactorConfig]:
        """获取所有因子配置"""
        factors = {}
        
        for category in ['technical_factors', 'volume_price_factors', 'market_factors', 'cost_factors']:
            if category in self._factors_config:
                for factor_id, config in self._factors_config[category].items():
                    factors[factor_id] = FactorConfig(
                        name=config.get('name', factor_id),
                        category=config.get('category', 'unknown'),
                        description=config.get('description', ''),
                        enabled=config.get('enabled', False),
                        params=config.get('params', {}),
                        optimization_range=config.get('optimization_range', {})
                    )
        
        return factors
    
    def get_factor(self, factor_id: str) -> Optional[FactorConfig]:
        """获取单个因子配置"""
        for category in ['technical_factors', 'volume_price_factors', 'market_factors', 'cost_factors']:
            if category in self._factors_config and factor_id in self._factors_config[category]:
                config = self._factors_config[category][factor_id]
                return FactorConfig(
                    name=config.get('name', factor_id),
                    category=config.get('category', 'unknown'),
                    description=config.get('description', ''),
                    enabled=config.get('enabled', False),
                    params=config.get('params', {}),
                    optimization_range=config.get('optimization_range', {})
                )
        return None
    
    def get_enabled_factors(self) -> Dict[str, FactorConfig]:
        """获取启用的因子"""
        all_factors = self.get_all_factors()
        return {k: v for k, v in all_factors.items() if v.enabled}
    
    def get_factor_params(self, factor_id: str) -> Dict[str, Any]:
        """获取因子参数"""
        factor = self.get_factor(factor_id)
        return factor.params if factor else {}
    
    def set_factor_param(self, factor_id: str, param_name: str, value: Any):
        """设置因子参数"""
        for category in ['technical_factors', 'volume_price_factors', 'market_factors', 'cost_factors']:
            if category in self._factors_config and factor_id in self._factors_config[category]:
                if 'params' not in self._factors_config[category][factor_id]:
                    self._factors_config[category][factor_id]['params'] = {}
                self._factors_config[category][factor_id]['params'][param_name] = value
                logger.info(f"已设置因子 {factor_id} 的参数 {param_name} = {value}")
                return True
        return False
    
    def get_factor_combinations(self) -> Dict[str, FactorCombination]:
        """获取所有因子组合"""
        combinations = {}
        
        if 'factor_combinations' in self._factors_config:
            for combo_id, config in self._factors_config['factor_combinations'].items():
                combinations[combo_id] = FactorCombination(
                    name=config.get('name', combo_id),
                    description=config.get('description', ''),
                    factors=config.get('factors', [])
                )
        
        return combinations
    
    def get_factor_combination(self, combo_id: str) -> Optional[FactorCombination]:
        """获取单个因子组合"""
        combos = self.get_factor_combinations()
        return combos.get(combo_id)
    
    # ==================== 过滤器配置接口 ====================
    
    def get_all_filters(self) -> Dict[str, FilterConfig]:
        """获取所有过滤器配置"""
        filters = {}
        
        for category in ['base_filters', 'liquidity_filters', 'price_filters', 
                        'sector_filters', 'market_cap_filters', 'fundamental_filters', 'pattern_filters']:
            if category in self._filters_config:
                for filter_id, config in self._filters_config[category].items():
                    filters[filter_id] = FilterConfig(
                        name=config.get('name', filter_id),
                        category=config.get('category', 'unknown'),
                        description=config.get('description', ''),
                        enabled=config.get('enabled', False),
                        params=config.get('params', {}),
                        optimization_range=config.get('optimization_range', {})
                    )
        
        return filters
    
    def get_filter(self, filter_id: str) -> Optional[FilterConfig]:
        """获取单个过滤器配置"""
        for category in ['base_filters', 'liquidity_filters', 'price_filters',
                        'sector_filters', 'market_cap_filters', 'fundamental_filters', 'pattern_filters']:
            if category in self._filters_config and filter_id in self._filters_config[category]:
                config = self._filters_config[category][filter_id]
                return FilterConfig(
                    name=config.get('name', filter_id),
                    category=config.get('category', 'unknown'),
                    description=config.get('description', ''),
                    enabled=config.get('enabled', False),
                    params=config.get('params', {}),
                    optimization_range=config.get('optimization_range', {})
                )
        return None
    
    def get_enabled_filters(self) -> Dict[str, FilterConfig]:
        """获取启用的过滤器"""
        all_filters = self.get_all_filters()
        return {k: v for k, v in all_filters.items() if v.enabled}
    
    def get_filter_params(self, filter_id: str) -> Dict[str, Any]:
        """获取过滤器参数"""
        filter_config = self.get_filter(filter_id)
        return filter_config.params if filter_config else {}
    
    def get_filter_combinations(self) -> Dict[str, FilterCombination]:
        """获取所有过滤器组合"""
        combinations = {}
        
        if 'filter_combinations' in self._filters_config:
            for combo_id, config in self._filters_config['filter_combinations'].items():
                combinations[combo_id] = FilterCombination(
                    name=config.get('name', combo_id),
                    description=config.get('description', ''),
                    filters=config.get('filters', [])
                )
        
        return combinations
    
    def get_filter_combination(self, combo_id: str) -> Optional[FilterCombination]:
        """获取单个过滤器组合"""
        combos = self.get_filter_combinations()
        return combos.get(combo_id)
    
    # ==================== 优化配置接口 ====================
    
    def get_factor_validation_thresholds(self) -> Dict[str, Any]:
        """获取因子有效性阈值"""
        return self._factors_config.get('factor_validation', {})
    
    def get_factor_optimization_config(self) -> Dict[str, Any]:
        """获取因子优化配置"""
        return self._factors_config.get('factor_optimization', {})
    
    def get_filter_optimization_config(self) -> Dict[str, Any]:
        """获取过滤器优化配置"""
        return self._filters_config.get('filter_optimization', {})
    
    # ==================== 实用方法 ====================
    
    def get_factor_optimization_params(self, factor_id: str) -> Dict[str, List]:
        """获取因子的优化参数范围"""
        factor = self.get_factor(factor_id)
        return factor.optimization_range if factor else {}
    
    def generate_factor_param_combinations(self, factor_id: str) -> List[Dict[str, Any]]:
        """生成因子的所有参数组合（用于网格搜索）"""
        from itertools import product
        
        opt_range = self.get_factor_optimization_params(factor_id)
        if not opt_range:
            return [{}]
        
        param_names = list(opt_range.keys())
        param_values = [opt_range[name] for name in param_names]
        
        combinations = []
        for values in product(*param_values):
            combinations.append(dict(zip(param_names, values)))
        
        return combinations
    
    def print_factor_summary(self):
        """打印因子配置摘要"""
        print("\n" + "=" * 80)
        print("因子配置摘要")
        print("=" * 80)
        
        factors = self.get_all_factors()
        
        categories = {}
        for factor_id, factor in factors.items():
            cat = factor.category
            if cat not in categories:
                categories[cat] = []
            categories[cat].append((factor_id, factor))
        
        for cat, items in categories.items():
            print(f"\n【{cat}】")
            for factor_id, factor in items:
                status = "✓" if factor.enabled else "✗"
                print(f"  {status} {factor_id}: {factor.name}")
                if factor.params:
                    print(f"      参数: {factor.params}")
        
        print("\n" + "=" * 80)
        print(f"总计: {len(factors)} 个因子，其中 {len(self.get_enabled_factors())} 个已启用")
        print("=" * 80)
    
    def print_filter_summary(self):
        """打印过滤器配置摘要"""
        print("\n" + "=" * 80)
        print("过滤器配置摘要")
        print("=" * 80)
        
        filters = self.get_all_filters()
        
        categories = {}
        for filter_id, filter_config in filters.items():
            cat = filter_config.category
            if cat not in categories:
                categories[cat] = []
            categories[cat].append((filter_id, filter_config))
        
        for cat, items in categories.items():
            print(f"\n【{cat}】")
            for filter_id, filter_config in items:
                status = "✓" if filter_config.enabled else "✗"
                print(f"  {status} {filter_id}: {filter_config.name}")
                if filter_config.params:
                    print(f"      参数: {filter_config.params}")
        
        print("\n" + "=" * 80)
        print(f"总计: {len(filters)} 个过滤器，其中 {len(self.get_enabled_filters())} 个已启用")
        print("=" * 80)


# 全局配置管理器实例
_config_manager = None


def get_factor_filter_config() -> FactorFilterConfigManager:
    """获取全局配置管理器实例"""
    global _config_manager
    if _config_manager is None:
        _config_manager = FactorFilterConfigManager()
    return _config_manager


if __name__ == "__main__":
    # 测试配置管理器
    config = get_factor_filter_config()
    config.print_factor_summary()
    config.print_filter_summary()
