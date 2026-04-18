#!/usr/bin/env python3
"""
统一配置管理系统
整合所有配置来源：YAML文件、环境变量、代码默认值

使用方式:
    from core.unified_config import config, StrategyConfig
    
    # 获取当前策略配置
    threshold = config.get('strategy.stock_selection.trend_stocks_limit', 50)
    
    # 获取指定策略配置（用于回测优化）
    strategy_cfg = StrategyConfig.load_strategy_config('fund_behavior')
    
    # 获取带类型的配置
    port = config.get_int('database.port', 3306)
    debug = config.get_bool('app.debug', False)
"""
import os
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


class UnifiedConfig:
    """统一配置管理器"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_dir: Optional[str] = None):
        if self._initialized:
            return
            
        self._initialized = True
        self._config_dir = Path(config_dir) if config_dir else Path(__file__).parent.parent / "config"
        self._configs: Dict[str, Any] = {}
        self._strategy_configs: Dict[str, Dict[str, Any]] = {}  # 策略独立配置缓存
        self._env_prefix = "XCN_"
        
        # 加载所有配置
        self._load_all_configs()
    
    def _load_all_configs(self):
        """加载所有配置文件"""
        if not self._config_dir.exists():
            logger.warning(f"配置目录不存在: {self._config_dir}")
            return
        
        # 加载全局配置（数据库、日志等）
        self._load_global_configs()
        
        # 加载策略独立配置
        self._load_strategy_configs()
        
        # 加载策略因子配置
        self._load_strategy_factors_config()
        
        logger.info(f"配置加载完成，共 {len(self._configs)} 个全局配置项，{len(self._strategy_configs)} 个策略配置")
    
    def _load_global_configs(self):
        """加载全局配置"""
        global_configs = [
            "xcn_comm.yaml",
        ]
        
        for config_file in global_configs:
            path = self._config_dir / config_file
            if path.exists():
                self._load_yaml(path)
                logger.info(f"加载全局配置: {config_file}")
    
    def _load_strategy_configs(self):
        """加载策略独立配置"""
        # 策略配置文件映射
        strategy_files = {
            'fund_behavior': 'fund_behavior_config.yaml',
            'multi_factor': 'strategies/multi_factor.yaml',
            'trend_following': 'strategies/trend_following.yaml',
            'champion': 'strategies/champion.yaml',
        }
        
        for strategy_name, filename in strategy_files.items():
            path = self._config_dir / filename
            if path.exists():
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        self._strategy_configs[strategy_name] = yaml.safe_load(f)
                    logger.info(f"加载策略配置: {strategy_name} -> {filename}")
                except Exception as e:
                    logger.error(f"加载策略配置失败 {filename}: {e}")
    
    def _load_strategy_factors_config(self):
        """加载策略因子配置"""
        factors_config_path = self._config_dir / "strategy_factors.yaml"
        if factors_config_path.exists():
            try:
                with open(factors_config_path, 'r', encoding='utf-8') as f:
                    self._strategy_factors = yaml.safe_load(f)
                logger.info(f"加载策略因子配置: strategy_factors.yaml")
            except Exception as e:
                logger.error(f"加载策略因子配置失败: {e}")
                self._strategy_factors = {}
        else:
            self._strategy_factors = {}
    
    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """
        获取指定策略的独立配置
        
        Args:
            strategy_name: 策略名称，如 'fund_behavior'
        
        Returns:
            策略配置字典
        """
        if strategy_name not in self._strategy_configs:
            logger.warning(f"策略配置不存在: {strategy_name}，返回空配置")
            return {}
        return self._strategy_configs[strategy_name].copy()
    
    def get_strategy_factors(self, strategy_name: str) -> Dict[str, Any]:
        """
        获取指定策略的因子配置
        
        Args:
            strategy_name: 策略名称
        
        Returns:
            因子配置字典
        """
        if hasattr(self, '_strategy_factors') and strategy_name in self._strategy_factors:
            return self._strategy_factors[strategy_name].copy()
        return {}
    
    def get_factor_validation_thresholds(self) -> Dict[str, Any]:
        """获取因子有效性验证阈值"""
        if hasattr(self, '_strategy_factors'):
            return self._strategy_factors.get('factor_validation', {})
        return {}
    
    def get_factor_optimization_config(self) -> Dict[str, Any]:
        """获取因子优化配置"""
        if hasattr(self, '_strategy_factors'):
            return self._strategy_factors.get('factor_optimization', {})
        return {}
    
    def list_strategies(self) -> List[str]:
        """列出所有可用的策略名称"""
        return list(self._strategy_configs.keys())
    
    def _load_legacy_configs(self):
        """加载旧版配置文件（兼容模式）"""
        # 加载主配置文件
        main_configs = [
            "fund_behavior_config.yaml",
            "xcn_comm.yaml",
        ]
        
        for config_file in main_configs:
            path = self._config_dir / config_file
            if path.exists():
                self._load_yaml(path)
        
        # 加载策略配置
        strategies_dir = self._config_dir / "strategies"
        if strategies_dir.exists():
            for yaml_file in strategies_dir.glob("*.yaml"):
                self._load_yaml(yaml_file, namespace="strategies")
        
        # 加载因子配置
        factors_dir = self._config_dir / "factors"
        if factors_dir.exists():
            for yaml_file in factors_dir.glob("*.yaml"):
                self._load_yaml(yaml_file, namespace="factors")
            # 递归加载子目录
            for subdir in factors_dir.iterdir():
                if subdir.is_dir():
                    for yaml_file in subdir.glob("*.yaml"):
                        self._load_yaml(yaml_file, namespace=f"factors.{subdir.name}")
        
        # 加载过滤器配置
        filters_dir = self._config_dir / "filters"
        if filters_dir.exists():
            for yaml_file in filters_dir.glob("*.yaml"):
                self._load_yaml(yaml_file, namespace="filters")
            # 递归加载子目录
            for subdir in filters_dir.iterdir():
                if subdir.is_dir():
                    for yaml_file in subdir.glob("*.yaml"):
                        self._load_yaml(yaml_file, namespace=f"filters.{subdir.name}")
    
    def _load_yaml(self, path: Path, namespace: Optional[str] = None):
        """加载单个YAML文件"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                
            if data is None:
                return
            
            # 使用文件名作为命名空间
            if namespace is None:
                namespace = path.stem
            
            # 递归合并配置
            self._merge_config(namespace, data)
            logger.debug(f"加载配置: {path} -> {namespace}")
            
        except Exception as e:
            logger.error(f"加载配置失败 {path}: {e}")
    
    def _merge_config(self, namespace: str, data: Dict):
        """递归合并配置"""
        for key, value in data.items():
            full_key = f"{namespace}.{key}" if namespace else key
            
            if isinstance(value, dict):
                self._merge_config(full_key, value)
            else:
                self._configs[full_key] = value
    
    def _get_env_key(self, key: str) -> str:
        """将配置键转换为环境变量名"""
        # 将点号替换为下划线，并添加前缀
        env_key = key.replace('.', '_').replace('-', '_').upper()
        return f"{self._env_prefix}{env_key}"
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        优先级: 环境变量 > 配置文件 > 默认值
        
        Args:
            key: 配置键，支持点号分隔（如 'strategy.position.trend'）
            default: 默认值
        
        Returns:
            配置值
        """
        # 1. 检查环境变量
        env_key = self._get_env_key(key)
        env_value = os.getenv(env_key)
        if env_value is not None:
            return self._convert_type(env_value, default)
        
        # 2. 检查配置文件
        if key in self._configs:
            return self._configs[key]
        
        # 3. 返回默认值
        return default
    
    def _convert_type(self, value: str, default: Any) -> Any:
        """根据默认值类型转换环境变量值"""
        if default is None:
            return value
        
        if isinstance(default, bool):
            return value.lower() in ('true', '1', 'yes', 'on')
        elif isinstance(default, int):
            return int(value)
        elif isinstance(default, float):
            return float(value)
        elif isinstance(default, list):
            # 尝试解析JSON列表
            try:
                import json
                return json.loads(value)
            except:
                return value.split(',')
        
        return value
    
    def get_int(self, key: str, default: int = 0) -> int:
        """获取整数配置"""
        value = self.get(key, default)
        if isinstance(value, str):
            return int(value)
        return int(value) if value is not None else default
    
    def get_float(self, key: str, default: float = 0.0) -> float:
        """获取浮点数配置"""
        value = self.get(key, default)
        if isinstance(value, str):
            return float(value)
        return float(value) if value is not None else default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """获取布尔配置"""
        value = self.get(key, default)
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value) if value is not None else default
    
    def get_list(self, key: str, default: Optional[list] = None) -> list:
        """获取列表配置"""
        value = self.get(key, default or [])
        if isinstance(value, str):
            return [v.strip() for v in value.split(',')]
        return list(value) if value is not None else (default or [])
    
    def get_dict(self, key: str, default: Optional[dict] = None) -> dict:
        """获取字典配置"""
        value = self.get(key, default or {})
        return dict(value) if value is not None else (default or {})
    
    def set(self, key: str, value: Any):
        """设置配置值（运行时）"""
        self._configs[key] = value
        logger.debug(f"设置配置: {key} = {value}")
    
    def has(self, key: str) -> bool:
        """检查配置是否存在"""
        env_key = self._get_env_key(key)
        return os.getenv(env_key) is not None or key in self._configs
    
    def get_all(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        获取所有配置
        
        Args:
            prefix: 可选，只返回指定前缀的配置
        """
        result = {}
        
        # 添加配置文件中的配置
        for key, value in self._configs.items():
            if prefix is None or key.startswith(prefix):
                result[key] = value
        
        # 添加环境变量中的配置
        for env_key, env_value in os.environ.items():
            if env_key.startswith(self._env_prefix):
                key = env_key[len(self._env_prefix):].lower().replace('_', '.')
                if prefix is None or key.startswith(prefix):
                    result[key] = env_value
        
        return result
    
    def reload(self):
        """重新加载所有配置"""
        self._configs.clear()
        self._load_all_configs()
        logger.info("配置已重新加载")
    
    def dump(self) -> str:
        """导出所有配置为YAML格式"""
        return yaml.dump(self._configs, allow_unicode=True, sort_keys=True)


# 全局配置实例
config = UnifiedConfig()


# 便捷函数
def get(key: str, default: Any = None) -> Any:
    """获取配置值"""
    return config.get(key, default)


def get_int(key: str, default: int = 0) -> int:
    """获取整数配置"""
    return config.get_int(key, default)


def get_float(key: str, default: float = 0.0) -> float:
    """获取浮点数配置"""
    return config.get_float(key, default)


def get_bool(key: str, default: bool = False) -> bool:
    """获取布尔配置"""
    return config.get_bool(key, default)


def get_list(key: str, default: Optional[list] = None) -> list:
    """获取列表配置"""
    return config.get_list(key, default)


def get_dict(key: str, default: Optional[dict] = None) -> dict:
    """获取字典配置"""
    return config.get_dict(key, default)


def set(key: str, value: Any):
    """设置配置值"""
    config.set(key, value)


def has(key: str) -> bool:
    """检查配置是否存在"""
    return config.has(key)


# 策略配置便捷访问
class StrategyConfig:
    """策略配置访问类 - 支持多策略独立配置"""
    
    _current_strategy: str = 'fund_behavior'  # 默认策略
    
    @classmethod
    def set_strategy(cls, strategy_name: str):
        """设置当前使用的策略"""
        if strategy_name not in config.list_strategies():
            logger.warning(f"策略 {strategy_name} 不存在，可用策略: {config.list_strategies()}")
        cls._current_strategy = strategy_name
        logger.info(f"当前策略设置为: {strategy_name}")
    
    @classmethod
    def get_strategy_config(cls, strategy_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取指定策略的完整配置
        
        Args:
            strategy_name: 策略名称，默认使用当前策略
        
        Returns:
            策略配置字典
        """
        name = strategy_name or cls._current_strategy
        return config.get_strategy_config(name)
    
    @classmethod
    def _get_from_strategy(cls, key: str, default: Any = None, strategy_name: Optional[str] = None) -> Any:
        """从策略配置中获取值"""
        cfg = cls.get_strategy_config(strategy_name)
        keys = key.split('.')
        value = cfg
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
    
    @classmethod
    def get_position_weights(cls, strategy_name: Optional[str] = None) -> Dict[str, float]:
        """获取仓位权重"""
        return {
            'trend': cls._get_from_strategy('strategy.position.trend', 0.5, strategy_name),
            'short_term': cls._get_from_strategy('strategy.position.short_term', 0.4, strategy_name),
            'cash': cls._get_from_strategy('strategy.position.cash', 0.1, strategy_name),
        }
    
    @classmethod
    def get_stock_limits(cls, strategy_name: Optional[str] = None) -> Dict[str, int]:
        """获取选股数量限制"""
        return {
            'trend': cls._get_from_strategy('strategy.stock_selection.trend_stocks_limit', 50, strategy_name),
            'short_term': cls._get_from_strategy('strategy.stock_selection.short_term_stocks_limit', 20, strategy_name),
        }
    
    @classmethod
    def get_defense_params(cls, strategy_name: Optional[str] = None) -> Dict[str, Any]:
        """获取防守信号参数"""
        return {
            'open_auction_min': cls._get_from_strategy('indicators.defense.params.open_auction_min', 500, strategy_name),
            'support_break_threshold': cls._get_from_strategy('indicators.defense.params.support_break_threshold', -0.02, strategy_name),
            'volume_shrink_threshold': cls._get_from_strategy('indicators.defense.params.volume_shrink_threshold', 0.6, strategy_name),
        }
    
    @classmethod
    def get_hedge_params(cls, strategy_name: Optional[str] = None) -> Dict[str, Any]:
        """获取对冲参数"""
        return {
            'support_level': cls._get_from_strategy('indicators.hedge.support_level', 4067, strategy_name),
            'resistance_levels': cls._get_from_strategy('indicators.hedge.resistance_levels', [4117, 4140], strategy_name),
            'v_total_threshold': cls._get_from_strategy('indicators.hedge.v_total_threshold', 1800, strategy_name),
        }
    
    @classmethod
    def get_sentiment_thresholds(cls, strategy_name: Optional[str] = None) -> Dict[str, float]:
        """获取情绪阈值"""
        return {
            'strong': cls._get_from_strategy('indicators.market_sentiment.thresholds.sentiment_temperature_strong', 50, strategy_name),
            'overheat': cls._get_from_strategy('indicators.market_sentiment.thresholds.sentiment_temperature_overheat', 80, strategy_name),
            'v_total_strong': cls._get_from_strategy('indicators.market_sentiment.thresholds.strong_v_total', 1800, strategy_name),
        }
    
    @classmethod
    def get_factors(cls, strategy_name: Optional[str] = None) -> Dict[str, Any]:
        """获取策略因子配置"""
        return cls._get_from_strategy('factors', {}, strategy_name)
    
    @classmethod
    def get_backtest_config(cls, strategy_name: Optional[str] = None) -> Dict[str, Any]:
        """获取回测配置"""
        return cls._get_from_strategy('backtest', {}, strategy_name)
    
    @classmethod
    def get_factor_weights(cls, strategy_name: Optional[str] = None, factor_type: str = 'trend') -> Dict[str, float]:
        """
        获取策略因子权重
        
        Args:
            strategy_name: 策略名称
            factor_type: 因子类型 (trend/short_term/market)
        
        Returns:
            因子权重字典 {factor_name: weight}
        """
        name = strategy_name or cls._current_strategy
        factor_config = config.get_strategy_factors(name)
        
        factors = factor_config.get('factors', {}).get(factor_type, [])
        weights = {}
        
        for factor in factors:
            if factor.get('enabled', True):
                weights[factor['name']] = factor.get('weight', 0)
        
        return weights
    
    @classmethod
    def get_factor_params(cls, strategy_name: Optional[str] = None, factor_name: str = '') -> Dict[str, Any]:
        """
        获取因子参数
        
        Args:
            strategy_name: 策略名称
            factor_name: 因子名称
        
        Returns:
            因子参数字典
        """
        name = strategy_name or cls._current_strategy
        factor_config = config.get_strategy_factors(name)
        
        # 遍历所有因子类型查找
        for factor_type in ['trend', 'short_term', 'market', 'risk', 'value', 'quality', 'technical', 'momentum']:
            factors = factor_config.get('factors', {}).get(factor_type, [])
            for factor in factors:
                if factor['name'] == factor_name:
                    return factor.get('params', {})
        
        return {}


# 因子配置便捷访问
class FactorConfig:
    """因子配置访问类"""
    
    @staticmethod
    def is_enabled(factor_name: str) -> bool:
        """检查因子是否启用"""
        return config.get_bool(f'factors.{factor_name}.enabled', True)
    
    @staticmethod
    def get_params(factor_name: str) -> Dict[str, Any]:
        """获取因子参数"""
        return config.get_dict(f'factors.{factor_name}.params', {})


# 过滤器配置便捷访问
class FilterConfig:
    """过滤器配置访问类"""
    
    @staticmethod
    def is_enabled(filter_name: str) -> bool:
        """检查过滤器是否启用"""
        return config.get_bool(f'filters.{filter_name}.enabled', True)
    
    @staticmethod
    def get_params(filter_name: str) -> Dict[str, Any]:
        """获取过滤器参数"""
        return config.get_dict(f'filters.{filter_name}.params', {})
    
    @staticmethod
    def get_exclude_prefixes() -> List[str]:
        """获取排除的代码前缀"""
        return config.get_list('filters.code_filter.params.exclude_prefixes', ['688', '8', '4'])


# 选股评分配置
class ScoringConfig:
    """选股评分配置访问类"""
    
    @staticmethod
    def get_trend_weights() -> Dict[str, float]:
        """获取波段选股评分权重"""
        return {
            'factor_ma5_bias': config.get_float('scoring.trend.factor_ma5_bias', 0.3),
            'ma5_slope': config.get_float('scoring.trend.ma5_slope', 0.2),
            'factor_v_ratio10': config.get_float('scoring.trend.factor_v_ratio10', 0.2),
            'factor_limit_up_score': config.get_float('scoring.trend.factor_limit_up_score', 0.15),
            'factor_cost_peak': config.get_float('scoring.trend.factor_cost_peak', 0.15),
        }
    
    @staticmethod
    def get_short_term_weights() -> Dict[str, float]:
        """获取短线选股评分权重"""
        return {
            'is_limit_up': config.get_float('scoring.short_term.is_limit_up', 100),
            'factor_limit_up_score': config.get_float('scoring.short_term.factor_limit_up_score', 10),
            'factor_ma5_bias': config.get_float('scoring.short_term.factor_ma5_bias', 10),
        }


if __name__ == "__main__":
    # 测试配置系统
    print("=" * 80)
    print("统一配置管理系统测试")
    print("=" * 80)
    
    # 测试基本配置获取
    print("\n【基本配置】")
    print(f"波段仓位权重: {StrategyConfig.get_position_weights()['trend']}")
    print(f"趋势选股数量: {StrategyConfig.get_stock_limits()['trend']}")
    print(f"防守开盘金额阈值: {StrategyConfig.get_defense_params()['open_auction_min']}")
    
    # 测试因子配置
    print("\n【因子配置】")
    print(f"ma5_bias 启用: {FactorConfig.is_enabled('ma5_bias')}")
    print(f"limit_up_score 启用: {FactorConfig.is_enabled('limit_up_score')}")
    
    # 测试环境变量覆盖
    print("\n【环境变量测试】")
    os.environ['XCN_STRATEGY_POSITION_TREND'] = '0.6'
    print(f"环境变量覆盖后: {config.get('strategy.position.trend')}")
    del os.environ['XCN_STRATEGY_POSITION_TREND']
    
    # 导出所有配置
    print("\n【所有配置】")
    all_configs = config.get_all('strategy')
    for key, value in sorted(all_configs.items())[:10]:
        print(f"  {key}: {value}")
    print(f"  ... 共 {len(all_configs)} 项")
    
    print("\n" + "=" * 80)
