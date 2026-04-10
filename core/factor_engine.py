"""
因子计算引擎
负责加载因子配置和计算因子值
"""
import yaml
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import importlib

from core.factor_library import BaseFactor, FactorRegistry
from core.logger import get_logger

logger = get_logger(__name__)


class FactorEngine:
    """因子计算引擎"""

    def __init__(self, config_dir: str = None):
        if config_dir is None:
            project_root = Path(__file__).parent.parent
            config_dir = project_root / "config" / "factors"
        self.config_dir = Path(config_dir)
        self.factor_configs: Dict[str, dict] = {}
        self._import_factor_modules()
        self._load_factor_configs()

    def _import_factor_modules(self):
        """导入所有因子模块以触发注册"""
        try:
            importlib.import_module("factors.market")
            importlib.import_module("factors.technical")
            importlib.import_module("factors.volume_price")
            logger.debug("因子模块已导入")
        except ImportError as e:
            logger.warning(f"导入因子模块失败: {e}")
    
    def _load_factor_configs(self):
        """加载所有因子配置"""
        if not self.config_dir.exists():
            logger.warning(f"因子配置目录不存在: {self.config_dir}")
            return

        config_files = sorted(
            self.config_dir.rglob("*.yaml"),
            key=lambda path: (len(path.relative_to(self.config_dir).parts), str(path)),
            reverse=True,
        )

        for config_file in config_files:
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    
                    if config is None:
                        continue

                    if "factor" in config and isinstance(config["factor"], dict):
                        normalized_config = self._normalize_single_factor_config(
                            config["factor"], config_file
                        )
                        if normalized_config:
                            name = normalized_config["name"]
                            self.factor_configs[name] = normalized_config
                            logger.debug(f"加载单因子配置: {name}")
                        continue

                    factors_data = config.get("factors", {})

                    if isinstance(factors_data, list):
                        for factor_config in factors_data:
                            normalized_config = self._normalize_multi_factor_config(
                                factor_config
                            )
                            name = normalized_config["name"]
                            self.factor_configs[name] = normalized_config
                            logger.debug(f"加载因子配置: {name}")
                    elif isinstance(factors_data, dict):
                        for category, factors in factors_data.items():
                            if isinstance(factors, list):
                                for factor_config in factors:
                                    normalized_config = self._normalize_multi_factor_config(
                                        factor_config, category
                                    )
                                    name = normalized_config["name"]
                                    self.factor_configs[name] = normalized_config
                                    logger.debug(f"加载因子配置: {name} ({category})")
            except Exception as e:
                logger.error(f"加载配置文件失败 {config_file}: {e}")
        
        logger.info(f"加载了 {len(self.factor_configs)} 个因子配置")

    def _normalize_multi_factor_config(
        self, factor_config: Dict[str, Any], category: str = None
    ) -> Dict[str, Any]:
        """标准化多因子配置结构"""
        normalized_config = dict(factor_config)
        if category and "category" not in normalized_config:
            normalized_config["category"] = category
        return normalized_config

    def _normalize_single_factor_config(
        self, factor_config: Dict[str, Any], config_file: Path
    ) -> Optional[Dict[str, Any]]:
        """标准化单因子配置结构"""
        name = factor_config.get("name")
        if not name:
            return None

        params = factor_config.get("params", {})
        if isinstance(params, dict) and "default" in params:
            params = params.get("default", {})

        scoring = factor_config.get("scoring", {})

        return {
            "name": name,
            "category": factor_config.get(
                "category",
                config_file.parent.name if config_file.parent != self.config_dir else "unknown",
            ),
            "description": factor_config.get("description", ""),
            "params": params if isinstance(params, dict) else {},
            "weight": scoring.get("weight", factor_config.get("weight", 0)),
            "enabled": factor_config.get("enabled", True),
        }
    
    def _find_factor_module(self, name: str, category: str) -> tuple:
        """查找因子所在的模块和类名

        Returns:
            (module_path, class_name) 或 (None, None)
        """
        from pathlib import Path

        module_dir = Path("factors") / category
        if not module_dir.exists():
            return None, None

        target_base = self._to_class_name(name)

        for py_file in module_dir.glob("*.py"):
            if py_file.name.startswith("_"):
                continue
            try:
                module_name = f"factors.{category}.{py_file.stem}"
                module = importlib.import_module(module_name)

                for attr_name in dir(module):
                    if attr_name.endswith("Factor"):
                        cls = getattr(module, attr_name)
                        if isinstance(cls, type) and issubclass(cls, BaseFactor):
                            base_name = attr_name[:-6]
                            if base_name.lower() == target_base.lower() or base_name == target_base:
                                return module_name, attr_name
            except ImportError:
                continue

        return None, None

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
                module_path, class_name = self._find_factor_module(name, category)
                if module_path and class_name:
                    module = importlib.import_module(module_path)
                    factor_class = getattr(module, class_name)

                    merged_params = {**config.get("params", {}), **(params or {})}
                    return factor_class(
                        name=name,
                        category=category,
                        params=merged_params,
                        description=config.get("description", "")
                    )
                else:
                    logger.warning(f"动态导入因子失败 {name}: 未找到模块")
            except (ImportError, AttributeError) as e:
                logger.warning(f"动态导入因子失败 {name}: {e}")

        logger.warning(f"因子 {name} 未找到")
        return None
    
    def _to_class_name(self, name: str) -> str:
        """将因子名转换为类名"""
        import re
        parts = name.split('_')
        result = []
        for p in parts:
            match = re.match(r'^([a-zA-Z]+)(\d*)$', p)
            if match:
                letters, digits = match.groups()
                result.append(letters.capitalize() + digits)
            else:
                result.append(p.capitalize())
        return ''.join(result)
    
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
