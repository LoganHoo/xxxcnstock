"""
因子配置加载器
支持独立配置文件加载和管理
"""
import yaml
from pathlib import Path
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class FactorConfigLoader:
    """因子配置加载器"""
    
    def __init__(self, config_dir: str = "config/factors"):
        self.config_dir = Path(config_dir)
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def load_factor_config(self, factor_name: str, category: str = None) -> Dict[str, Any]:
        """
        加载单个因子配置
        
        Args:
            factor_name: 因子名称
            category: 因子类别 (technical, volume_price等)
        
        Returns:
            因子配置字典
        """
        cache_key = f"{category}/{factor_name}" if category else factor_name
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        if category:
            config_path = self.config_dir / category / f"{factor_name}.yaml"
        else:
            config_path = self._find_factor_config(factor_name)
        
        if not config_path.exists():
            logger.warning(f"因子配置文件不存在: {config_path}")
            return {}
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        self._cache[cache_key] = config
        return config
    
    def _find_factor_config(self, factor_name: str) -> Path:
        """查找因子配置文件"""
        for category_dir in self.config_dir.iterdir():
            if category_dir.is_dir():
                config_path = category_dir / f"{factor_name}.yaml"
                if config_path.exists():
                    return config_path
        return self.config_dir / f"{factor_name}.yaml"
    
    def get_params(self, factor_name: str, preset: str = "default", 
                   category: str = None) -> Dict[str, Any]:
        """
        获取因子参数
        
        Args:
            factor_name: 因子名称
            preset: 参数预设 (default, conservative, aggressive, standard)
            category: 因子类别
        
        Returns:
            参数字典
        """
        config = self.load_factor_config(factor_name, category)
        
        if not config:
            return {}
        
        params_config = config.get("factor", {}).get("params", {})
        
        if preset == "default":
            return params_config.get("default", {})
        
        presets = params_config.get("presets", {})
        if preset in presets:
            preset_config = presets[preset]
            return {k: v for k, v in preset_config.items() if k != "description"}
        
        return params_config.get("default", {})
    
    def get_scoring(self, factor_name: str, category: str = None) -> Dict[str, Any]:
        """获取因子评分配置"""
        config = self.load_factor_config(factor_name, category)
        return config.get("factor", {}).get("scoring", {})
    
    def get_weight(self, factor_name: str, category: str = None) -> float:
        """获取因子权重"""
        scoring = self.get_scoring(factor_name, category)
        return scoring.get("weight", 0.05)
    
    def get_threshold(self, factor_name: str, category: str = None) -> float:
        """获取因子阈值"""
        scoring = self.get_scoring(factor_name, category)
        return scoring.get("threshold", 30)
    
    def get_optimization_params(self, factor_name: str, 
                                category: str = None) -> Dict[str, Any]:
        """获取优化参数范围"""
        config = self.load_factor_config(factor_name, category)
        return config.get("factor", {}).get("optimization", {})
    
    def list_available_presets(self, factor_name: str, 
                               category: str = None) -> List[str]:
        """列出可用的参数预设"""
        config = self.load_factor_config(factor_name, category)
        params_config = config.get("factor", {}).get("params", {})
        
        presets = ["default"]
        if "presets" in params_config:
            presets.extend(list(params_config["presets"].keys()))
        
        return presets
    
    def load_all_factors(self) -> Dict[str, Dict[str, Any]]:
        """加载所有因子配置"""
        all_configs = {}
        
        for category_dir in self.config_dir.iterdir():
            if category_dir.is_dir():
                category = category_dir.name
                for config_file in category_dir.glob("*.yaml"):
                    factor_name = config_file.stem
                    cache_key = f"{category}/{factor_name}"
                    all_configs[cache_key] = self.load_factor_config(factor_name, category)
        
        return all_configs
    
    def get_factor_info(self, factor_name: str, category: str = None) -> Dict[str, Any]:
        """获取因子完整信息"""
        config = self.load_factor_config(factor_name, category)
        factor_config = config.get("factor", {})
        
        return {
            "name": factor_config.get("name", factor_name),
            "category": factor_config.get("category", category or "unknown"),
            "description": factor_config.get("description", ""),
            "version": factor_config.get("version", "1.0"),
            "params": self.get_params(factor_name, "default", category),
            "presets": self.list_available_presets(factor_name, category),
            "weight": self.get_weight(factor_name, category),
            "threshold": self.get_threshold(factor_name, category),
            "optimization": self.get_optimization_params(factor_name, category)
        }


factor_config_loader = FactorConfigLoader()
