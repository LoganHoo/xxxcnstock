"""
过滤器配置加载器
支持独立配置文件加载和管理
"""
import yaml
from pathlib import Path
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class FilterConfigLoader:
    """过滤器配置加载器"""
    
    def __init__(self, config_dir: str = "config/filters"):
        self.config_dir = Path(config_dir)
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def load_filter_config(self, filter_name: str, category: str = None) -> Dict[str, Any]:
        """
        加载单个过滤器配置
        
        Args:
            filter_name: 过滤器名称
            category: 过滤器类别 (stock, market, fundamental, technical, liquidity, valuation, pattern)
        
        Returns:
            过滤器配置字典
        """
        cache_key = f"{category}/{filter_name}" if category else filter_name
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        if category:
            config_path = self.config_dir / category / f"{filter_name}.yaml"
        else:
            config_path = self._find_filter_config(filter_name)
        
        if not config_path.exists():
            logger.warning(f"过滤器配置文件不存在: {config_path}")
            return {}
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        self._cache[cache_key] = config
        return config
    
    def _find_filter_config(self, filter_name: str) -> Path:
        """查找过滤器配置文件"""
        categories = ["stock", "market", "fundamental", "technical", "liquidity", "valuation", "pattern"]
        
        for category in categories:
            config_path = self.config_dir / category / f"{filter_name}.yaml"
            if config_path.exists():
                return config_path
        
        return self.config_dir / f"{filter_name}.yaml"
    
    def get_params(self, filter_name: str, preset: str = "default", 
                   category: str = None) -> Dict[str, Any]:
        """
        获取过滤器参数
        
        Args:
            filter_name: 过滤器名称
            preset: 参数预设 (default, conservative, standard, aggressive)
            category: 过滤器类别
        
        Returns:
            参数字典
        """
        config = self.load_filter_config(filter_name, category)
        
        if not config:
            return {}
        
        filter_config = config.get("filter", {})
        
        if preset == "default" or preset is None:
            return filter_config.get("params", {})
        
        presets = filter_config.get("presets", {})
        if preset in presets:
            preset_config = presets[preset]
            base_params = filter_config.get("params", {}).copy()
            preset_params = {k: v for k, v in preset_config.items() if k != "description"}
            base_params.update(preset_params)
            return base_params
        
        return filter_config.get("params", {})
    
    def is_enabled(self, filter_name: str, category: str = None) -> bool:
        """检查过滤器是否启用"""
        config = self.load_filter_config(filter_name, category)
        return config.get("filter", {}).get("enabled", True)
    
    def get_description(self, filter_name: str, category: str = None) -> str:
        """获取过滤器描述"""
        config = self.load_filter_config(filter_name, category)
        return config.get("filter", {}).get("description", "")
    
    def get_risk_level(self, filter_name: str, category: str = None) -> str:
        """获取风险等级"""
        config = self.load_filter_config(filter_name, category)
        return config.get("filter", {}).get("risk_level", "medium")
    
    def get_category(self, filter_name: str, category: str = None) -> str:
        """获取过滤器类别"""
        config = self.load_filter_config(filter_name, category)
        return config.get("filter", {}).get("category", category or "unknown")
    
    def list_available_presets(self, filter_name: str, 
                               category: str = None) -> List[str]:
        """列出可用的参数预设"""
        config = self.load_filter_config(filter_name, category)
        filter_config = config.get("filter", {})
        
        presets = ["default"]
        if "presets" in filter_config:
            presets.extend(list(filter_config["presets"].keys()))
        
        return presets
    
    def load_all_filters(self) -> Dict[str, Dict[str, Any]]:
        """加载所有过滤器配置"""
        all_configs = {}
        
        categories = ["stock", "market", "fundamental", "technical", "liquidity", "valuation", "pattern"]
        
        for category in categories:
            category_dir = self.config_dir / category
            if category_dir.exists():
                for config_file in category_dir.glob("*.yaml"):
                    filter_name = config_file.stem
                    cache_key = f"{category}/{filter_name}"
                    all_configs[cache_key] = self.load_filter_config(filter_name, category)
        
        return all_configs
    
    def load_category_filters(self, category: str) -> Dict[str, Dict[str, Any]]:
        """加载指定类别的所有过滤器配置"""
        category_configs = {}
        category_dir = self.config_dir / category
        
        if not category_dir.exists():
            return category_configs
        
        for config_file in category_dir.glob("*.yaml"):
            filter_name = config_file.stem
            cache_key = f"{category}/{filter_name}"
            category_configs[filter_name] = self.load_filter_config(filter_name, category)
        
        return category_configs
    
    def get_filter_info(self, filter_name: str, category: str = None) -> Dict[str, Any]:
        """获取过滤器完整信息"""
        config = self.load_filter_config(filter_name, category)
        filter_config = config.get("filter", {})
        
        return {
            "name": filter_config.get("name", filter_name),
            "category": filter_config.get("category", category or "unknown"),
            "description": filter_config.get("description", ""),
            "enabled": filter_config.get("enabled", True),
            "risk_level": filter_config.get("risk_level", "medium"),
            "params": self.get_params(filter_name, "default", category),
            "presets": self.list_available_presets(filter_name, category)
        }
    
    def apply_preset(self, filter_name: str, preset: str, 
                     category: str = None) -> Dict[str, Any]:
        """
        应用预设配置
        
        Args:
            filter_name: 过滤器名称
            preset: 预设名称
            category: 过滤器类别
        
        Returns:
            应用预设后的完整配置
        """
        config = self.load_filter_config(filter_name, category)
        filter_config = config.get("filter", {}).copy()
        
        if preset != "default":
            presets = filter_config.get("presets", {})
            if preset in presets:
                preset_config = presets[preset]
                filter_config["params"] = self.get_params(filter_name, preset, category)
                if "enabled" in preset_config:
                    filter_config["enabled"] = preset_config["enabled"]
        
        return {"filter": filter_config}


filter_config_loader = FilterConfigLoader()
