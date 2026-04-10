"""
过滤引擎
负责加载过滤器配置和应用过滤器
"""
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional

from filters.base_filter import BaseFilter, FilterRegistry
from core.filter_config_loader import FilterConfigLoader
from core.logger import get_logger

logger = get_logger(__name__)


class FilterEngine:
    """过滤引擎"""
    
    def __init__(self, config_dir: str = "config/filters", preset: str = "default"):
        self.config_dir = Path(config_dir)
        self.preset = preset
        self.config_loader = FilterConfigLoader(str(self.config_dir))
        self.filter_configs: Dict[str, dict] = {}
        self.filters: Dict[str, BaseFilter] = {}
        self._load_filter_configs()
        self._init_filters()
    
    def _load_filter_configs(self):
        """加载所有过滤器配置"""
        all_configs = self.config_loader.load_all_filters()
        
        for cache_key, config in all_configs.items():
            filter_config = config.get("filter", {})
            filter_name = filter_config.get("name", cache_key.split("/")[-1])
            self.filter_configs[filter_name] = config
        
        logger.info(f"加载了 {len(self.filter_configs)} 个过滤器配置")
    
    def _init_filters(self):
        """初始化过滤器实例"""
        for name, config in self.filter_configs.items():
            filter_class = FilterRegistry.get(name)
            
            if filter_class:
                try:
                    params = self.config_loader.get_params(name, self.preset)
                    enabled = self.config_loader.is_enabled(name)
                    
                    if not enabled:
                        params["enabled"] = False
                    
                    filter_instance = filter_class(params=params)
                    self.filters[name] = filter_instance
                    logger.debug(f"初始化过滤器: {name}")
                except Exception as e:
                    logger.error(f"初始化过滤器失败 {name}: {e}")
            else:
                logger.warning(f"未找到过滤器类: {name}")
        
        logger.info(f"初始化了 {len(self.filters)} 个过滤器")
    
    def apply_filters(self, stock_list: pl.DataFrame, filter_names: List[str] = None) -> pl.DataFrame:
        """
        应用过滤器
        
        Args:
            stock_list: 股票列表 DataFrame
            filter_names: 指定过滤器名称列表 (None 则应用所有启用的)
        
        Returns:
            过滤后的股票列表
        """
        if len(stock_list) == 0:
            return stock_list
        
        result = stock_list.clone()
        original_count = len(result)
        
        filters_to_apply = self._get_filters_to_apply(filter_names)
        
        for name, filter_instance in filters_to_apply.items():
            if filter_instance.is_enabled():
                try:
                    before_count = len(result)
                    result = filter_instance.filter(result)
                    after_count = len(result)
                    
                    if before_count != after_count:
                        logger.info(
                            f"过滤器 [{name}]: {before_count} -> {after_count} "
                            f"(移除 {before_count - after_count})"
                        )
                except Exception as e:
                    logger.error(f"应用过滤器失败 [{name}]: {e}")
        
        final_count = len(result)
        logger.info(
            f"过滤完成: {original_count} -> {final_count} "
            f"(共移除 {original_count - final_count} 只股票)"
        )
        
        return result
    
    def _get_filters_to_apply(self, filter_names: List[str] = None) -> Dict[str, BaseFilter]:
        """获取要应用的过滤器"""
        if filter_names:
            return {
                name: self.filters[name]
                for name in filter_names
                if name in self.filters
            }
        
        return self.filters
    
    def get_filter(self, name: str) -> Optional[BaseFilter]:
        """获取单个过滤器"""
        return self.filters.get(name)
    
    def list_filters(self, enabled_only: bool = False) -> List[dict]:
        """列出所有过滤器"""
        filters = []
        
        for name, filter_instance in self.filters.items():
            if enabled_only and not filter_instance.is_enabled():
                continue
            
            info = self.config_loader.get_filter_info(name)
            filters.append({
                "name": name,
                "enabled": filter_instance.is_enabled(),
                "description": filter_instance.description,
                "risk_level": info.get("risk_level", "medium"),
                "category": info.get("category", "unknown"),
                "presets": info.get("presets", [])
            })
        
        return filters
    
    def enable_filter(self, name: str):
        """启用过滤器"""
        if name in self.filters:
            self.filters[name].enabled = True
            logger.info(f"启用过滤器: {name}")
    
    def disable_filter(self, name: str):
        """禁用过滤器"""
        if name in self.filters:
            self.filters[name].enabled = False
            logger.info(f"禁用过滤器: {name}")
    
    def set_preset(self, preset: str):
        """
        设置预设并重新初始化过滤器
        
        Args:
            preset: 预设名称 (default, conservative, standard, aggressive)
        """
        self.preset = preset
        self.filters.clear()
        self._init_filters()
        logger.info(f"已应用预设: {preset}")
    
    def get_filter_stats(self, stock_list: pl.DataFrame) -> Dict[str, Any]:
        """获取过滤器统计信息"""
        stats = {
            "original_count": len(stock_list),
            "filters": []
        }
        
        result = stock_list.clone()
        
        for name, filter_instance in self.filters.items():
            if filter_instance.is_enabled():
                before_count = len(result)
                try:
                    filtered = filter_instance.filter(result)
                    after_count = len(filtered)
                    
                    stats["filters"].append({
                        "name": name,
                        "before": before_count,
                        "after": after_count,
                        "removed": before_count - after_count
                    })
                    
                    result = filtered
                except Exception as e:
                    stats["filters"].append({
                        "name": name,
                        "error": str(e)
                    })
        
        stats["final_count"] = len(result)
        stats["total_removed"] = stats["original_count"] - stats["final_count"]
        
        return stats
    
    def get_filters_by_category(self, category: str) -> List[str]:
        """获取指定类别的过滤器列表"""
        return [
            name for name, config in self.filter_configs.items()
            if config.get("filter", {}).get("category") == category
        ]
    
    def get_filters_by_risk_level(self, risk_level: str) -> List[str]:
        """获取指定风险等级的过滤器列表"""
        return [
            name for name, config in self.filter_configs.items()
            if config.get("filter", {}).get("risk_level") == risk_level
        ]
