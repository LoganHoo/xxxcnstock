#!/usr/bin/env python3
"""
双源采集配置管理器 (v3.0)

职责：
1. 集中管理所有配置参数
2. 提供配置验证和边界检查
3. 支持依赖注入（非全局单例）
4. 类型安全的配置访问

设计原则：
- 单一职责：只负责配置管理
- 不可变性：配置一旦创建不应修改
- 验证前置：构造时即验证所有参数
"""

from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DualSourceConfig:
    """
    双源采集配置（不可变）
    
    使用方式：
        config = DualSourceConfig.from_yaml(yaml_data)
        # 或
        config = DualSourceConfig(
            split_mode="position",
            baostock_concurrency=4,
            ...
        )
    """
    
    # ==================== 核心分割配置 ====================
    enabled: bool = True
    split_mode: str = "position"  # "position" | "date"
    split_ratio: float = 0.5
    
    # ==================== 并发控制 ====================
    baostock_concurrency: int = 4
    tencent_concurrency: int = 32
    
    # ==================== 数据验证 ====================
    cross_validate: bool = True
    validation_overlap_days: int = 5
    
    # ==================== 增量缓存 ====================
    cache_enabled: bool = True
    cache_backend: str = "redis"
    cache_key_prefix: str = "xcnstock:kline:last_date"
    cache_ttl: int = 86400
    
    # ==================== 批处理 ====================
    batch_enabled: bool = True
    group_by: str = "sector"
    batch_size: int = 50
    parallel_batches: int = 4
    
    # ==================== 断点续传 ====================
    checkpoint_enabled: bool = True
    checkpoint_file: str = "data/kline/.fetch_progress.json"
    auto_save_interval: int = 10
    save_on_error: bool = True
    max_resume_age_hours: int = 24
    
    # ==================== 快速检查 ====================
    quick_check_enabled: bool = True
    quick_check_skip_days: int = 1
    use_redis_for_check: bool = True
    
    # ==================== 退市检测与降级 ====================
    auto_blacklist_no_data: bool = True
    fallback_enabled: bool = True
    max_fallback_attempts: int = 1
    blacklist_threshold: int = 3  # 新增：连续失败次数阈值
    
    # ==================== 重试机制 ====================
    max_retries: int = 3
    retry_delay_base: float = 0.5
    
    # ==================== 向后兼容（已弃用） ====================
    split_date_days_ago: int = 180  # 仅用于兼容旧配置
    
    def __post_init__(self):
        """构造后验证和规范化"""
        self._validate_and_normalize()
    
    def _validate_and_normalize(self):
        """验证所有配置参数"""
        errors = self.validate()
        if errors:
            error_msg = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(f"配置验证失败:\n{error_msg}")
    
    @classmethod
    def from_yaml(cls, yaml_data: Dict[str, Any]) -> 'DualSourceConfig':
        """
        从 YAML 字典创建配置实例
        
        Args:
            yaml_data: 解析后的 YAML 配置字典
            
        Returns:
            DualSourceConfig 实例
        """
        def get(path: str, default=None):
            """安全获取嵌套字典值"""
            keys = path.split('.')
            value = yaml_data
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
            return value
        
        return cls(
            enabled=get("dual_source.enabled", True),
            split_mode=get("dual_source.split_mode", "position"),
            split_ratio=get("dual_source.split_ratio", 0.5),
            
            baostock_concurrency=_clamp(get("dual_source.baostock_concurrency", 4), 1, 16),
            tencent_concurrency=_clamp(get("dual_source.tencent_concurrency", 32), 1, 64),
            
            cross_validate=get("dual_source.cross_validate", True),
            validation_overlap_days=get("dual_source.validation_overlap_days", 5),
            
            cache_enabled=get("incremental_cache.enabled", True),
            cache_backend=get("incremental_cache.backend", "redis"),
            cache_key_prefix=get("incremental_cache.redis_key_prefix", "xcnstock:kline:last_date"),
            cache_ttl=get("incremental_cache.ttl", 86400),
            
            batch_enabled=get("batch_processing.enabled", True),
            group_by=get("batch_processing.group_by", "sector"),
            batch_size=_clamp(get("batch_processing.batch_size", 50), 1, 200),
            parallel_batches=get("batch_processing.parallel_batches", 4),
            
            checkpoint_enabled=get("checkpoint.enabled", True),
            checkpoint_file=get("checkpoint.file", "data/kline/.fetch_progress.json"),
            auto_save_interval=get("checkpoint.auto_save_interval", 10),
            save_on_error=get("checkpoint.save_on_error", True),
            max_resume_age_hours=get("checkpoint.max_resume_age_hours", 24),
            
            quick_check_enabled=get("checkpoint.quick_check.enabled", True),
            quick_check_skip_days=get("checkpoint.quick_check.skip_fresh_days", 1),
            use_redis_for_check=get("checkpoint.quick_check.use_redis_cache", True),
            
            auto_blacklist_no_data=get("delisting_detection.auto_blacklist_on_no_data", True),
            fallback_enabled=get("delisting_detection.fallback_enabled", True),
            max_fallback_attempts=get("delisting_detection.max_fallback_attempts", 1),
            blacklist_threshold=get("delisting_detection.blacklist_threshold", 3),
            
            max_retries=_clamp(get("retry.max_attempts", 3), 0, 10),
            retry_delay_base=_clamp(get("retry.delay_base_seconds", 0.5), 0, 10),
            
            split_date_days_ago=get("dual_source.split_date_days_ago", 180)
        )
    
    def validate(self) -> List[str]:
        """
        验证配置有效性
        
        Returns:
            错误信息列表（空列表表示有效）
        """
        errors = []
        
        if self.split_mode not in ("position", "date"):
            errors.append(f"无效的分割模式: {self.split_mode} (必须是 'position' 或 'date')")
        
        if not (0 < self.split_ratio < 1):
            errors.append(f"分割比例必须在 0-1 之间: {self.split_ratio}")
        
        if not (1 <= self.baostock_concurrency <= 16):
            errors.append(f"Baostock并发数超出范围(1-16): {self.baostock_concurrency}")
        
        if not (1 <= self.tencent_concurrency <= 64):
            errors.append(f"腾讯并发数超出范围(1-64): {self.tencent_concurrency}")
        
        if not (1 <= self.batch_size <= 200):
            errors.append(f"批次大小超出范围(1-200): {self.batch_size}")
        
        if not (0 <= self.max_retries <= 10):
            errors.append(f"重试次数超出范围(0-10): {self.max_retries}")
        
        if not (0 <= self.retry_delay_base <= 10):
            errors.append(f"重试延迟基数超出范围(0-10s): {self.retry_delay_base}")
        
        if not (1 <= self.blacklist_threshold <= 10):
            errors.append(f"黑名单阈值超出范围(1-10): {self.blacklist_threshold}")
        
        if not (1 <= self.validation_overlap_days <= 30):
            errors.append(f"验证重叠天数超出范围(1-30): {self.validation_overlap_days}")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于日志或序列化）"""
        return {
            'split_mode': self.split_mode,
            'split_ratio': self.split_ratio,
            'baostock_concurrency': self.baostock_concurrency,
            'tencent_concurrency': self.tencent_concurrency,
            'batch_size': self.batch_size,
            'max_retries': self.max_retries,
            'fallback_enabled': self.fallback_enabled,
            'auto_blacklist': self.auto_blacklist_no_data,
            'checkpoint_enabled': self.checkpoint_enabled,
            'quick_check_enabled': self.quick_check_enabled,
        }
    
    def __str__(self) -> str:
        """友好的字符串表示"""
        parts = [
            f"DualSourceConfig(mode={self.split_mode}, ratio={self.split_ratio:.0%})",
            f"  Baostock: concurrency={self.baostock_concurrency}",
            f"  Tencent: concurrency={self.tencent_concurrency}",
            f"  Batch: size={self.batch_size}, parallel={self.parallel_batches}",
            f"  Retry: max={self.max_retries}, delay={self.retry_delay_base}s",
            f"  Fallback: {'✓' if self.fallback_enabled else '✗'}",
            f"  Blacklist: threshold={self.blacklist_threshold}",
        ]
        return "\n".join(parts)


def _clamp(value, min_val, max_val):
    """将值限制在指定范围内"""
    return max(min_val, min(value, max_val))


def get_default_config() -> DualSourceConfig:
    """
    获取默认配置实例（从项目 YAML 文件加载）
    
    Returns:
        DualSourceConfig 实例
    """
    try:
        from services.data_service.config.kline_config import get_kline_config
        yaml_data = get_kline_config()
        return DualSourceConfig.from_yaml(yaml_data)
    except Exception as e:
        import logging
        logging.warning(f"无法从YAML加载配置，使用默认值: {e}")
        return DualSourceConfig()
