#!/usr/bin/env python3
"""
数据目录服务

提供数据资产的发现、搜索和管理功能
"""
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from enum import Enum

from .schema_registry import SchemaRegistry, get_schema_registry
from .lineage_tracker import LineageTracker, get_lineage_tracker

logger = logging.getLogger(__name__)


class DataAssetType(Enum):
    """数据资产类型"""
    DATASET = "dataset"           # 数据集
    MODEL = "model"               # 机器学习模型
    REPORT = "report"             # 报告
    DASHBOARD = "dashboard"       # 仪表盘
    PIPELINE = "pipeline"         # 数据管道


class DataDomain(Enum):
    """数据域"""
    MARKET_DATA = "market_data"           # 市场数据
    FUNDAMENTAL = "fundamental"           # 基本面数据
    TECHNICAL = "technical"               # 技术指标
    ALTERNATIVE = "alternative"           # 另类数据
    DERIVED = "derived"                   # 衍生数据


@dataclass
class DataAssetInfo:
    """数据资产信息"""
    name: str
    type: DataAssetType
    domain: DataDomain
    description: str
    owner: str
    tags: List[str]
    created_at: datetime
    updated_at: datetime
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    quality_score: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'type': self.type.value,
            'domain': self.domain.value,
            'description': self.description,
            'owner': self.owner,
            'tags': self.tags,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'row_count': self.row_count,
            'size_bytes': self.size_bytes,
            'quality_score': self.quality_score
        }


class DataCatalog:
    """数据目录"""
    
    def __init__(
        self,
        catalog_dir: Optional[Path] = None,
        schema_registry: Optional[SchemaRegistry] = None,
        lineage_tracker: Optional[LineageTracker] = None
    ):
        self.catalog_dir = catalog_dir or Path("data/metadata/catalog")
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        
        self.schema_registry = schema_registry or get_schema_registry()
        self.lineage_tracker = lineage_tracker or get_lineage_tracker()
        
        self._assets: Dict[str, DataAssetInfo] = {}
        self._load_catalog()
    
    def _load_catalog(self):
        """加载数据目录"""
        catalog_file = self.catalog_dir / "catalog.json"
        if catalog_file.exists():
            try:
                with open(catalog_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                for name, asset_data in data.get('assets', {}).items():
                    self._assets[name] = DataAssetInfo(
                        name=asset_data['name'],
                        type=DataAssetType(asset_data['type']),
                        domain=DataDomain(asset_data['domain']),
                        description=asset_data['description'],
                        owner=asset_data['owner'],
                        tags=asset_data['tags'],
                        created_at=datetime.fromisoformat(asset_data['created_at']),
                        updated_at=datetime.fromisoformat(asset_data['updated_at']),
                        row_count=asset_data.get('row_count'),
                        size_bytes=asset_data.get('size_bytes'),
                        quality_score=asset_data.get('quality_score')
                    )
                
                logger.info(f"已加载数据目录: {len(self._assets)} 个资产")
            except Exception as e:
                logger.error(f"加载数据目录失败: {e}")
    
    def register_asset(self, asset: DataAssetInfo) -> bool:
        """
        注册数据资产到目录
        
        Args:
            asset: 数据资产信息
            
        Returns:
            是否成功
        """
        self._assets[asset.name] = asset
        self._save_catalog()
        
        logger.info(f"✅ 数据资产已注册到目录: {asset.name}")
        return True
    
    def search(
        self,
        query: str,
        asset_type: Optional[DataAssetType] = None,
        domain: Optional[DataDomain] = None,
        tags: Optional[List[str]] = None
    ) -> List[DataAssetInfo]:
        """
        搜索数据资产
        
        Args:
            query: 搜索关键词
            asset_type: 资产类型筛选
            domain: 数据域筛选
            tags: 标签筛选
            
        Returns:
            匹配的资产列表
        """
        results = []
        query_lower = query.lower()
        
        for asset in self._assets.values():
            # 关键词匹配
            match = (
                query_lower in asset.name.lower() or
                query_lower in asset.description.lower() or
                any(query_lower in tag.lower() for tag in asset.tags)
            )
            
            if not match:
                continue
            
            # 类型筛选
            if asset_type and asset.type != asset_type:
                continue
            
            # 域筛选
            if domain and asset.domain != domain:
                continue
            
            # 标签筛选
            if tags and not all(tag in asset.tags for tag in tags):
                continue
            
            results.append(asset)
        
        # 按质量评分排序
        results.sort(key=lambda x: x.quality_score or 0, reverse=True)
        
        return results
    
    def get_asset(self, name: str) -> Optional[DataAssetInfo]:
        """获取数据资产信息"""
        return self._assets.get(name)
    
    def get_assets_by_domain(self, domain: DataDomain) -> List[DataAssetInfo]:
        """按数据域获取资产"""
        return [a for a in self._assets.values() if a.domain == domain]
    
    def get_assets_by_owner(self, owner: str) -> List[DataAssetInfo]:
        """按所有者获取资产"""
        return [a for a in self._assets.values() if a.owner == owner]
    
    def update_asset_stats(
        self,
        name: str,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        quality_score: Optional[float] = None
    ):
        """更新资产统计信息"""
        if name in self._assets:
            asset = self._assets[name]
            if row_count is not None:
                asset.row_count = row_count
            if size_bytes is not None:
                asset.size_bytes = size_bytes
            if quality_score is not None:
                asset.quality_score = quality_score
            asset.updated_at = datetime.now()
            
            self._save_catalog()
            logger.debug(f"已更新资产统计: {name}")
    
    def get_asset_details(self, name: str) -> Dict[str, Any]:
        """获取资产详细信息"""
        asset = self._assets.get(name)
        if not asset:
            return {}
        
        # 获取 Schema
        schema = self.schema_registry.get_schema(name)
        
        # 获取血缘关系
        upstreams = self.lineage_tracker.get_upstream_dependencies(name)
        downstreams = self.lineage_tracker.get_downstream_impact(name)
        
        return {
            'asset': asset.to_dict(),
            'schema': schema.to_dict() if schema else None,
            'lineage': {
                'upstreams': upstreams,
                'downstreams': downstreams
            }
        }
    
    def _save_catalog(self):
        """保存数据目录"""
        data = {
            'assets': {name: asset.to_dict() for name, asset in self._assets.items()},
            'updated_at': datetime.now().isoformat()
        }
        
        catalog_file = self.catalog_dir / "catalog.json"
        with open(catalog_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def export_catalog(self) -> Dict[str, Any]:
        """导出完整目录"""
        return {
            'assets': [asset.to_dict() for asset in self._assets.values()],
            'statistics': {
                'total_assets': len(self._assets),
                'by_type': {
                    t.value: len([a for a in self._assets.values() if a.type == t])
                    for t in DataAssetType
                },
                'by_domain': {
                    d.value: len([a for a in self._assets.values() if a.domain == d])
                    for d in DataDomain
                }
            },
            'generated_at': datetime.now().isoformat()
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取目录统计信息"""
        total = len(self._assets)
        
        by_type = {}
        for t in DataAssetType:
            count = len([a for a in self._assets.values() if a.type == t])
            if count > 0:
                by_type[t.value] = count
        
        by_domain = {}
        for d in DataDomain:
            count = len([a for a in self._assets.values() if a.domain == d])
            if count > 0:
                by_domain[d.value] = count
        
        # 质量评分分布
        quality_scores = [a.quality_score for a in self._assets.values() if a.quality_score is not None]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        return {
            'total_assets': total,
            'by_type': by_type,
            'by_domain': by_domain,
            'average_quality_score': round(avg_quality, 2),
            'updated_at': datetime.now().isoformat()
        }


# 便捷函数
_catalog_instance: Optional[DataCatalog] = None


def get_data_catalog() -> DataCatalog:
    """获取数据目录实例"""
    global _catalog_instance
    if _catalog_instance is None:
        _catalog_instance = DataCatalog()
    return _catalog_instance


def initialize_default_catalog():
    """初始化默认数据目录"""
    catalog = get_data_catalog()
    schema_registry = get_schema_registry()
    
    from .schema_registry import get_kline_schema, get_stock_list_schema, get_technical_features_schema
    
    # 1. 注册 K线数据
    kline_schema = get_kline_schema()
    schema_registry.register_schema(
        "kline_data",
        kline_schema,
        created_by="system",
        change_log="初始化K线数据Schema"
    )
    
    catalog.register_asset(DataAssetInfo(
        name="kline_data",
        type=DataAssetType.DATASET,
        domain=DataDomain.MARKET_DATA,
        description="股票K线数据，包含开盘价、收盘价、最高价、最低价、成交量等",
        owner="data_team",
        tags=["kline", "ohlc", "market", "daily"],
        created_at=datetime.now(),
        updated_at=datetime.now(),
        row_count=None,
        quality_score=0.95
    ))
    
    # 2. 注册股票列表
    stock_list_schema = get_stock_list_schema()
    schema_registry.register_schema(
        "stock_list",
        stock_list_schema,
        created_by="system",
        change_log="初始化股票列表Schema"
    )
    
    catalog.register_asset(DataAssetInfo(
        name="stock_list",
        type=DataAssetType.DATASET,
        domain=DataDomain.MARKET_DATA,
        description="A股股票列表，包含股票代码、名称、行业、市场等信息",
        owner="data_team",
        tags=["stock", "list", "basic"],
        created_at=datetime.now(),
        updated_at=datetime.now(),
        row_count=None,
        quality_score=0.98
    ))
    
    # 3. 注册技术指标
    tech_schema = get_technical_features_schema()
    schema_registry.register_schema(
        "technical_features",
        tech_schema,
        created_by="system",
        change_log="初始化技术指标Schema"
    )
    
    catalog.register_asset(DataAssetInfo(
        name="technical_features",
        type=DataAssetType.DATASET,
        domain=DataDomain.TECHNICAL,
        description="技术指标特征数据，包含MACD、KDJ、RSI、均线等",
        owner="data_team",
        tags=["technical", "features", "macd", "kdj", "rsi"],
        created_at=datetime.now(),
        updated_at=datetime.now(),
        row_count=None,
        quality_score=0.92
    ))
    
    logger.info("✅ 默认数据目录已初始化")
    return catalog
