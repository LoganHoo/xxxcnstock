#!/usr/bin/env python3
"""
数据血缘追踪器

追踪数据从采集到应用的完整血缘链路：
- 原始数据源 -> 清洗数据 -> 特征数据 -> 策略信号
"""
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from enum import Enum

from .datahub_client import DataHubClient, LineageEdge, get_datahub_client

logger = logging.getLogger(__name__)


class TransformationType(Enum):
    """数据转换类型"""
    RAW_INGESTION = "raw_ingestion"      # 原始数据采集
    CLEANING = "cleaning"                 # 数据清洗
    TRANSFORMATION = "transformation"     # 数据转换
    AGGREGATION = "aggregation"           # 数据聚合
    JOIN = "join"                         # 数据关联
    FEATURE_ENGINEERING = "feature_engineering"  # 特征工程
    MODEL_PREDICTION = "model_prediction" # 模型预测
    SIGNAL_GENERATION = "signal_generation"  # 信号生成


@dataclass
class DataAsset:
    """数据资产"""
    name: str
    type: str  # dataset, model, report
    platform: str  # parquet, mysql, model
    description: str
    schema: Dict[str, Any]
    owner: str = "data_team"
    tags: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class LineageNode:
    """血缘节点"""
    asset: DataAsset
    inputs: List['LineageNode'] = field(default_factory=list)
    transformation: Optional[TransformationType] = None
    transformation_desc: str = ""
    
    def add_input(self, node: 'LineageNode', transformation: TransformationType, desc: str = ""):
        """添加上游输入"""
        self.inputs.append(node)
        self.transformation = transformation
        self.transformation_desc = desc


class LineageTracker:
    """数据血缘追踪器"""
    
    def __init__(self, datahub_client: Optional[DataHubClient] = None):
        self.client = datahub_client or get_datahub_client()
        self.assets: Dict[str, DataAsset] = {}
        self.lineage_edges: List[LineageEdge] = []
        self._lineage_dir = Path("data/metadata/lineage")
        self._lineage_dir.mkdir(parents=True, exist_ok=True)
    
    def register_asset(self, asset: DataAsset) -> bool:
        """
        注册数据资产
        
        Args:
            asset: 数据资产
            
        Returns:
            是否成功
        """
        self.assets[asset.name] = asset
        
        # 推送到 DataHub
        from .datahub_client import DatasetMetadata
        
        schema_fields = []
        for field_name, field_info in asset.schema.items():
            if isinstance(field_info, dict):
                schema_fields.append({
                    'name': field_name,
                    'type': field_info.get('type', 'string'),
                    'description': field_info.get('description', ''),
                    'nullable': field_info.get('nullable', True)
                })
            else:
                schema_fields.append({
                    'name': field_name,
                    'type': str(field_info),
                    'nullable': True
                })
        
        metadata = DatasetMetadata(
            name=asset.name,
            platform=asset.platform,
            env="PROD",
            description=asset.description,
            schema_fields=schema_fields,
            properties={
                'type': asset.type,
                'owner': asset.owner
            },
            tags=asset.tags,
            owners=[asset.owner],
            created_at=asset.created_at.isoformat() if asset.created_at else None
        )
        
        success = self.client.emit_dataset(metadata)
        if success:
            logger.info(f"✅ 数据资产已注册: {asset.name}")
        
        return success
    
    def record_lineage(
        self,
        upstream: str,
        downstream: str,
        transformation: TransformationType,
        description: str = ""
    ) -> bool:
        """
        记录血缘关系
        
        Args:
            upstream: 上游数据集名称
            downstream: 下游数据集名称
            transformation: 转换类型
            description: 转换描述
            
        Returns:
            是否成功
        """
        lineage = LineageEdge(
            upstream_dataset=upstream,
            downstream_dataset=downstream,
            transformation_type=transformation.value,
            transformation_sql=description,
            created_at=datetime.now().isoformat()
        )
        
        self.lineage_edges.append(lineage)
        
        # 推送到 DataHub
        success = self.client.emit_lineage(lineage)
        if success:
            logger.info(f"✅ 血缘关系已记录: {upstream} -> {downstream}")
        
        return success
    
    def build_kline_lineage(self) -> List[LineageEdge]:
        """
        构建K线数据血缘链路
        
        链路: Baostock/Tushare -> Raw Kline -> Cleaned Kline -> Features -> Signals
        """
        edges = []
        
        # 1. 原始数据采集
        edges.append(LineageEdge(
            upstream_dataset="baostock_api",
            downstream_dataset="raw_kline_data",
            transformation_type=TransformationType.RAW_INGESTION.value,
            transformation_sql="从Baostock API采集原始K线数据",
            created_at=datetime.now().isoformat()
        ))
        
        # 2. 数据清洗
        edges.append(LineageEdge(
            upstream_dataset="raw_kline_data",
            downstream_dataset="cleaned_kline_data",
            transformation_type=TransformationType.CLEANING.value,
            transformation_sql="数据清洗: 去重、填充缺失值、异常值处理",
            created_at=datetime.now().isoformat()
        ))
        
        # 3. 特征工程
        edges.append(LineageEdge(
            upstream_dataset="cleaned_kline_data",
            downstream_dataset="technical_features",
            transformation_type=TransformationType.FEATURE_ENGINEERING.value,
            transformation_sql="计算技术指标: MACD, KDJ, RSI, 均线等",
            created_at=datetime.now().isoformat()
        ))
        
        # 4. 信号生成
        edges.append(LineageEdge(
            upstream_dataset="technical_features",
            downstream_dataset="trading_signals",
            transformation_type=TransformationType.SIGNAL_GENERATION.value,
            transformation_sql="生成交易信号: 金叉、突破、主力痕迹等",
            created_at=datetime.now().isoformat()
        ))
        
        # 推送到 DataHub
        for edge in edges:
            self.client.emit_lineage(edge)
            self.lineage_edges.append(edge)
        
        logger.info(f"✅ K线数据血缘链路已构建: {len(edges)} 条关系")
        return edges
    
    def get_upstream_dependencies(self, dataset_name: str) -> List[str]:
        """获取数据集的上游依赖"""
        upstreams = []
        for edge in self.lineage_edges:
            if edge.downstream_dataset == dataset_name:
                upstreams.append(edge.upstream_dataset)
        return upstreams
    
    def get_downstream_impact(self, dataset_name: str) -> List[str]:
        """获取数据集的下游影响"""
        downstreams = []
        for edge in self.lineage_edges:
            if edge.upstream_dataset == dataset_name:
                downstreams.append(edge.downstream_dataset)
        return downstreams
    
    def export_lineage_graph(self) -> Dict[str, Any]:
        """导出血缘关系图"""
        nodes = set()
        edges = []
        
        for edge in self.lineage_edges:
            nodes.add(edge.upstream_dataset)
            nodes.add(edge.downstream_dataset)
            edges.append({
                'source': edge.upstream_dataset,
                'target': edge.downstream_dataset,
                'type': edge.transformation_type,
                'description': edge.transformation_sql
            })
        
        return {
            'nodes': [{'id': n, 'name': n} for n in nodes],
            'edges': edges,
            'generated_at': datetime.now().isoformat()
        }
    
    def save_lineage_to_file(self, filename: str = "lineage_graph.json"):
        """保存血缘关系到文件"""
        import json
        
        graph = self.export_lineage_graph()
        file_path = self._lineage_dir / filename
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(graph, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 血缘关系图已保存: {file_path}")
        return file_path


# 便捷函数
def get_lineage_tracker() -> LineageTracker:
    """获取血缘追踪器实例"""
    return LineageTracker()
