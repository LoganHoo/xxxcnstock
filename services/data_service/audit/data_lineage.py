#!/usr/bin/env python3
"""
数据血缘追踪模块

追踪数据的完整生命周期:
- 数据来源追踪
- 数据转换记录
- 数据依赖关系
- 血缘图谱构建
"""
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, asdict, field
from enum import Enum
import networkx as nx

from core.logger import setup_logger
from core.paths import get_data_path


class DataSourceType(Enum):
    """数据源类型"""
    API = "api"                    # API接口
    DATABASE = "database"          # 数据库
    FILE = "file"                  # 文件
    STREAM = "stream"              # 数据流
    CALCULATION = "calculation"    # 计算生成
    MANUAL = "manual"              # 人工录入


class TransformationType(Enum):
    """转换类型"""
    FETCH = "fetch"                # 数据获取
    FILTER = "filter"              # 数据过滤
    TRANSFORM = "transform"        # 数据转换
    AGGREGATE = "aggregate"        # 数据聚合
    JOIN = "join"                  # 数据关联
    CALCULATE = "calculate"        # 数据计算
    VALIDATE = "validate"          # 数据验证
    EXPORT = "export"              # 数据导出


@dataclass
class DataSource:
    """数据源"""
    source_id: str
    source_type: str
    source_name: str
    source_location: str
    source_format: Optional[str] = None
    source_schema: Optional[Dict] = None
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Transformation:
    """数据转换"""
    transformation_id: str
    transformation_type: str
    transformation_name: str
    input_sources: List[str]
    output_target: str
    parameters: Optional[Dict] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    duration_ms: Optional[int] = None
    status: str = "success"
    error_message: Optional[str] = None
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DataLineageNode:
    """数据血缘节点"""
    node_id: str
    node_type: str  # 'source', 'transformation', 'target'
    node_name: str
    data_schema: Optional[Dict] = None
    record_count: Optional[int] = None
    data_hash: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DataLineageEdge:
    """数据血缘边"""
    edge_id: str
    from_node: str
    to_node: str
    edge_type: str  # 'derived_from', 'transformed_to', 'depends_on'
    transformation_id: Optional[str] = None
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DataLineageRecord:
    """数据血缘记录"""
    lineage_id: str
    data_id: str
    data_type: str
    nodes: List[DataLineageNode]
    edges: List[DataLineageEdge]
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'lineage_id': self.lineage_id,
            'data_id': self.data_id,
            'data_type': self.data_type,
            'nodes': [n.to_dict() for n in self.nodes],
            'edges': [e.to_dict() for e in self.edges],
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'metadata': self.metadata
        }


class DataLineageTracker:
    """数据血缘追踪器"""
    
    def __init__(self, storage_dir: Optional[Path] = None):
        """
        初始化数据血缘追踪器
        
        Args:
            storage_dir: 存储目录
        """
        self.storage_dir = storage_dir or get_data_path() / "lineage"
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = setup_logger("data_lineage")
        
        # 内存中的血缘图谱
        self._graph = nx.DiGraph()
        
        # 加载已有数据
        self._load_existing_lineage()
    
    def _generate_id(self, prefix: str = "LIN") -> str:
        """生成唯一ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        random_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6]
        return f"{prefix}{timestamp}{random_suffix}"
    
    def _load_existing_lineage(self):
        """加载已有的血缘数据"""
        try:
            lineage_files = list(self.storage_dir.glob("lineage_*.json"))
            for file in sorted(lineage_files)[-10:]:  # 只加载最近10个
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        self._rebuild_graph(data)
                except Exception as e:
                    self.logger.warning(f"加载血缘文件失败 {file}: {e}")
        except Exception as e:
            self.logger.error(f"加载血缘数据失败: {e}")
    
    def _rebuild_graph(self, lineage_data: Dict):
        """从血缘数据重建图谱"""
        for node in lineage_data.get('nodes', []):
            self._graph.add_node(
                node['node_id'],
                **{k: v for k, v in node.items() if k != 'node_id'}
            )
        
        for edge in lineage_data.get('edges', []):
            self._graph.add_edge(
                edge['from_node'],
                edge['to_node'],
                **{k: v for k, v in edge.items() if k not in ['from_node', 'to_node']}
            )
    
    def register_data_source(self,
                           source_type: DataSourceType,
                           source_name: str,
                           source_location: str,
                           source_format: Optional[str] = None,
                           source_schema: Optional[Dict] = None,
                           metadata: Optional[Dict] = None) -> str:
        """
        注册数据源
        
        Args:
            source_type: 数据源类型
            source_name: 数据源名称
            source_location: 数据源位置
            source_format: 数据格式
            source_schema: 数据模式
            metadata: 元数据
        
        Returns:
            数据源ID
        """
        source_id = self._generate_id("SRC")
        
        source = DataSource(
            source_id=source_id,
            source_type=source_type.value,
            source_name=source_name,
            source_location=source_location,
            source_format=source_format,
            source_schema=source_schema,
            metadata=metadata
        )
        
        # 保存数据源信息
        source_file = self.storage_dir / f"source_{source_id}.json"
        with open(source_file, 'w', encoding='utf-8') as f:
            json.dump(source.to_dict(), f, ensure_ascii=False, indent=2)
        
        # 添加到图谱
        self._graph.add_node(
            source_id,
            node_type='source',
            node_name=source_name,
            **source.to_dict()
        )
        
        self.logger.info(f"注册数据源: {source_name} ({source_id})")
        
        return source_id
    
    def record_transformation(self,
                            transformation_type: TransformationType,
                            transformation_name: str,
                            input_sources: List[str],
                            output_target: str,
                            parameters: Optional[Dict] = None,
                            duration_ms: Optional[int] = None,
                            status: str = "success",
                            error_message: Optional[str] = None,
                            metadata: Optional[Dict] = None) -> str:
        """
        记录数据转换
        
        Args:
            transformation_type: 转换类型
            transformation_name: 转换名称
            input_sources: 输入源ID列表
            output_target: 输出目标ID
            parameters: 转换参数
            duration_ms: 耗时
            status: 状态
            error_message: 错误信息
            metadata: 元数据
        
        Returns:
            转换ID
        """
        transformation_id = self._generate_id("TRF")
        
        transformation = Transformation(
            transformation_id=transformation_id,
            transformation_type=transformation_type.value,
            transformation_name=transformation_name,
            input_sources=input_sources,
            output_target=output_target,
            parameters=parameters,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            metadata=metadata
        )
        
        # 保存转换信息
        transformation_file = self.storage_dir / f"transformation_{transformation_id}.json"
        with open(transformation_file, 'w', encoding='utf-8') as f:
            json.dump(transformation.to_dict(), f, ensure_ascii=False, indent=2)
        
        # 添加到图谱
        self._graph.add_node(
            transformation_id,
            node_type='transformation',
            node_name=transformation_name,
            **transformation.to_dict()
        )
        
        # 添加边
        for source in input_sources:
            edge_id = self._generate_id("EDG")
            self._graph.add_edge(
                source,
                transformation_id,
                edge_id=edge_id,
                edge_type='input_to_transformation',
                transformation_id=transformation_id
            )
        
        edge_id = self._generate_id("EDG")
        self._graph.add_edge(
            transformation_id,
            output_target,
            edge_id=edge_id,
            edge_type='transformation_to_output',
            transformation_id=transformation_id
        )
        
        self.logger.info(f"记录转换: {transformation_name} ({transformation_id})")
        
        return transformation_id
    
    def create_lineage_record(self,
                            data_id: str,
                            data_type: str,
                            nodes: List[DataLineageNode],
                            edges: List[DataLineageEdge],
                            metadata: Optional[Dict] = None) -> str:
        """
        创建血缘记录
        
        Args:
            data_id: 数据ID
            data_type: 数据类型
            nodes: 节点列表
            edges: 边列表
            metadata: 元数据
        
        Returns:
            血缘记录ID
        """
        lineage_id = self._generate_id("LIN")
        
        record = DataLineageRecord(
            lineage_id=lineage_id,
            data_id=data_id,
            data_type=data_type,
            nodes=nodes,
            edges=edges,
            metadata=metadata
        )
        
        # 保存血缘记录
        lineage_file = self.storage_dir / f"lineage_{lineage_id}.json"
        with open(lineage_file, 'w', encoding='utf-8') as f:
            json.dump(record.to_dict(), f, ensure_ascii=False, indent=2)
        
        # 更新图谱
        for node in nodes:
            self._graph.add_node(
                node.node_id,
                node_type=node.node_type,
                node_name=node.node_name,
                **node.to_dict()
            )
        
        for edge in edges:
            self._graph.add_edge(
                edge.from_node,
                edge.to_node,
                edge_id=edge.edge_id,
                edge_type=edge.edge_type,
                **edge.to_dict()
            )
        
        self.logger.info(f"创建血缘记录: {data_id} ({lineage_id})")
        
        return lineage_id
    
    def get_lineage(self, data_id: str) -> Optional[DataLineageRecord]:
        """
        获取数据血缘
        
        Args:
            data_id: 数据ID
        
        Returns:
            血缘记录
        """
        try:
            lineage_files = list(self.storage_dir.glob("lineage_*.json"))
            
            for file in sorted(lineage_files, reverse=True):
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if data.get('data_id') == data_id:
                            return DataLineageRecord(
                                lineage_id=data['lineage_id'],
                                data_id=data['data_id'],
                                data_type=data['data_type'],
                                nodes=[DataLineageNode(**n) for n in data['nodes']],
                                edges=[DataLineageEdge(**e) for e in data['edges']],
                                created_at=data['created_at'],
                                updated_at=data['updated_at'],
                                metadata=data.get('metadata')
                            )
                except Exception as e:
                    self.logger.warning(f"读取血缘文件失败 {file}: {e}")
                    continue
            
            return None
            
        except Exception as e:
            self.logger.error(f"获取血缘失败: {e}")
            return None
    
    def get_upstream(self, data_id: str, depth: int = -1) -> List[Dict]:
        """
        获取上游血缘
        
        Args:
            data_id: 数据ID
            depth: 深度限制(-1表示无限制)
        
        Returns:
            上游节点列表
        """
        upstream = []
        
        try:
            if data_id not in self._graph:
                return upstream
            
            # 使用BFS获取上游节点
            visited = set()
            queue = [(data_id, 0)]
            
            while queue:
                node_id, current_depth = queue.pop(0)
                
                if node_id in visited:
                    continue
                
                visited.add(node_id)
                
                if depth >= 0 and current_depth > depth:
                    continue
                
                if node_id != data_id:
                    node_data = dict(self._graph.nodes[node_id])
                    node_data['node_id'] = node_id
                    node_data['depth'] = current_depth
                    upstream.append(node_data)
                
                # 获取前驱节点
                for predecessor in self._graph.predecessors(node_id):
                    if predecessor not in visited:
                        queue.append((predecessor, current_depth + 1))
            
            return upstream
            
        except Exception as e:
            self.logger.error(f"获取上游血缘失败: {e}")
            return []
    
    def get_downstream(self, data_id: str, depth: int = -1) -> List[Dict]:
        """
        获取下游血缘
        
        Args:
            data_id: 数据ID
            depth: 深度限制(-1表示无限制)
        
        Returns:
            下游节点列表
        """
        downstream = []
        
        try:
            if data_id not in self._graph:
                return downstream
            
            # 使用BFS获取下游节点
            visited = set()
            queue = [(data_id, 0)]
            
            while queue:
                node_id, current_depth = queue.pop(0)
                
                if node_id in visited:
                    continue
                
                visited.add(node_id)
                
                if depth >= 0 and current_depth > depth:
                    continue
                
                if node_id != data_id:
                    node_data = dict(self._graph.nodes[node_id])
                    node_data['node_id'] = node_id
                    node_data['depth'] = current_depth
                    downstream.append(node_data)
                
                # 获取后继节点
                for successor in self._graph.successors(node_id):
                    if successor not in visited:
                        queue.append((successor, current_depth + 1))
            
            return downstream
            
        except Exception as e:
            self.logger.error(f"获取下游血缘失败: {e}")
            return []
    
    def get_impact_analysis(self, data_id: str) -> Dict[str, Any]:
        """
        获取影响分析
        
        Args:
            data_id: 数据ID
        
        Returns:
            影响分析结果
        """
        downstream = self.get_downstream(data_id)
        
        analysis = {
            'data_id': data_id,
            'direct_dependents': [],
            'indirect_dependents': [],
            'total_impact_count': len(downstream),
            'by_type': {}
        }
        
        for node in downstream:
            if node.get('depth') == 1:
                analysis['direct_dependents'].append(node)
            else:
                analysis['indirect_dependents'].append(node)
            
            node_type = node.get('node_type', 'unknown')
            analysis['by_type'][node_type] = analysis['by_type'].get(node_type, 0) + 1
        
        return analysis
    
    def export_lineage_graph(self, output_file: Optional[Path] = None) -> Path:
        """
        导出血缘图谱
        
        Args:
            output_file: 输出文件路径
        
        Returns:
            输出文件路径
        """
        if output_file is None:
            output_file = self.storage_dir / f"lineage_graph_{datetime.now().strftime('%Y%m%d')}.json"
        
        try:
            # 导出节点和边
            graph_data = {
                'nodes': [
                    {'id': node, **data}
                    for node, data in self._graph.nodes(data=True)
                ],
                'edges': [
                    {'source': u, 'target': v, **data}
                    for u, v, data in self._graph.edges(data=True)
                ],
                'exported_at': datetime.now().isoformat()
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(graph_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"血缘图谱已导出: {output_file}")
            
            return output_file
            
        except Exception as e:
            self.logger.error(f"导出血缘图谱失败: {e}")
            raise
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取血缘统计信息
        
        Returns:
            统计信息
        """
        return {
            'total_nodes': self._graph.number_of_nodes(),
            'total_edges': self._graph.number_of_edges(),
            'node_types': {
                node_type: sum(1 for _, data in self._graph.nodes(data=True) 
                             if data.get('node_type') == node_type)
                for node_type in set(data.get('node_type') for _, data in self._graph.nodes(data=True))
            },
            'is_dag': nx.is_directed_acyclic_graph(self._graph),
            'connected_components': nx.number_weakly_connected_components(self._graph)
        }


# 全局血缘追踪器实例
_lineage_tracker: Optional[DataLineageTracker] = None


def get_lineage_tracker() -> DataLineageTracker:
    """获取全局血缘追踪器"""
    global _lineage_tracker
    if _lineage_tracker is None:
        _lineage_tracker = DataLineageTracker()
    return _lineage_tracker
