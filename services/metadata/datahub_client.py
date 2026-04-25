#!/usr/bin/env python3
"""
DataHub 客户端

集成 LinkedIn DataHub 元数据平台，提供：
- 数据资产注册
- 元数据推送
- 血缘关系记录
- Schema 变更追踪
"""
import os
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# 尝试导入 DataHub SDK
try:
    from datahub.emitter.mce_builder import (
        make_dataset_urn,
        make_schema_field_urn,
        make_data_platform_urn,
    )
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass,
        SchemaMetadataClass,
        SchemaFieldClass,
        SchemaFieldDataTypeClass,
        StringTypeClass,
        NumberTypeClass,
        DateTypeClass,
        AuditStampClass,
        StatusClass,
        UpstreamLineageClass,
        UpstreamClass,
        DatasetLineageTypeClass,
    )
    DATAHUB_AVAILABLE = True
except ImportError:
    logger.warning("DataHub SDK 未安装，使用模拟模式")
    DATAHUB_AVAILABLE = False


@dataclass
class DatasetMetadata:
    """数据集元数据"""
    name: str
    platform: str  # parquet, mysql, kafka
    env: str  # PROD, DEV
    description: str
    schema_fields: List[Dict[str, Any]]
    properties: Dict[str, str]
    tags: List[str]
    owners: List[str]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class LineageEdge:
    """血缘关系边"""
    upstream_dataset: str
    downstream_dataset: str
    transformation_type: str  # ETL, AGGREGATION, JOIN
    transformation_sql: Optional[str] = None
    created_at: Optional[str] = None


class DataHubClient:
    """DataHub 客户端"""
    
    def __init__(
        self,
        server_url: Optional[str] = None,
        token: Optional[str] = None
    ):
        self.server_url = server_url or os.getenv('DATAHUB_SERVER_URL', 'http://localhost:8080')
        self.token = token or os.getenv('DATAHUB_TOKEN')
        self.emitter = None
        
        if DATAHUB_AVAILABLE:
            try:
                self.emitter = DatahubRestEmitter(
                    gms_server=self.server_url,
                    token=self.token
                )
                logger.info(f"DataHub 客户端初始化成功: {self.server_url}")
            except Exception as e:
                logger.error(f"DataHub 客户端初始化失败: {e}")
                self.emitter = None
        else:
            logger.info("DataHub SDK 不可用，使用本地元数据存储模式")
    
    def emit_dataset(self, metadata: DatasetMetadata) -> bool:
        """
        推送数据集元数据到 DataHub
        
        Args:
            metadata: 数据集元数据
            
        Returns:
            是否成功
        """
        if not DATAHUB_AVAILABLE or not self.emitter:
            return self._emit_dataset_local(metadata)
        
        try:
            # 构建 Dataset URN
            dataset_urn = make_dataset_urn(
                platform=metadata.platform,
                name=metadata.name,
                env=metadata.env
            )
            
            # 构建 Schema 字段
            schema_fields = []
            for field in metadata.schema_fields:
                field_type = self._get_field_type(field.get('type', 'string'))
                schema_fields.append(
                    SchemaFieldClass(
                        fieldPath=field['name'],
                        type=SchemaFieldDataTypeClass(type=field_type),
                        nativeDataType=field.get('native_type', 'string'),
                        description=field.get('description', ''),
                        nullable=field.get('nullable', True)
                    )
                )
            
            # 构建 Schema 元数据
            actor_urn = "urn:li:corpuser:admin"
            current_time = int(datetime.now().timestamp() * 1000)
            schema_metadata = SchemaMetadataClass(
                schemaName=metadata.name,
                platform=make_data_platform_urn(metadata.platform),
                version=0,
                created=AuditStampClass(time=current_time, actor=actor_urn),
                lastModified=AuditStampClass(time=current_time, actor=actor_urn),
                hash="",
                platformSchema=DatasetPropertiesClass(
                    customProperties=metadata.properties
                ),
                fields=schema_fields
            )
            
            # 构建 Dataset 属性
            dataset_properties = DatasetPropertiesClass(
                name=metadata.name,
                description=metadata.description,
                customProperties=metadata.properties,
                tags=metadata.tags
            )
            
            # 发送元数据
            self.emitter.emit_mce({
                "proposedSnapshot": {
                    "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                        "urn": dataset_urn,
                        "aspects": [
                            dataset_properties,
                            schema_metadata,
                            StatusClass(removed=False)
                        ]
                    }
                }
            })
            
            logger.info(f"✅ 数据集元数据已推送: {metadata.name}")
            return True
            
        except Exception as e:
            logger.exception(f"❌ 推送数据集元数据失败: {metadata.name}")
            return self._emit_dataset_local(metadata)
    
    def emit_lineage(self, lineage: LineageEdge) -> bool:
        """
        推送血缘关系到 DataHub
        
        Args:
            lineage: 血缘关系
            
        Returns:
            是否成功
        """
        if not DATAHUB_AVAILABLE or not self.emitter:
            return self._emit_lineage_local(lineage)
        
        try:
            # 构建 URN
            upstream_urn = make_dataset_urn(
                platform="parquet",
                name=lineage.upstream_dataset,
                env="PROD"
            )
            downstream_urn = make_dataset_urn(
                platform="parquet",
                name=lineage.downstream_dataset,
                env="PROD"
            )
            
            # 构建血缘关系
            lineage_aspect = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=upstream_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                        auditStamp=AuditStampClass(
                            time=int(datetime.now().timestamp() * 1000),
                            actor="urn:li:corpuser:admin"
                        )
                    )
                ]
            )
            
            # 发送血缘关系
            self.emitter.emit_mce({
                "proposedSnapshot": {
                    "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                        "urn": downstream_urn,
                        "aspects": [lineage_aspect]
                    }
                }
            })
            
            logger.info(f"✅ 血缘关系已推送: {lineage.upstream_dataset} -> {lineage.downstream_dataset}")
            return True
            
        except Exception as e:
            logger.exception(f"❌ 推送血缘关系失败")
            return self._emit_lineage_local(lineage)
    
    def _get_field_type(self, type_name: str):
        """获取字段类型"""
        type_mapping = {
            'string': StringTypeClass(),
            'str': StringTypeClass(),
            'int': NumberTypeClass(),
            'integer': NumberTypeClass(),
            'float': NumberTypeClass(),
            'double': NumberTypeClass(),
            'date': DateTypeClass(),
            'datetime': DateTypeClass(),
        }
        return type_mapping.get(type_name.lower(), StringTypeClass())
    
    def _emit_dataset_local(self, metadata: DatasetMetadata) -> bool:
        """本地存储数据集元数据（降级方案）"""
        try:
            metadata_dir = Path("data/metadata")
            metadata_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = metadata_dir / f"{metadata.name.replace('/', '_')}.json"
            
            data = asdict(metadata)
            data['emitted_at'] = datetime.now().isoformat()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 数据集元数据已本地存储: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"本地存储数据集元数据失败: {e}")
            return False
    
    def _emit_lineage_local(self, lineage: LineageEdge) -> bool:
        """本地存储血缘关系（降级方案）"""
        try:
            lineage_dir = Path("data/metadata/lineage")
            lineage_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = lineage_dir / f"{lineage.downstream_dataset.replace('/', '_')}_lineage.json"
            
            # 读取现有血缘关系
            existing_lineage = []
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_lineage = json.load(f)
            
            # 添加新血缘关系
            data = asdict(lineage)
            data['emitted_at'] = datetime.now().isoformat()
            existing_lineage.append(data)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_lineage, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 血缘关系已本地存储: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"本地存储血缘关系失败: {e}")
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        status = {
            'sdk_available': DATAHUB_AVAILABLE,
            'emitter_ready': self.emitter is not None,
            'server_url': self.server_url,
            'timestamp': datetime.now().isoformat()
        }
        
        if self.emitter:
            try:
                # 测试连接
                self.emitter.test_connection()
                status['connection'] = 'healthy'
            except Exception as e:
                status['connection'] = f'unhealthy: {e}'
        
        return status


# 便捷函数
def get_datahub_client() -> DataHubClient:
    """获取 DataHub 客户端实例"""
    return DataHubClient()
