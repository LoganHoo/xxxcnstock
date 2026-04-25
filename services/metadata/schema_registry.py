#!/usr/bin/env python3
"""
Schema 注册中心

管理数据 Schema 的版本控制和变更追踪
"""
import json
import hashlib
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)


class SchemaChangeType(Enum):
    """Schema 变更类型"""
    ADDED = "added"           # 新增字段
    REMOVED = "removed"       # 删除字段
    MODIFIED = "modified"     # 修改字段
    DEPRECATED = "deprecated" # 废弃字段


@dataclass
class SchemaField:
    """Schema 字段定义"""
    name: str
    type: str
    description: str
    nullable: bool = True
    default_value: Optional[Any] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @property
    def signature(self) -> str:
        """字段签名，用于比较"""
        return f"{self.name}:{self.type}:{self.nullable}"


@dataclass
class SchemaVersion:
    """Schema 版本"""
    version: int
    fields: List[SchemaField]
    created_at: datetime
    created_by: str
    change_log: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'version': self.version,
            'fields': [f.to_dict() for f in self.fields],
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by,
            'change_log': self.change_log
        }
    
    @property
    def hash(self) -> str:
        """Schema 哈希值"""
        content = json.dumps([f.signature for f in self.fields], sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()[:8]


@dataclass
class SchemaChange:
    """Schema 变更记录"""
    change_type: SchemaChangeType
    field_name: str
    old_value: Optional[Any]
    new_value: Optional[Any]
    description: str


class SchemaRegistry:
    """Schema 注册中心"""
    
    def __init__(self, registry_dir: Optional[Path] = None):
        self.registry_dir = registry_dir or Path("data/metadata/schemas")
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self._schemas: Dict[str, List[SchemaVersion]] = {}
        self._load_existing_schemas()
    
    def _load_existing_schemas(self):
        """加载已存在的 Schema"""
        if not self.registry_dir.exists():
            return
        
        for schema_file in self.registry_dir.glob("*.json"):
            dataset_name = schema_file.stem
            try:
                with open(schema_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                versions = []
                for v_data in data.get('versions', []):
                    fields = [SchemaField(**f) for f in v_data['fields']]
                    version = SchemaVersion(
                        version=v_data['version'],
                        fields=fields,
                        created_at=datetime.fromisoformat(v_data['created_at']),
                        created_by=v_data['created_by'],
                        change_log=v_data['change_log']
                    )
                    versions.append(version)
                
                self._schemas[dataset_name] = versions
                logger.debug(f"已加载 Schema: {dataset_name} ({len(versions)} 个版本)")
                
            except Exception as e:
                logger.error(f"加载 Schema 失败 {dataset_name}: {e}")
    
    def register_schema(
        self,
        dataset_name: str,
        fields: List[SchemaField],
        created_by: str = "system",
        change_log: str = ""
    ) -> SchemaVersion:
        """
        注册新的 Schema 版本
        
        Args:
            dataset_name: 数据集名称
            fields: 字段列表
            created_by: 创建者
            change_log: 变更日志
            
        Returns:
            新版本
        """
        # 获取当前版本
        existing_versions = self._schemas.get(dataset_name, [])
        current_version = len(existing_versions)
        
        # 检查是否有变更
        if existing_versions:
            last_version = existing_versions[-1]
            if self._schemas_equal(last_version.fields, fields):
                logger.info(f"Schema 未变更: {dataset_name} (版本 {last_version.version})")
                return last_version
        
        # 创建新版本
        new_version = SchemaVersion(
            version=current_version + 1,
            fields=fields,
            created_at=datetime.now(),
            created_by=created_by,
            change_log=change_log or f"版本 {current_version + 1}"
        )
        
        # 保存
        if dataset_name not in self._schemas:
            self._schemas[dataset_name] = []
        self._schemas[dataset_name].append(new_version)
        
        self._save_schema(dataset_name)
        
        logger.info(f"✅ Schema 已注册: {dataset_name} (版本 {new_version.version})")
        return new_version
    
    def get_schema(self, dataset_name: str, version: Optional[int] = None) -> Optional[SchemaVersion]:
        """
        获取 Schema
        
        Args:
            dataset_name: 数据集名称
            version: 版本号，None 表示最新版本
            
        Returns:
            Schema 版本
        """
        versions = self._schemas.get(dataset_name)
        if not versions:
            return None
        
        if version is None:
            return versions[-1]
        
        for v in versions:
            if v.version == version:
                return v
        
        return None
    
    def get_schema_history(self, dataset_name: str) -> List[SchemaVersion]:
        """获取 Schema 历史版本"""
        return self._schemas.get(dataset_name, [])
    
    def compare_versions(
        self,
        dataset_name: str,
        from_version: int,
        to_version: int
    ) -> List[SchemaChange]:
        """
        比较两个 Schema 版本
        
        Args:
            dataset_name: 数据集名称
            from_version: 起始版本
            to_version: 目标版本
            
        Returns:
            变更列表
        """
        v1 = self.get_schema(dataset_name, from_version)
        v2 = self.get_schema(dataset_name, to_version)
        
        if not v1 or not v2:
            return []
        
        changes = []
        
        # 字段映射
        v1_fields = {f.name: f for f in v1.fields}
        v2_fields = {f.name: f for f in v2.fields}
        
        # 检查新增和修改
        for name, field in v2_fields.items():
            if name not in v1_fields:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.ADDED,
                    field_name=name,
                    old_value=None,
                    new_value=field.to_dict(),
                    description=f"新增字段: {name}"
                ))
            elif v1_fields[name].signature != field.signature:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.MODIFIED,
                    field_name=name,
                    old_value=v1_fields[name].to_dict(),
                    new_value=field.to_dict(),
                    description=f"修改字段: {name}"
                ))
        
        # 检查删除
        for name in v1_fields:
            if name not in v2_fields:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.REMOVED,
                    field_name=name,
                    old_value=v1_fields[name].to_dict(),
                    new_value=None,
                    description=f"删除字段: {name}"
                ))
        
        return changes
    
    def validate_data(self, dataset_name: str, data: Dict[str, Any]) -> List[str]:
        """
        验证数据是否符合 Schema
        
        Args:
            dataset_name: 数据集名称
            data: 数据字典
            
        Returns:
            错误列表
        """
        schema = self.get_schema(dataset_name)
        if not schema:
            return [f"Schema 不存在: {dataset_name}"]
        
        errors = []
        field_map = {f.name: f for f in schema.fields}
        
        # 检查必填字段
        for field in schema.fields:
            if not field.nullable and field.name not in data:
                errors.append(f"缺少必填字段: {field.name}")
        
        # 检查类型
        for field_name, value in data.items():
            if field_name in field_map:
                field = field_map[field_name]
                if not self._validate_type(value, field.type):
                    errors.append(f"字段类型错误: {field_name} (期望 {field.type})")
        
        return errors
    
    def _schemas_equal(self, fields1: List[SchemaField], fields2: List[SchemaField]) -> bool:
        """比较两个 Schema 是否相等"""
        if len(fields1) != len(fields2):
            return False
        
        sig1 = {f.signature for f in fields1}
        sig2 = {f.signature for f in fields2}
        return sig1 == sig2
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """验证值类型"""
        type_mapping = {
            'string': str,
            'str': str,
            'int': int,
            'integer': int,
            'float': float,
            'double': float,
            'bool': bool,
            'boolean': bool,
            'date': str,
            'datetime': str,
        }
        
        expected = type_mapping.get(expected_type.lower())
        if expected is None:
            return True
        
        return isinstance(value, expected)
    
    def _save_schema(self, dataset_name: str):
        """保存 Schema 到文件"""
        versions = self._schemas.get(dataset_name, [])
        
        data = {
            'dataset_name': dataset_name,
            'versions': [v.to_dict() for v in versions],
            'updated_at': datetime.now().isoformat()
        }
        
        file_path = self.registry_dir / f"{dataset_name}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def get_all_schemas(self) -> Dict[str, List[SchemaVersion]]:
        """获取所有 Schema"""
        return self._schemas.copy()
    
    def export_schema_catalog(self) -> Dict[str, Any]:
        """导出 Schema 目录"""
        catalog = {
            'schemas': {},
            'generated_at': datetime.now().isoformat()
        }
        
        for dataset_name, versions in self._schemas.items():
            latest = versions[-1] if versions else None
            if latest:
                catalog['schemas'][dataset_name] = {
                    'current_version': latest.version,
                    'field_count': len(latest.fields),
                    'hash': latest.hash,
                    'fields': [f.to_dict() for f in latest.fields]
                }
        
        return catalog


# 预定义的 Schema
def get_kline_schema() -> List[SchemaField]:
    """K线数据 Schema"""
    return [
        SchemaField('code', 'string', '股票代码', nullable=False, tags=['primary_key']),
        SchemaField('trade_date', 'date', '交易日期', nullable=False, tags=['primary_key', 'partition']),
        SchemaField('open', 'float', '开盘价', nullable=False, tags=['ohlc']),
        SchemaField('high', 'float', '最高价', nullable=False, tags=['ohlc']),
        SchemaField('low', 'float', '最低价', nullable=False, tags=['ohlc']),
        SchemaField('close', 'float', '收盘价', nullable=False, tags=['ohlc']),
        SchemaField('volume', 'integer', '成交量', nullable=False, tags=['volume']),
        SchemaField('amount', 'float', '成交额', nullable=True, tags=['amount']),
        SchemaField('pct_chg', 'float', '涨跌幅(%)', nullable=True, tags=['derived']),
    ]


def get_stock_list_schema() -> List[SchemaField]:
    """股票列表 Schema"""
    return [
        SchemaField('code', 'string', '股票代码', nullable=False, tags=['primary_key']),
        SchemaField('name', 'string', '股票名称', nullable=False),
        SchemaField('industry', 'string', '所属行业', nullable=True),
        SchemaField('market', 'string', '市场(沪市/深市)', nullable=False),
        SchemaField('list_date', 'date', '上市日期', nullable=True),
    ]


def get_technical_features_schema() -> List[SchemaField]:
    """技术指标特征 Schema"""
    return [
        SchemaField('code', 'string', '股票代码', nullable=False, tags=['primary_key']),
        SchemaField('trade_date', 'date', '交易日期', nullable=False, tags=['primary_key']),
        SchemaField('ma5', 'float', '5日均线', nullable=True),
        SchemaField('ma10', 'float', '10日均线', nullable=True),
        SchemaField('ma20', 'float', '20日均线', nullable=True),
        SchemaField('macd_dif', 'float', 'MACD DIF', nullable=True),
        SchemaField('macd_dea', 'float', 'MACD DEA', nullable=True),
        SchemaField('macd_hist', 'float', 'MACD柱状图', nullable=True),
        SchemaField('rsi6', 'float', 'RSI6', nullable=True),
        SchemaField('rsi12', 'float', 'RSI12', nullable=True),
        SchemaField('kdj_k', 'float', 'KDJ K值', nullable=True),
        SchemaField('kdj_d', 'float', 'KDJ D值', nullable=True),
        SchemaField('kdj_j', 'float', 'KDJ J值', nullable=True),
    ]


# 便捷函数
_registry_instance: Optional[SchemaRegistry] = None


def get_schema_registry() -> SchemaRegistry:
    """获取 Schema 注册中心实例"""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = SchemaRegistry()
    return _registry_instance
