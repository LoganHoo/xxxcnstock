#!/usr/bin/env python3
"""
数据变更审计模块

记录和追踪所有数据变更:
- 变更前/后快照
- 变更差异分析
- 变更回滚支持
- 变更历史查询
"""
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import difflib

import pandas as pd
import numpy as np

from core.logger import setup_logger
from core.paths import get_data_path
from .audit_logger import AuditLogger, AuditType, AuditLevel, get_audit_logger


class ChangeType(Enum):
    """变更类型"""
    INSERT = "insert"          # 插入
    UPDATE = "update"          # 更新
    DELETE = "delete"          # 删除
    BATCH_INSERT = "batch_insert"  # 批量插入
    BATCH_UPDATE = "batch_update"  # 批量更新
    BATCH_DELETE = "batch_delete"  # 批量删除


@dataclass
class DataSnapshot:
    """数据快照"""
    snapshot_id: str
    data_id: str
    data_type: str
    data_content: Dict[str, Any]
    data_hash: str
    created_at: str
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dataframe(cls,
                      snapshot_id: str,
                      data_id: str,
                      data_type: str,
                      df: pd.DataFrame,
                      metadata: Optional[Dict] = None) -> 'DataSnapshot':
        """从DataFrame创建快照"""
        # 转换DataFrame为字典
        data_content = {
            'columns': df.columns.tolist(),
            'data': df.to_dict('records'),
            'shape': df.shape
        }
        
        # 计算哈希
        data_str = json.dumps(data_content, sort_keys=True, default=str)
        data_hash = hashlib.sha256(data_str.encode()).hexdigest()
        
        return cls(
            snapshot_id=snapshot_id,
            data_id=data_id,
            data_type=data_type,
            data_content=data_content,
            data_hash=data_hash,
            created_at=datetime.now().isoformat(),
            metadata=metadata
        )


@dataclass
class ChangeRecord:
    """变更记录"""
    change_id: str
    change_type: str
    data_id: str
    data_type: str
    before_snapshot_id: Optional[str]
    after_snapshot_id: Optional[str]
    changed_fields: List[str]
    change_summary: str
    user_id: Optional[str]
    operation_id: Optional[str]
    timestamp: str
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ChangeDiff:
    """变更差异"""
    field: str
    old_value: Any
    new_value: Any
    change_type: str  # 'added', 'removed', 'modified'
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ChangeAuditor:
    """数据变更审计器"""
    
    def __init__(self, storage_dir: Optional[Path] = None):
        """
        初始化变更审计器
        
        Args:
            storage_dir: 存储目录
        """
        self.storage_dir = storage_dir or get_data_path() / "change_audit"
        self.snapshot_dir = self.storage_dir / "snapshots"
        self.change_dir = self.storage_dir / "changes"
        
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.change_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = setup_logger("change_audit")
        self.audit_logger = get_audit_logger()
    
    def _generate_id(self, prefix: str = "CHG") -> str:
        """生成唯一ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        random_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6]
        return f"{prefix}{timestamp}{random_suffix}"
    
    def create_snapshot(self,
                       data_id: str,
                       data_type: str,
                       data: Any,
                       metadata: Optional[Dict] = None) -> str:
        """
        创建数据快照
        
        Args:
            data_id: 数据ID
            data_type: 数据类型
            data: 数据内容(DataFrame或字典)
            metadata: 元数据
        
        Returns:
            快照ID
        """
        snapshot_id = self._generate_id("SNP")
        
        if isinstance(data, pd.DataFrame):
            snapshot = DataSnapshot.from_dataframe(
                snapshot_id=snapshot_id,
                data_id=data_id,
                data_type=data_type,
                df=data,
                metadata=metadata
            )
        else:
            data_content = data if isinstance(data, dict) else {'data': str(data)}
            data_str = json.dumps(data_content, sort_keys=True, default=str)
            data_hash = hashlib.sha256(data_str.encode()).hexdigest()
            
            snapshot = DataSnapshot(
                snapshot_id=snapshot_id,
                data_id=data_id,
                data_type=data_type,
                data_content=data_content,
                data_hash=data_hash,
                created_at=datetime.now().isoformat(),
                metadata=metadata
            )
        
        # 保存快照
        snapshot_file = self.snapshot_dir / f"{snapshot_id}.json"
        with open(snapshot_file, 'w', encoding='utf-8') as f:
            json.dump(snapshot.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.debug(f"创建快照: {snapshot_id} for {data_id}")
        
        return snapshot_id
    
    def record_change(self,
                     change_type: ChangeType,
                     data_id: str,
                     data_type: str,
                     before_data: Optional[Any] = None,
                     after_data: Optional[Any] = None,
                     user_id: Optional[str] = None,
                     operation_id: Optional[str] = None,
                     metadata: Optional[Dict] = None) -> str:
        """
        记录数据变更
        
        Args:
            change_type: 变更类型
            data_id: 数据ID
            data_type: 数据类型
            before_data: 变更前数据
            after_data: 变更后数据
            user_id: 用户ID
            operation_id: 操作ID
            metadata: 元数据
        
        Returns:
            变更记录ID
        """
        change_id = self._generate_id()
        
        # 创建快照
        before_snapshot_id = None
        after_snapshot_id = None
        
        if before_data is not None:
            before_snapshot_id = self.create_snapshot(
                data_id=data_id,
                data_type=data_type,
                data=before_data,
                metadata={'change_id': change_id, 'snapshot_type': 'before'}
            )
        
        if after_data is not None:
            after_snapshot_id = self.create_snapshot(
                data_id=data_id,
                data_type=data_type,
                data=after_data,
                metadata={'change_id': change_id, 'snapshot_type': 'after'}
            )
        
        # 分析变更字段
        changed_fields = []
        change_summary = ""
        
        if before_data is not None and after_data is not None:
            if isinstance(before_data, pd.DataFrame) and isinstance(after_data, pd.DataFrame):
                changed_fields = self._analyze_dataframe_changes(before_data, after_data)
                change_summary = f"DataFrame变更: {len(changed_fields)}个字段受影响"
            elif isinstance(before_data, dict) and isinstance(after_data, dict):
                changed_fields = self._analyze_dict_changes(before_data, after_data)
                change_summary = f"字典变更: {len(changed_fields)}个字段变更"
        elif before_data is None and after_data is not None:
            change_summary = "新增数据"
        elif before_data is not None and after_data is None:
            change_summary = "删除数据"
        
        # 创建变更记录
        record = ChangeRecord(
            change_id=change_id,
            change_type=change_type.value,
            data_id=data_id,
            data_type=data_type,
            before_snapshot_id=before_snapshot_id,
            after_snapshot_id=after_snapshot_id,
            changed_fields=changed_fields,
            change_summary=change_summary,
            user_id=user_id,
            operation_id=operation_id,
            timestamp=datetime.now().isoformat(),
            metadata=metadata
        )
        
        # 保存变更记录
        change_file = self.change_dir / f"{change_id}.json"
        with open(change_file, 'w', encoding='utf-8') as f:
            json.dump(record.to_dict(), f, ensure_ascii=False, indent=2)
        
        # 记录审计日志
        audit_level = AuditLevel.INFO
        if change_type == ChangeType.DELETE or change_type == ChangeType.BATCH_DELETE:
            audit_level = AuditLevel.WARNING
        
        self.audit_logger.log(
            audit_type=AuditType.DATA_MODIFY,
            audit_level=audit_level,
            resource_type=data_type,
            resource_id=data_id,
            action=change_type.value,
            user_id=user_id,
            request_data={'before_snapshot': before_snapshot_id},
            response_data={'after_snapshot': after_snapshot_id},
            metadata={
                'change_id': change_id,
                'changed_fields': changed_fields,
                'operation_id': operation_id
            }
        )
        
        self.logger.info(f"记录变更: {change_type.value} - {data_id} ({change_id})")
        
        return change_id
    
    def _analyze_dataframe_changes(self, before: pd.DataFrame, after: pd.DataFrame) -> List[str]:
        """分析DataFrame变更"""
        changed_fields = []
        
        # 检查列变化
        before_cols = set(before.columns)
        after_cols = set(after.columns)
        
        added_cols = after_cols - before_cols
        removed_cols = before_cols - after_cols
        
        changed_fields.extend(list(added_cols))
        changed_fields.extend(list(removed_cols))
        
        # 检查共同列的数据变化
        common_cols = before_cols & after_cols
        
        for col in common_cols:
            if not before[col].equals(after[col]):
                changed_fields.append(col)
        
        return changed_fields
    
    def _analyze_dict_changes(self, before: Dict, after: Dict) -> List[str]:
        """分析字典变更"""
        changed_fields = []
        
        all_keys = set(before.keys()) | set(after.keys())
        
        for key in all_keys:
            if key not in before:
                changed_fields.append(f"+{key}")
            elif key not in after:
                changed_fields.append(f"-{key}")
            elif before[key] != after[key]:
                changed_fields.append(f"~{key}")
        
        return changed_fields
    
    def get_snapshot(self, snapshot_id: str) -> Optional[DataSnapshot]:
        """
        获取快照
        
        Args:
            snapshot_id: 快照ID
        
        Returns:
            快照对象
        """
        try:
            snapshot_file = self.snapshot_dir / f"{snapshot_id}.json"
            
            if not snapshot_file.exists():
                return None
            
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return DataSnapshot(**data)
            
        except Exception as e:
            self.logger.error(f"获取快照失败 {snapshot_id}: {e}")
            return None
    
    def get_change(self, change_id: str) -> Optional[ChangeRecord]:
        """
        获取变更记录
        
        Args:
            change_id: 变更ID
        
        Returns:
            变更记录
        """
        try:
            change_file = self.change_dir / f"{change_id}.json"
            
            if not change_file.exists():
                return None
            
            with open(change_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return ChangeRecord(**data)
            
        except Exception as e:
            self.logger.error(f"获取变更记录失败 {change_id}: {e}")
            return None
    
    def get_changes(self,
                   data_id: Optional[str] = None,
                   data_type: Optional[str] = None,
                   change_type: Optional[ChangeType] = None,
                   user_id: Optional[str] = None,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   limit: int = 100) -> List[ChangeRecord]:
        """
        查询变更记录
        
        Args:
            data_id: 数据ID
            data_type: 数据类型
            change_type: 变更类型
            user_id: 用户ID
            start_time: 开始时间
            end_time: 结束时间
            limit: 限制数量
        
        Returns:
            变更记录列表
        """
        records = []
        
        change_files = sorted(self.change_dir.glob("*.json"), reverse=True)
        
        for file in change_files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # 应用过滤条件
                if data_id and data.get('data_id') != data_id:
                    continue
                
                if data_type and data.get('data_type') != data_type:
                    continue
                
                if change_type and data.get('change_type') != change_type.value:
                    continue
                
                if user_id and data.get('user_id') != user_id:
                    continue
                
                if start_time:
                    record_time = datetime.fromisoformat(data.get('timestamp', ''))
                    if record_time < start_time:
                        continue
                
                if end_time:
                    record_time = datetime.fromisoformat(data.get('timestamp', ''))
                    if record_time > end_time:
                        continue
                
                records.append(ChangeRecord(**data))
                
                if len(records) >= limit:
                    break
                    
            except Exception as e:
                self.logger.warning(f"读取变更记录失败 {file}: {e}")
                continue
        
        return records
    
    def get_change_diff(self, change_id: str) -> List[ChangeDiff]:
        """
        获取变更差异详情
        
        Args:
            change_id: 变更ID
        
        Returns:
            差异列表
        """
        change = self.get_change(change_id)
        
        if not change:
            return []
        
        diffs = []
        
        # 获取前后快照
        before_snapshot = None
        after_snapshot = None
        
        if change.before_snapshot_id:
            before_snapshot = self.get_snapshot(change.before_snapshot_id)
        
        if change.after_snapshot_id:
            after_snapshot = self.get_snapshot(change.after_snapshot_id)
        
        if not before_snapshot or not after_snapshot:
            return diffs
        
        # 分析差异
        before_data = before_snapshot.data_content
        after_data = after_snapshot.data_content
        
        if 'data' in before_data and 'data' in after_data:
            # DataFrame数据
            before_records = before_data['data']
            after_records = after_data['data']
            
            # 简化的差异分析
            for field in change.changed_fields:
                diffs.append(ChangeDiff(
                    field=field,
                    old_value="...",
                    new_value="...",
                    change_type='modified'
                ))
        
        return diffs
    
    def rollback_change(self, change_id: str, user_id: Optional[str] = None) -> Optional[Any]:
        """
        回滚变更
        
        Args:
            change_id: 变更ID
            user_id: 执行回滚的用户ID
        
        Returns:
            回滚后的数据
        """
        change = self.get_change(change_id)
        
        if not change:
            self.logger.error(f"变更记录不存在: {change_id}")
            return None
        
        if not change.before_snapshot_id:
            self.logger.error(f"变更没有前置快照，无法回滚: {change_id}")
            return None
        
        # 获取前置快照
        before_snapshot = self.get_snapshot(change.before_snapshot_id)
        
        if not before_snapshot:
            self.logger.error(f"前置快照不存在: {change.before_snapshot_id}")
            return None
        
        # 记录回滚操作
        rollback_id = self.record_change(
            change_type=ChangeType.UPDATE,
            data_id=change.data_id,
            data_type=change.data_type,
            before_data=None,  # 当前状态未知
            after_data=before_snapshot.data_content,
            user_id=user_id,
            metadata={'rollback_from': change_id}
        )
        
        self.logger.info(f"执行回滚: {change_id} -> {rollback_id}")
        
        return before_snapshot.data_content
    
    def get_change_statistics(self, days: int = 7) -> Dict[str, Any]:
        """
        获取变更统计信息
        
        Args:
            days: 统计天数
        
        Returns:
            统计信息
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        stats = {
            'total_changes': 0,
            'by_type': {},
            'by_data_type': {},
            'by_user': {},
            'insert_count': 0,
            'update_count': 0,
            'delete_count': 0,
            'total_snapshots': 0,
            'storage_size_mb': 0
        }
        
        # 统计变更
        change_files = list(self.change_dir.glob("*.json"))
        
        for file in change_files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                record_time = datetime.fromisoformat(data.get('timestamp', ''))
                
                if record_time < start_time or record_time > end_time:
                    continue
                
                stats['total_changes'] += 1
                
                # 按类型统计
                change_type = data.get('change_type', 'unknown')
                stats['by_type'][change_type] = stats['by_type'].get(change_type, 0) + 1
                
                # 按数据类型统计
                data_type = data.get('data_type', 'unknown')
                stats['by_data_type'][data_type] = stats['by_data_type'].get(data_type, 0) + 1
                
                # 按用户统计
                user = data.get('user_id', 'system')
                stats['by_user'][user] = stats['by_user'].get(user, 0) + 1
                
                # 分类统计
                if 'insert' in change_type:
                    stats['insert_count'] += 1
                elif 'update' in change_type:
                    stats['update_count'] += 1
                elif 'delete' in change_type:
                    stats['delete_count'] += 1
                    
            except Exception as e:
                self.logger.warning(f"统计变更文件失败 {file}: {e}")
                continue
        
        # 统计快照
        snapshot_files = list(self.snapshot_dir.glob("*.json"))
        stats['total_snapshots'] = len(snapshot_files)
        
        # 计算存储大小
        total_size = 0
        for file in list(self.change_dir.glob("*.json")) + snapshot_files:
            total_size += file.stat().st_size
        
        stats['storage_size_mb'] = round(total_size / (1024 * 1024), 2)
        
        return stats
    
    def cleanup_old_snapshots(self, days: int = 30):
        """
        清理旧快照
        
        Args:
            days: 保留天数
        """
        cutoff_time = datetime.now() - timedelta(days=days)
        
        removed_count = 0
        
        for snapshot_file in self.snapshot_dir.glob("*.json"):
            try:
                # 从文件修改时间判断
                mtime = datetime.fromtimestamp(snapshot_file.stat().st_mtime)
                
                if mtime < cutoff_time:
                    snapshot_file.unlink()
                    removed_count += 1
                    
            except Exception as e:
                self.logger.warning(f"删除快照失败 {snapshot_file}: {e}")
                continue
        
        self.logger.info(f"清理旧快照完成: 删除{removed_count}个")


# 全局变更审计器实例
_change_auditor: Optional[ChangeAuditor] = None


def get_change_auditor() -> ChangeAuditor:
    """获取全局变更审计器"""
    global _change_auditor
    if _change_auditor is None:
        _change_auditor = ChangeAuditor()
    return _change_auditor
