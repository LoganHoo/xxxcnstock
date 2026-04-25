#!/usr/bin/env python3
"""
操作审计模块

记录和审计所有数据操作:
- 用户操作追踪
- 系统操作记录
- 操作权限检查
- 异常操作检测
"""
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from functools import wraps
import threading

from core.logger import setup_logger
from core.paths import get_data_path
from .audit_logger import AuditLogger, AuditType, AuditLevel, get_audit_logger


class OperationType(Enum):
    """操作类型"""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"
    SEARCH = "search"
    EXPORT = "export"
    IMPORT = "import"
    EXECUTE = "execute"


class OperationStatus(Enum):
    """操作状态"""
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class OperationRecord:
    """操作记录"""
    operation_id: str
    operation_type: str
    operation_name: str
    user_id: Optional[str]
    resource_type: str
    resource_id: str
    parameters: Optional[Dict]
    status: str
    start_time: str
    end_time: Optional[str]
    duration_ms: Optional[int]
    result_summary: Optional[str]
    error_details: Optional[str]
    client_info: Optional[Dict]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OperationAuditor:
    """操作审计器"""
    
    def __init__(self, storage_dir: Optional[Path] = None):
        """
        初始化操作审计器
        
        Args:
            storage_dir: 存储目录
        """
        self.storage_dir = storage_dir or get_data_path() / "operation_audit"
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = setup_logger("operation_audit")
        self.audit_logger = get_audit_logger()
        
        # 活跃操作记录
        self._active_operations: Dict[str, OperationRecord] = {}
        self._lock = threading.Lock()
        
        # 异常检测规则
        self._anomaly_rules: List[Callable] = []
        self._setup_default_rules()
    
    def _generate_id(self, prefix: str = "OP") -> str:
        """生成唯一ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        random_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:6]
        return f"{prefix}{timestamp}{random_suffix}"
    
    def _setup_default_rules(self):
        """设置默认异常检测规则"""
        # 规则1: 短时间内大量操作
        self._anomaly_rules.append(self._check_high_frequency)
        
        # 规则2: 非工作时间操作
        self._anomaly_rules.append(self._check_off_hours)
        
        # 规则3: 敏感数据访问
        self._anomaly_rules.append(self._check_sensitive_access)
    
    def start_operation(self,
                       operation_type: OperationType,
                       operation_name: str,
                       resource_type: str,
                       resource_id: str,
                       user_id: Optional[str] = None,
                       parameters: Optional[Dict] = None,
                       client_info: Optional[Dict] = None) -> str:
        """
        开始记录操作
        
        Args:
            operation_type: 操作类型
            operation_name: 操作名称
            resource_type: 资源类型
            resource_id: 资源ID
            user_id: 用户ID
            parameters: 操作参数
            client_info: 客户端信息
        
        Returns:
            操作ID
        """
        operation_id = self._generate_id()
        
        record = OperationRecord(
            operation_id=operation_id,
            operation_type=operation_type.value,
            operation_name=operation_name,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            parameters=parameters,
            status=OperationStatus.SUCCESS.value,
            start_time=datetime.now().isoformat(),
            end_time=None,
            duration_ms=None,
            result_summary=None,
            error_details=None,
            client_info=client_info
        )
        
        with self._lock:
            self._active_operations[operation_id] = record
        
        # 记录审计日志
        self.audit_logger.log(
            audit_type=AuditType.USER_ACTION,
            audit_level=AuditLevel.INFO,
            resource_type=resource_type,
            resource_id=resource_id,
            action=f"{operation_type.value}_{operation_name}",
            user_id=user_id,
            request_data={
                'operation_id': operation_id,
                'parameters': parameters
            },
            metadata={'client_info': client_info}
        )
        
        self.logger.debug(f"操作开始: {operation_name} ({operation_id})")
        
        return operation_id
    
    def end_operation(self,
                     operation_id: str,
                     status: OperationStatus = OperationStatus.SUCCESS,
                     result_summary: Optional[str] = None,
                     error_details: Optional[str] = None):
        """
        结束记录操作
        
        Args:
            operation_id: 操作ID
            status: 操作状态
            result_summary: 结果摘要
            error_details: 错误详情
        """
        with self._lock:
            record = self._active_operations.pop(operation_id, None)
        
        if record is None:
            self.logger.warning(f"未找到操作记录: {operation_id}")
            return
        
        # 计算耗时
        end_time = datetime.now()
        start_time = datetime.fromisoformat(record.start_time)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        record.end_time = end_time.isoformat()
        record.duration_ms = duration_ms
        record.status = status.value
        record.result_summary = result_summary
        record.error_details = error_details
        
        # 保存操作记录
        self._save_operation_record(record)
        
        # 记录审计日志
        audit_level = AuditLevel.INFO if status == OperationStatus.SUCCESS else AuditLevel.WARNING
        if status == OperationStatus.FAILURE:
            audit_level = AuditLevel.ERROR
        
        self.audit_logger.log(
            audit_type=AuditType.USER_ACTION,
            audit_level=audit_level,
            resource_type=record.resource_type,
            resource_id=record.resource_id,
            action=f"{record.operation_type}_{record.operation_name}",
            status=status.value,
            user_id=record.user_id,
            duration_ms=duration_ms,
            error_message=error_details,
            metadata={
                'operation_id': operation_id,
                'result_summary': result_summary
            }
        )
        
        # 异常检测
        self._detect_anomalies(record)
        
        self.logger.debug(f"操作结束: {record.operation_name} ({operation_id}) - {status.value}")
    
    def _save_operation_record(self, record: OperationRecord):
        """保存操作记录"""
        date_str = datetime.now().strftime('%Y%m%d')
        record_file = self.storage_dir / f"operations_{date_str}.jsonl"
        
        with open(record_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record.to_dict(), ensure_ascii=False) + '\n')
    
    def _detect_anomalies(self, record: OperationRecord):
        """检测异常操作"""
        for rule in self._anomaly_rules:
            try:
                is_anomaly, reason = rule(record)
                if is_anomaly:
                    self._handle_anomaly(record, reason)
            except Exception as e:
                self.logger.error(f"异常检测失败: {e}")
    
    def _check_high_frequency(self, record: OperationRecord) -> tuple:
        """检查高频操作"""
        # 获取最近1分钟内的操作数
        recent_count = self._get_recent_operation_count(
            record.user_id,
            timedelta(minutes=1)
        )
        
        if recent_count > 100:  # 每分钟超过100次操作
            return True, f"高频操作: 最近1分钟{recent_count}次操作"
        
        return False, None
    
    def _check_off_hours(self, record: OperationRecord) -> tuple:
        """检查非工作时间操作"""
        operation_time = datetime.fromisoformat(record.start_time)
        hour = operation_time.hour
        
        # 假设工作时间为 9:00-18:00
        if hour < 9 or hour >= 18:
            # 检查是否为敏感操作
            if record.operation_type in [OperationType.DELETE.value, OperationType.EXPORT.value]:
                return True, f"非工作时间敏感操作: {record.operation_name}"
        
        return False, None
    
    def _check_sensitive_access(self, record: OperationRecord) -> tuple:
        """检查敏感数据访问"""
        sensitive_types = ['financial_raw', 'user_confidential']
        
        if record.resource_type in sensitive_types:
            # 检查是否有权限
            # 这里简化处理，实际应该查询权限系统
            if record.operation_type == OperationType.EXPORT.value:
                return True, f"敏感数据导出: {record.resource_type}"
        
        return False, None
    
    def _get_recent_operation_count(self, user_id: Optional[str], window: timedelta) -> int:
        """获取最近操作数"""
        count = 0
        cutoff_time = datetime.now() - window
        
        # 检查活跃操作
        with self._lock:
            for record in self._active_operations.values():
                if record.user_id == user_id:
                    start_time = datetime.fromisoformat(record.start_time)
                    if start_time > cutoff_time:
                        count += 1
        
        return count
    
    def _handle_anomaly(self, record: OperationRecord, reason: str):
        """处理异常操作"""
        self.logger.warning(f"检测到异常操作: {reason} - {record.operation_id}")
        
        # 记录安全事件
        self.audit_logger.log(
            audit_type=AuditType.SECURITY_EVENT,
            audit_level=AuditLevel.WARNING,
            resource_type=record.resource_type,
            resource_id=record.resource_id,
            action="anomaly_detected",
            user_id=record.user_id,
            error_message=reason,
            metadata={
                'operation_id': record.operation_id,
                'operation_type': record.operation_type,
                'anomaly_type': 'behavioral'
            }
        )
        
        # 保存异常记录
        anomaly_file = self.storage_dir / "anomalies.jsonl"
        anomaly_record = {
            'timestamp': datetime.now().isoformat(),
            'operation_id': record.operation_id,
            'user_id': record.user_id,
            'reason': reason,
            'operation': record.to_dict()
        }
        
        with open(anomaly_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(anomaly_record, ensure_ascii=False) + '\n')
    
    def query_operations(self,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        user_id: Optional[str] = None,
                        operation_type: Optional[OperationType] = None,
                        resource_type: Optional[str] = None,
                        status: Optional[OperationStatus] = None,
                        limit: int = 100) -> List[OperationRecord]:
        """
        查询操作记录
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            user_id: 用户ID
            operation_type: 操作类型
            resource_type: 资源类型
            status: 状态
            limit: 限制数量
        
        Returns:
            操作记录列表
        """
        records = []
        
        # 确定要查询的文件
        operation_files = sorted(self.storage_dir.glob("operations_*.jsonl"), reverse=True)
        
        for file in operation_files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            data = json.loads(line)
                            
                            # 应用过滤条件
                            if start_time:
                                record_time = datetime.fromisoformat(data.get('start_time', ''))
                                if record_time < start_time:
                                    continue
                            
                            if end_time:
                                record_time = datetime.fromisoformat(data.get('start_time', ''))
                                if record_time > end_time:
                                    continue
                            
                            if user_id and data.get('user_id') != user_id:
                                continue
                            
                            if operation_type and data.get('operation_type') != operation_type.value:
                                continue
                            
                            if resource_type and data.get('resource_type') != resource_type:
                                continue
                            
                            if status and data.get('status') != status.value:
                                continue
                            
                            records.append(OperationRecord(**data))
                            
                            if len(records) >= limit:
                                return records
                                
                        except (json.JSONDecodeError, TypeError):
                            continue
                            
            except Exception as e:
                self.logger.error(f"读取操作记录失败 {file}: {e}")
                continue
        
        return records
    
    def get_operation_statistics(self, days: int = 7) -> Dict[str, Any]:
        """
        获取操作统计信息
        
        Args:
            days: 统计天数
        
        Returns:
            统计信息
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        stats = {
            'total_operations': 0,
            'by_type': {},
            'by_status': {},
            'by_user': {},
            'by_resource_type': {},
            'average_duration_ms': 0,
            'error_count': 0,
            'anomaly_count': 0
        }
        
        total_duration = 0
        duration_count = 0
        
        operation_files = sorted(self.storage_dir.glob("operations_*.jsonl"), reverse=True)
        
        for file in operation_files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            data = json.loads(line)
                            record_time = datetime.fromisoformat(data.get('start_time', ''))
                            
                            if record_time < start_time or record_time > end_time:
                                continue
                            
                            stats['total_operations'] += 1
                            
                            # 按类型统计
                            op_type = data.get('operation_type', 'unknown')
                            stats['by_type'][op_type] = stats['by_type'].get(op_type, 0) + 1
                            
                            # 按状态统计
                            op_status = data.get('status', 'unknown')
                            stats['by_status'][op_status] = stats['by_status'].get(op_status, 0) + 1
                            
                            # 按用户统计
                            user = data.get('user_id', 'anonymous')
                            stats['by_user'][user] = stats['by_user'].get(user, 0) + 1
                            
                            # 按资源类型统计
                            res_type = data.get('resource_type', 'unknown')
                            stats['by_resource_type'][res_type] = stats['by_resource_type'].get(res_type, 0) + 1
                            
                            # 耗时统计
                            duration = data.get('duration_ms')
                            if duration is not None:
                                total_duration += duration
                                duration_count += 1
                            
                            # 错误统计
                            if op_status == OperationStatus.FAILURE.value:
                                stats['error_count'] += 1
                                
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
                            
            except Exception as e:
                self.logger.error(f"统计操作记录失败 {file}: {e}")
                continue
        
        # 计算平均耗时
        if duration_count > 0:
            stats['average_duration_ms'] = total_duration / duration_count
        
        # 统计异常数
        try:
            anomaly_file = self.storage_dir / "anomalies.jsonl"
            if anomaly_file.exists():
                with open(anomaly_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            record_time = datetime.fromisoformat(data.get('timestamp', ''))
                            if start_time <= record_time <= end_time:
                                stats['anomaly_count'] += 1
                        except:
                            continue
        except Exception as e:
            self.logger.error(f"统计异常记录失败: {e}")
        
        return stats


def audit_operation(operation_type: OperationType,
                   resource_type: str,
                   operation_name: Optional[str] = None):
    """
    操作审计装饰器
    
    Args:
        operation_type: 操作类型
        resource_type: 资源类型
        operation_name: 操作名称(默认为函数名)
    
    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            auditor = get_operation_auditor()
            
            # 确定资源ID
            resource_id = kwargs.get('code', kwargs.get('resource_id', 'unknown'))
            
            # 确定用户ID
            user_id = kwargs.get('user_id', None)
            
            # 开始操作记录
            op_id = auditor.start_operation(
                operation_type=operation_type,
                operation_name=operation_name or func.__name__,
                resource_type=resource_type,
                resource_id=str(resource_id),
                user_id=user_id,
                parameters={'args': str(args), 'kwargs': {k: str(v) for k, v in kwargs.items()}}
            )
            
            try:
                # 执行函数
                result = func(*args, **kwargs)
                
                # 记录成功
                auditor.end_operation(
                    operation_id=op_id,
                    status=OperationStatus.SUCCESS,
                    result_summary=str(result)[:100] if result else None
                )
                
                return result
                
            except Exception as e:
                # 记录失败
                auditor.end_operation(
                    operation_id=op_id,
                    status=OperationStatus.FAILURE,
                    error_details=str(e)
                )
                raise
        
        return wrapper
    return decorator


# 全局操作审计器实例
_operation_auditor: Optional[OperationAuditor] = None


def get_operation_auditor() -> OperationAuditor:
    """获取全局操作审计器"""
    global _operation_auditor
    if _operation_auditor is None:
        _operation_auditor = OperationAuditor()
    return _operation_auditor
