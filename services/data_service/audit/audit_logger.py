#!/usr/bin/env python3
"""
审计日志模块

提供统一的审计日志记录功能:
- 结构化日志记录
- 多级别日志支持
- 日志轮转和归档
- 日志查询接口
"""
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass, asdict

from core.logger import setup_logger
from core.paths import get_data_path


class AuditLevel(Enum):
    """审计级别"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditType(Enum):
    """审计类型"""
    DATA_ACCESS = "data_access"           # 数据访问
    DATA_MODIFY = "data_modify"           # 数据修改
    DATA_DELETE = "data_delete"           # 数据删除
    DATA_EXPORT = "data_export"           # 数据导出
    DATA_IMPORT = "data_import"           # 数据导入
    USER_LOGIN = "user_login"             # 用户登录
    USER_LOGOUT = "user_logout"           # 用户登出
    USER_ACTION = "user_action"           # 用户操作
    SYSTEM_EVENT = "system_event"         # 系统事件
    SECURITY_EVENT = "security_event"     # 安全事件


@dataclass
class AuditRecord:
    """审计记录"""
    timestamp: str
    audit_id: str
    audit_type: str
    audit_level: str
    user_id: Optional[str]
    session_id: Optional[str]
    source_ip: Optional[str]
    resource_type: str
    resource_id: str
    action: str
    status: str
    request_data: Optional[Dict]
    response_data: Optional[Dict]
    error_message: Optional[str]
    duration_ms: Optional[int]
    metadata: Optional[Dict]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)
    
    def compute_hash(self) -> str:
        """计算记录哈希值(用于完整性验证)"""
        data = self.to_json()
        return hashlib.sha256(data.encode()).hexdigest()


class AuditLogger:
    """审计日志记录器"""
    
    def __init__(self, 
                 log_dir: Optional[Path] = None,
                 enable_file: bool = True,
                 enable_console: bool = False):
        """
        初始化审计日志记录器
        
        Args:
            log_dir: 日志目录
            enable_file: 是否启用文件日志
            enable_console: 是否启用控制台日志
        """
        self.log_dir = log_dir or get_data_path() / "audit"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建日志记录器
        self.logger = logging.getLogger("data_audit")
        self.logger.setLevel(logging.INFO)
        
        # 清除现有处理器
        self.logger.handlers.clear()
        
        # 文件处理器
        if enable_file:
            log_file = self.log_dir / f"audit_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # 控制台处理器
        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            self.logger.addHandler(console_handler)
        
        # 系统日志
        self.system_logger = setup_logger("audit_logger")
    
    def _generate_audit_id(self) -> str:
        """生成审计ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        random_suffix = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
        return f"AUD{timestamp}{random_suffix}"
    
    def log(self,
            audit_type: AuditType,
            audit_level: AuditLevel,
            resource_type: str,
            resource_id: str,
            action: str,
            status: str = "success",
            user_id: Optional[str] = None,
            session_id: Optional[str] = None,
            source_ip: Optional[str] = None,
            request_data: Optional[Dict] = None,
            response_data: Optional[Dict] = None,
            error_message: Optional[str] = None,
            duration_ms: Optional[int] = None,
            metadata: Optional[Dict] = None) -> str:
        """
        记录审计日志
        
        Args:
            audit_type: 审计类型
            audit_level: 审计级别
            resource_type: 资源类型
            resource_id: 资源ID
            action: 操作动作
            status: 操作状态
            user_id: 用户ID
            session_id: 会话ID
            source_ip: 来源IP
            request_data: 请求数据
            response_data: 响应数据
            error_message: 错误信息
            duration_ms: 耗时(毫秒)
            metadata: 元数据
        
        Returns:
            审计记录ID
        """
        # 创建审计记录
        record = AuditRecord(
            timestamp=datetime.now().isoformat(),
            audit_id=self._generate_audit_id(),
            audit_type=audit_type.value,
            audit_level=audit_level.value,
            user_id=user_id,
            session_id=session_id,
            source_ip=source_ip,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            status=status,
            request_data=request_data,
            response_data=response_data,
            error_message=error_message,
            duration_ms=duration_ms,
            metadata=metadata
        )
        
        # 记录日志
        log_entry = record.to_json()
        
        if audit_level == AuditLevel.ERROR or audit_level == AuditLevel.CRITICAL:
            self.logger.error(log_entry)
        elif audit_level == AuditLevel.WARNING:
            self.logger.warning(log_entry)
        else:
            self.logger.info(log_entry)
        
        return record.audit_id
    
    def log_data_access(self,
                        resource_type: str,
                        resource_id: str,
                        action: str = "read",
                        user_id: Optional[str] = None,
                        **kwargs) -> str:
        """记录数据访问"""
        return self.log(
            audit_type=AuditType.DATA_ACCESS,
            audit_level=AuditLevel.INFO,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            user_id=user_id,
            **kwargs
        )
    
    def log_data_modify(self,
                        resource_type: str,
                        resource_id: str,
                        action: str = "update",
                        user_id: Optional[str] = None,
                        **kwargs) -> str:
        """记录数据修改"""
        return self.log(
            audit_type=AuditType.DATA_MODIFY,
            audit_level=AuditLevel.INFO,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            user_id=user_id,
            **kwargs
        )
    
    def log_data_delete(self,
                        resource_type: str,
                        resource_id: str,
                        user_id: Optional[str] = None,
                        **kwargs) -> str:
        """记录数据删除"""
        return self.log(
            audit_type=AuditType.DATA_DELETE,
            audit_level=AuditLevel.WARNING,
            resource_type=resource_type,
            resource_id=resource_id,
            action="delete",
            user_id=user_id,
            **kwargs
        )
    
    def log_data_export(self,
                        resource_type: str,
                        resource_id: str,
                        user_id: Optional[str] = None,
                        **kwargs) -> str:
        """记录数据导出"""
        return self.log(
            audit_type=AuditType.DATA_EXPORT,
            audit_level=AuditLevel.INFO,
            resource_type=resource_type,
            resource_id=resource_id,
            action="export",
            user_id=user_id,
            **kwargs
        )
    
    def log_error(self,
                  resource_type: str,
                  resource_id: str,
                  action: str,
                  error_message: str,
                  user_id: Optional[str] = None,
                  **kwargs) -> str:
        """记录错误"""
        return self.log(
            audit_type=AuditType.SYSTEM_EVENT,
            audit_level=AuditLevel.ERROR,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            status="error",
            error_message=error_message,
            user_id=user_id,
            **kwargs
        )
    
    def query_logs(self,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   audit_type: Optional[AuditType] = None,
                   user_id: Optional[str] = None,
                   resource_type: Optional[str] = None,
                   resource_id: Optional[str] = None,
                   limit: int = 100) -> List[AuditRecord]:
        """
        查询审计日志
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            audit_type: 审计类型
            user_id: 用户ID
            resource_type: 资源类型
            resource_id: 资源ID
            limit: 返回记录数限制
        
        Returns:
            审计记录列表
        """
        records = []
        
        # 确定要查询的日志文件
        log_files = sorted(self.log_dir.glob("audit_*.log"), reverse=True)
        
        for log_file in log_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        # 解析日志行 (格式: timestamp | json_data)
                        parts = line.split(' | ', 1)
                        if len(parts) != 2:
                            continue
                        
                        try:
                            data = json.loads(parts[1])
                            record = AuditRecord(**data)
                            
                            # 应用过滤条件
                            if start_time and datetime.fromisoformat(record.timestamp) < start_time:
                                continue
                            if end_time and datetime.fromisoformat(record.timestamp) > end_time:
                                continue
                            if audit_type and record.audit_type != audit_type.value:
                                continue
                            if user_id and record.user_id != user_id:
                                continue
                            if resource_type and record.resource_type != resource_type:
                                continue
                            if resource_id and record.resource_id != resource_id:
                                continue
                            
                            records.append(record)
                            
                            if len(records) >= limit:
                                return records
                                
                        except (json.JSONDecodeError, TypeError):
                            continue
                            
            except Exception as e:
                self.system_logger.error(f"读取日志文件失败 {log_file}: {e}")
                continue
        
        return records
    
    def get_log_stats(self, days: int = 7) -> Dict[str, Any]:
        """
        获取日志统计信息
        
        Args:
            days: 统计天数
        
        Returns:
            统计信息字典
        """
        from datetime import timedelta
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        stats = {
            'total_records': 0,
            'by_type': {},
            'by_level': {},
            'by_status': {},
            'by_resource_type': {},
            'error_count': 0
        }
        
        log_files = sorted(self.log_dir.glob("audit_*.log"), reverse=True)
        
        for log_file in log_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        parts = line.split(' | ', 1)
                        if len(parts) != 2:
                            continue
                        
                        try:
                            data = json.loads(parts[1])
                            record_time = datetime.fromisoformat(data.get('timestamp', ''))
                            
                            if record_time < start_time or record_time > end_time:
                                continue
                            
                            stats['total_records'] += 1
                            
                            # 按类型统计
                            audit_type = data.get('audit_type', 'unknown')
                            stats['by_type'][audit_type] = stats['by_type'].get(audit_type, 0) + 1
                            
                            # 按级别统计
                            audit_level = data.get('audit_level', 'unknown')
                            stats['by_level'][audit_level] = stats['by_level'].get(audit_level, 0) + 1
                            
                            # 按状态统计
                            status = data.get('status', 'unknown')
                            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1
                            
                            # 按资源类型统计
                            resource_type = data.get('resource_type', 'unknown')
                            stats['by_resource_type'][resource_type] = stats['by_resource_type'].get(resource_type, 0) + 1
                            
                            # 错误统计
                            if status == 'error' or audit_level in ['error', 'critical']:
                                stats['error_count'] += 1
                                
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
                            
            except Exception as e:
                self.system_logger.error(f"统计日志文件失败 {log_file}: {e}")
                continue
        
        return stats


# 全局审计日志记录器实例
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """获取全局审计日志记录器"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger
