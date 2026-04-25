#!/usr/bin/env python3
"""
工作流状态管理 - SQLite存储

提供工作流执行历史的持久化存储
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import sqlite3
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from contextlib import contextmanager

from core.logger import setup_logger
from core.paths import get_data_path


class WorkflowExecutionStatus(Enum):
    """工作流执行状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"


@dataclass
class WorkflowExecution:
    """工作流执行记录"""
    id: Optional[int] = None
    workflow_name: str = ""
    execution_id: str = ""
    status: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: int = 0
    params: str = ""  # JSON
    result: str = ""  # JSON
    error_message: str = ""
    retry_count: int = 0
    created_at: Optional[str] = None


@dataclass
class WorkflowCheckpoint:
    """工作流检查点记录"""
    id: Optional[int] = None
    execution_id: str = ""
    checkpoint_name: str = ""
    status: str = ""  # PASS, FAIL
    details: str = ""  # JSON
    created_at: Optional[str] = None


class WorkflowStateDB:
    """工作流状态数据库管理器"""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        初始化状态数据库
        
        Args:
            db_path: 数据库文件路径，默认使用 data/workflow_state.db
        """
        if db_path is None:
            db_path = get_data_path() / "workflow_state.db"
        
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.logger = setup_logger("workflow_state_db")
        
        # 初始化数据库
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接上下文"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 工作流执行记录表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workflow_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_name TEXT NOT NULL,
                    execution_id TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    duration_seconds INTEGER DEFAULT 0,
                    params TEXT,
                    result TEXT,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 工作流检查点表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workflow_checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    checkpoint_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
                )
            """)
            
            # 工作流依赖检查表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workflow_dependencies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_name TEXT NOT NULL,
                    dependency_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_name 
                ON workflow_executions(workflow_name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_status 
                ON workflow_executions(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_checkpoints_execution 
                ON workflow_checkpoints(execution_id)
            """)
            
            conn.commit()
            self.logger.info(f"数据库初始化完成: {self.db_path}")
    
    def create_execution(self, workflow_name: str, execution_id: str, 
                         params: Optional[Dict] = None) -> int:
        """
        创建工作流执行记录
        
        Args:
            workflow_name: 工作流名称
            execution_id: 执行ID
            params: 执行参数
            
        Returns:
            记录ID
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO workflow_executions 
                (workflow_name, execution_id, status, started_at, params)
                VALUES (?, ?, ?, ?, ?)
            """, (
                workflow_name,
                execution_id,
                WorkflowExecutionStatus.PENDING.value,
                datetime.now().isoformat(),
                json.dumps(params) if params else "{}"
            ))
            conn.commit()
            return cursor.lastrowid
    
    def update_execution_status(self, execution_id: str, status: str,
                                result: Optional[Dict] = None,
                                error_message: str = "",
                                duration_seconds: int = 0):
        """
        更新工作流执行状态
        
        Args:
            execution_id: 执行ID
            status: 新状态
            result: 执行结果
            error_message: 错误信息
            duration_seconds: 执行时长
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            completed_at = datetime.now().isoformat() if status in ['success', 'failed', 'cancelled'] else None
            
            cursor.execute("""
                UPDATE workflow_executions 
                SET status = ?, result = ?, error_message = ?, 
                    duration_seconds = ?, completed_at = ?
                WHERE execution_id = ?
            """, (
                status,
                json.dumps(result) if result else "{}",
                error_message,
                duration_seconds,
                completed_at,
                execution_id
            ))
            conn.commit()
            
            self.logger.debug(f"执行状态更新: {execution_id} -> {status}")

    def update_workflow_status(self, execution_id: str, status: str,
                               result: Optional[Dict] = None,
                               error_message: str = "",
                               duration_seconds: int = 0):
        """
        更新工作流状态（update_execution_status 的别名）

        Args:
            execution_id: 执行ID
            status: 新状态
            result: 执行结果
            error_message: 错误信息
            duration_seconds: 执行时长
        """
        return self.update_execution_status(
            execution_id=execution_id,
            status=status,
            result=result,
            error_message=error_message,
            duration_seconds=duration_seconds
        )
    
    def increment_retry_count(self, execution_id: str) -> int:
        """
        增加重试次数
        
        Args:
            execution_id: 执行ID
            
        Returns:
            当前重试次数
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE workflow_executions 
                SET retry_count = retry_count + 1
                WHERE execution_id = ?
            """, (execution_id,))
            conn.commit()
            
            # 获取更新后的值
            cursor.execute("""
                SELECT retry_count FROM workflow_executions WHERE execution_id = ?
            """, (execution_id,))
            row = cursor.fetchone()
            return row['retry_count'] if row else 0
    
    def get_workflow_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """
        获取工作流执行状态
        
        Args:
            execution_id: 执行ID
            
        Returns:
            执行状态字典
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM workflow_executions WHERE execution_id = ?
            """, (execution_id,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row['id'],
                    'workflow_name': row['workflow_name'],
                    'execution_id': row['execution_id'],
                    'status': row['status'],
                    'started_at': row['started_at'],
                    'completed_at': row['completed_at'],
                    'duration_seconds': row['duration_seconds'],
                    'params': json.loads(row['params']) if row['params'] else {},
                    'result': json.loads(row['result']) if row['result'] else {},
                    'error_message': row['error_message'],
                    'retry_count': row['retry_count'],
                    'created_at': row['created_at']
                }
            return None
    
    def get_latest_execution(self, workflow_name: str) -> Optional[Dict[str, Any]]:
        """
        获取最新执行记录
        
        Args:
            workflow_name: 工作流名称
            
        Returns:
            最新执行记录
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM workflow_executions 
                WHERE workflow_name = ?
                ORDER BY id DESC
                LIMIT 1
            """, (workflow_name,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row['id'],
                    'workflow_name': row['workflow_name'],
                    'execution_id': row['execution_id'],
                    'status': row['status'],
                    'started_at': row['started_at'],
                    'completed_at': row['completed_at'],
                    'duration_seconds': row['duration_seconds'],
                    'params': json.loads(row['params']) if row['params'] else {},
                    'result': json.loads(row['result']) if row['result'] else {},
                    'error_message': row['error_message'],
                    'retry_count': row['retry_count'],
                    'created_at': row['created_at']
                }
            return None
    
    def list_executions(self, workflow_name: Optional[str] = None,
                       status: Optional[str] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """
        列出工作流执行记录
        
        Args:
            workflow_name: 工作流名称过滤
            status: 状态过滤
            limit: 返回数量限制
            
        Returns:
            执行记录列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM workflow_executions WHERE 1=1"
            params = []
            
            if workflow_name:
                query += " AND workflow_name = ?"
                params.append(workflow_name)
            
            if status:
                query += " AND status = ?"
                params.append(status)
            
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [{
                'id': row['id'],
                'workflow_name': row['workflow_name'],
                'execution_id': row['execution_id'],
                'status': row['status'],
                'started_at': row['started_at'],
                'completed_at': row['completed_at'],
                'duration_seconds': row['duration_seconds'],
                'retry_count': row['retry_count'],
                'created_at': row['created_at']
            } for row in rows]
    
    def add_checkpoint(self, execution_id: str, checkpoint_name: str,
                       status: str, details: Optional[Dict] = None):
        """
        添加检查点记录
        
        Args:
            execution_id: 执行ID
            checkpoint_name: 检查点名称
            status: 检查点状态 (PASS/FAIL)
            details: 详细信息
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO workflow_checkpoints 
                (execution_id, checkpoint_name, status, details)
                VALUES (?, ?, ?, ?)
            """, (
                execution_id,
                checkpoint_name,
                status,
                json.dumps(details) if details else "{}"
            ))
            conn.commit()
            
            self.logger.debug(f"检查点记录: {execution_id} - {checkpoint_name} - {status}")
    
    def get_checkpoints(self, execution_id: str) -> List[Dict[str, Any]]:
        """
        获取执行的所有检查点
        
        Args:
            execution_id: 执行ID
            
        Returns:
            检查点列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM workflow_checkpoints 
                WHERE execution_id = ?
                ORDER BY created_at ASC
            """, (execution_id,))
            rows = cursor.fetchall()
            
            return [{
                'id': row['id'],
                'execution_id': row['execution_id'],
                'checkpoint_name': row['checkpoint_name'],
                'status': row['status'],
                'details': json.loads(row['details']) if row['details'] else {},
                'created_at': row['created_at']
            } for row in rows]
    
    def add_dependency_check(self, workflow_name: str, dependency_name: str,
                            status: str, message: str = ""):
        """
        添加依赖检查记录
        
        Args:
            workflow_name: 工作流名称
            dependency_name: 依赖名称
            status: 依赖状态
            message: 状态信息
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO workflow_dependencies 
                (workflow_name, dependency_name, status, message)
                VALUES (?, ?, ?, ?)
            """, (workflow_name, dependency_name, status, message))
            conn.commit()
    
    def get_dependency_status(self, workflow_name: str) -> List[Dict[str, Any]]:
        """
        获取工作流的依赖状态
        
        Args:
            workflow_name: 工作流名称
            
        Returns:
            依赖状态列表
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM workflow_dependencies 
                WHERE workflow_name = ?
                ORDER BY checked_at DESC
            """, (workflow_name,))
            rows = cursor.fetchall()
            
            return [{
                'id': row['id'],
                'workflow_name': row['workflow_name'],
                'dependency_name': row['dependency_name'],
                'status': row['status'],
                'message': row['message'],
                'checked_at': row['checked_at']
            } for row in rows]
    
    def get_statistics(self, workflow_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取工作流统计信息
        
        Args:
            workflow_name: 工作流名称过滤
            
        Returns:
            统计信息字典
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 基础查询条件
            where_clause = "WHERE workflow_name = ?" if workflow_name else ""
            params = [workflow_name] if workflow_name else []
            
            # 总执行次数
            cursor.execute(f"""
                SELECT COUNT(*) as total FROM workflow_executions {where_clause}
            """, params)
            total = cursor.fetchone()['total']
            
            # 各状态统计
            cursor.execute(f"""
                SELECT status, COUNT(*) as count 
                FROM workflow_executions 
                {where_clause}
                GROUP BY status
            """, params)
            status_counts = {row['status']: row['count'] for row in cursor.fetchall()}
            
            # 平均执行时长
            cursor.execute(f"""
                SELECT AVG(duration_seconds) as avg_duration 
                FROM workflow_executions 
                {where_clause}
                AND status = 'success'
            """, params)
            avg_duration = cursor.fetchone()['avg_duration'] or 0
            
            # 今日执行次数
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute(f"""
                SELECT COUNT(*) as today_count 
                FROM workflow_executions 
                {where_clause}
                AND DATE(created_at) = DATE(?)
            """, params + [today])
            today_count = cursor.fetchone()['today_count']
            
            return {
                'total_executions': total,
                'status_counts': status_counts,
                'success_rate': status_counts.get('success', 0) / total * 100 if total > 0 else 0,
                'avg_duration_seconds': avg_duration,
                'today_executions': today_count
            }
    
    def cleanup_old_records(self, days: int = 30):
        """
        清理旧记录
        
        Args:
            days: 保留天数
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 先删除关联的检查点
            cursor.execute("""
                DELETE FROM workflow_checkpoints 
                WHERE execution_id IN (
                    SELECT execution_id FROM workflow_executions 
                    WHERE datetime(created_at) < datetime('now', '-{} days')
                )
            """.format(days))
            
            # 再删除执行记录
            cursor.execute("""
                DELETE FROM workflow_executions 
                WHERE datetime(created_at) < datetime('now', '-{} days')
            """.format(days))
            
            deleted = cursor.rowcount
            conn.commit()
            self.logger.info(f"已清理 {days} 天前的记录，删除 {deleted} 条")


# 全局实例
_workflow_state_db: Optional[WorkflowStateDB] = None


def get_workflow_state_db() -> WorkflowStateDB:
    """获取全局工作流状态数据库实例"""
    global _workflow_state_db
    if _workflow_state_db is None:
        _workflow_state_db = WorkflowStateDB()
    return _workflow_state_db


if __name__ == "__main__":
    # 测试代码
    db = WorkflowStateDB()
    
    # 创建执行记录
    exec_id = db.create_execution("test_workflow", "test_001", {"param1": "value1"})
    print(f"创建执行记录: ID={exec_id}")
    
    # 更新状态
    db.update_execution_status("test_001", "success", {"output": "result"}, duration_seconds=120)
    print("状态更新完成")
    
    # 获取状态
    status = db.get_workflow_status("test_001")
    print(f"执行状态: {status}")
    
    # 获取统计
    stats = db.get_statistics()
    print(f"统计信息: {stats}")
