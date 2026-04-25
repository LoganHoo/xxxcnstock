#!/usr/bin/env python3
"""
DolphinScheduler DataHub 血缘采集模块

通过查询 DS 元数据库 (MySQL) 获取工作流血缘信息
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import json

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from core.logger import setup_logger


@dataclass
class WorkflowLineage:
    """工作流血缘信息"""
    workflow_code: str
    workflow_name: str
    project_name: str
    task_name: str
    task_type: str
    upstream_tasks: List[str]
    downstream_tasks: List[str]
    create_time: str
    update_time: str


@dataclass
class TaskExecution:
    """任务执行记录"""
    task_name: str
    workflow_name: str
    start_time: str
    end_time: Optional[str]
    status: str
    duration_ms: Optional[int]


class DSDataHubLineageCollector:
    """DolphinScheduler DataHub 血缘采集器"""
    
    def __init__(self):
        """初始化采集器"""
        self.logger = setup_logger("ds_datahub_lineage")
        
        # DS 元数据库配置 (DolphinScheduler 元数据存储在远程 MySQL)
        self.db_host = os.getenv('DS_DB_HOST', '49.233.10.199')
        self.db_port = int(os.getenv('DS_DB_PORT', '3306'))
        self.db_name = os.getenv('DS_DB_NAME', 'dolphinscheduler')
        self.db_user = os.getenv('DS_DB_USER', 'nextai')
        self.db_password = os.getenv('DS_DB_PASSWORD', '100200')
        
        self.logger.info(f"DS 元数据库配置: {self.db_host}:{self.db_port}/{self.db_name}")
        
        self.engine = None
        self._init_connection()
    
    def _init_connection(self):
        """初始化数据库连接"""
        if not SQLALCHEMY_AVAILABLE:
            self.logger.warning("SQLAlchemy 未安装，使用 pymysql")
            return
        
        try:
            connection_string = (
                f"mysql+pymysql://{self.db_user}:{self.db_password}"
                f"@{self.db_host}:{self.db_port}/{self.db_name}"
            )
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            self.logger.info("✅ 数据库连接初始化成功")
        except Exception as e:
            self.logger.error(f"❌ 数据库连接初始化失败: {e}")
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        status = {
            'db_host': self.db_host,
            'db_port': self.db_port,
            'db_name': self.db_name,
            'timestamp': datetime.now().isoformat(),
            'db_connected': False
        }
        
        if not self.engine:
            status['error'] = '数据库引擎未初始化'
            return status
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                status['db_connected'] = True
                self.logger.info("✅ 数据库连接正常")
        except Exception as e:
            status['error'] = str(e)
            self.logger.error(f"❌ 数据库连接失败: {e}")
        
        return status
    
    def get_workflow_lineage(self, project_name: Optional[str] = None) -> List[WorkflowLineage]:
        """
        获取工作流血缘关系
        
        Args:
            project_name: 项目名称 (可选，用于过滤)
        
        Returns:
            工作流血缘列表
        """
        if not self.engine:
            self.logger.error("数据库引擎未初始化")
            return []
        
        try:
            query = """
                SELECT 
                    wd.code as workflow_code,
                    wd.name as workflow_name,
                    p.name as project_name,
                    td.name as task_name,
                    td.task_type,
                    td.task_params as upstream_tasks,
                    wd.create_time,
                    wd.update_time
                FROM t_ds_workflow_definition wd
                JOIN t_ds_project p ON wd.project_code = p.code
                JOIN t_ds_workflow_task_relation wtr ON wtr.workflow_definition_code = wd.code
                JOIN t_ds_task_definition td ON td.code = wtr.pre_task_code
                WHERE wd.release_state = 1
            """
            
            if project_name:
                query += f" AND p.name = '{project_name}'"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                
                lineages = []
                for row in result:
                    # 解析上游任务
                    upstream = []
                    if row.upstream_tasks:
                        try:
                            upstream = json.loads(row.upstream_tasks)
                        except:
                            upstream = str(row.upstream_tasks).split(',')
                    
                    lineage = WorkflowLineage(
                        workflow_code=str(row.workflow_code),
                        workflow_name=row.workflow_name,
                        project_name=row.project_name,
                        task_name=row.task_name,
                        task_type=row.task_type,
                        upstream_tasks=upstream,
                        downstream_tasks=[],  # 需要额外查询
                        create_time=str(row.create_time),
                        update_time=str(row.update_time)
                    )
                    lineages.append(lineage)
                
                self.logger.info(f"获取到 {len(lineages)} 条血缘记录")
                return lineages
                
        except Exception as e:
            self.logger.error(f"获取血缘关系失败: {e}")
            return []
    
    def get_task_execution_history(
        self,
        workflow_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[TaskExecution]:
        """
        获取任务执行历史
        
        Args:
            workflow_name: 工作流名称
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            limit: 返回记录数限制
        
        Returns:
            任务执行记录列表
        """
        if not self.engine:
            self.logger.error("数据库引擎未初始化")
            return []
        
        try:
            query = """
                SELECT 
                    ti.name as task_name,
                    wd.name as workflow_name,
                    ti.start_time,
                    ti.end_time,
                    ti.state as status,
                    TIMESTAMPDIFF(SECOND, ti.start_time, ti.end_time) * 1000 as duration_ms
                FROM t_ds_task_instance ti
                JOIN t_ds_workflow_instance wi ON ti.workflow_instance_id = wi.id
                JOIN t_ds_workflow_definition wd ON wi.workflow_definition_code = wd.code
                WHERE 1=1
            """
            
            if workflow_name:
                query += f" AND wd.name = '{workflow_name}'"
            
            if start_date:
                query += f" AND ti.start_time >= '{start_date} 00:00:00'"
            
            if end_date:
                query += f" AND ti.start_time <= '{end_date} 23:59:59'"
            
            query += f" ORDER BY ti.start_time DESC LIMIT {limit}"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                
                executions = []
                for row in result:
                    execution = TaskExecution(
                        task_name=row.task_name,
                        workflow_name=row.workflow_name,
                        start_time=str(row.start_time),
                        end_time=str(row.end_time) if row.end_time else None,
                        status=self._map_status(row.status),
                        duration_ms=row.duration_ms
                    )
                    executions.append(execution)
                
                self.logger.info(f"获取到 {len(executions)} 条执行记录")
                return executions
                
        except Exception as e:
            self.logger.error(f"获取执行历史失败: {e}")
            return []
    
    def _map_status(self, status_code: int) -> str:
        """映射状态码到状态名"""
        status_map = {
            0: 'SUBMITTED_SUCCESS',
            1: 'RUNNING_EXECUTION',
            2: 'READY_PAUSE',
            3: 'PAUSE',
            4: 'READY_STOP',
            5: 'STOP',
            6: 'FAILURE',
            7: 'SUCCESS',
            8: 'NEED_FAULT_TOLERANCE',
            9: 'KILL',
            10: 'WAITING_THREAD',
            11: 'WAITING_DEPEND'
        }
        return status_map.get(status_code, f'UNKNOWN({status_code})')
    
    def get_project_statistics(self) -> Dict[str, Any]:
        """获取项目统计信息"""
        if not self.engine:
            return {'error': '数据库引擎未初始化'}
        
        try:
            stats = {}
            
            with self.engine.connect() as conn:
                # 项目数量
                result = conn.execute(text("SELECT COUNT(*) FROM t_ds_project"))
                stats['project_count'] = result.scalar()
                
                # 工作流数量
                result = conn.execute(text("SELECT COUNT(*) FROM t_ds_workflow_definition WHERE release_state = 1"))
                stats['workflow_count'] = result.scalar()
                
                # 任务数量
                result = conn.execute(text("SELECT COUNT(*) FROM t_ds_task_definition"))
                stats['task_count'] = result.scalar()
                
                # 实例数量
                result = conn.execute(text("SELECT COUNT(*) FROM t_ds_workflow_instance"))
                stats['instance_count'] = result.scalar()
                
                # 成功/失败统计
                result = conn.execute(text("""
                    SELECT state, COUNT(*) as count 
                    FROM t_ds_workflow_instance 
                    GROUP BY state
                """))
                stats['instance_by_state'] = {self._map_status(row.state): row.count for row in result}
            
            self.logger.info(f"统计信息: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"获取统计信息失败: {e}")
            return {'error': str(e)}
    
    def export_lineage_to_datahub(self, project_name: Optional[str] = None) -> Dict[str, Any]:
        """
        导出血缘关系到 DataHub 格式
        
        Args:
            project_name: 项目名称
        
        Returns:
            DataHub 格式的血缘数据
        """
        lineages = self.get_workflow_lineage(project_name)
        
        # 转换为 DataHub 格式
        datahub_lineage = {
            'version': '1.0',
            'source': 'dolphinscheduler',
            'timestamp': datetime.now().isoformat(),
            'entities': [],
            'relationships': []
        }
        
        for lineage in lineages:
            # 添加工作流实体
            entity = {
                'urn': f"urn:li:workflow:{lineage.workflow_code}",
                'name': lineage.workflow_name,
                'type': 'workflow',
                'project': lineage.project_name,
                'properties': {
                    'task_name': lineage.task_name,
                    'task_type': lineage.task_type,
                    'create_time': lineage.create_time,
                    'update_time': lineage.update_time
                }
            }
            datahub_lineage['entities'].append(entity)
            
            # 添加血缘关系
            for upstream in lineage.upstream_tasks:
                relationship = {
                    'source': f"urn:li:task:{upstream}",
                    'destination': f"urn:li:task:{lineage.task_name}",
                    'type': 'upstream'
                }
                datahub_lineage['relationships'].append(relationship)
        
        self.logger.info(f"导出 {len(datahub_lineage['entities'])} 个实体, {len(datahub_lineage['relationships'])} 条关系")
        return datahub_lineage
    
    def close(self):
        """关闭连接"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("数据库连接已关闭")


def test_datahub_connection():
    """测试 DataHub 采集连接"""
    print("="*70)
    print("DolphinScheduler DataHub 血缘采集测试")
    print("="*70)
    
    collector = DSDataHubLineageCollector()
    
    # 健康检查
    status = collector.health_check()
    print(f"\n数据库状态:")
    print(f"  主机: {status['db_host']}:{status['db_port']}")
    print(f"  数据库: {status['db_name']}")
    print(f"  连接状态: {'✅ 成功' if status['db_connected'] else '❌ 失败'}")
    
    if status['db_connected']:
        # 获取统计信息
        stats = collector.get_project_statistics()
        print(f"\n统计信息:")
        print(f"  项目数: {stats.get('project_count', 0)}")
        print(f"  工作流数: {stats.get('workflow_count', 0)}")
        print(f"  任务数: {stats.get('task_count', 0)}")
        print(f"  实例数: {stats.get('instance_count', 0)}")
        
        # 获取血缘关系
        lineages = collector.get_workflow_lineage()
        print(f"\n血缘关系: {len(lineages)} 条")
        
        # 获取执行历史
        executions = collector.get_task_execution_history(limit=5)
        print(f"\n最近执行记录: {len(executions)} 条")
        for exec in executions[:3]:
            print(f"  - {exec.task_name}: {exec.status}")
    
    collector.close()
    return status['db_connected']


if __name__ == "__main__":
    if not SQLALCHEMY_AVAILABLE:
        print("❌ 请先安装 SQLAlchemy: pip install sqlalchemy pymysql")
        sys.exit(1)
    
    test_datahub_connection()
