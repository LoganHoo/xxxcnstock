#!/usr/bin/env python3
"""
DolphinScheduler Gateway 客户端 (3.1.1)

通过 Java Gateway (25333) 提交工作流定义
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

from pydolphinscheduler.core.process_definition import ProcessDefinition
from pydolphinscheduler.core.task import Task
from pydolphinscheduler.tasks.shell import Shell
from pydolphinscheduler.tasks.python import Python
from pydolphinscheduler.tasks.sql import Sql
from pydolphinscheduler.tasks.http import Http

from core.logger import setup_logger
from core.paths import get_project_root


class DSGatewayClient:
    """DolphinScheduler Gateway 客户端"""
    
    def __init__(self):
        """初始化 Gateway 客户端"""
        self.logger = setup_logger("ds_gateway_client")
        
        # 从环境变量读取配置
        self.gateway_host = os.getenv('DOLPHINSCHEDULER_GATEWAY_HOST', 'localhost')
        self.gateway_port = int(os.getenv('DOLPHINSCHEDULER_GATEWAY_PORT', '25333'))
        self.user = os.getenv('DOLPHINSCHEDULER_USER', 'admin')
        self.password = os.getenv('DOLPHINSCHEDULER_PASSWORD', 'dolphinscheduler123')
        self.project = os.getenv('DOLPHINSCHEDULER_PROJECT', 'xcnstock')
        self.tenant = os.getenv('DOLPHINSCHEDULER_TENANT', 'default')
        
        self.logger.info(f"Gateway 配置: {self.gateway_host}:{self.gateway_port}")
        self.logger.info(f"项目: {self.project}, 用户: {self.user}, 租户: {self.tenant}")
        
        # 配置 SDK
        self._configure_sdk()
    
    def _configure_sdk(self):
        """配置 pydolphinscheduler SDK"""
        os.environ['PYDS_JAVA_GATEWAY_ADDRESS'] = self.gateway_host
        os.environ['PYDS_JAVA_GATEWAY_PORT'] = str(self.gateway_port)
        os.environ['PYDS_USER'] = self.user
        os.environ['PYDS_PASSWORD'] = self.password
        os.environ['PYDS_PROJECT'] = self.project
        os.environ['PYDS_TENANT'] = self.tenant
        self.logger.info("SDK 配置完成")
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        status = {
            'gateway_host': self.gateway_host,
            'gateway_port': self.gateway_port,
            'user': self.user,
            'project': self.project,
            'tenant': self.tenant,
            'timestamp': datetime.now().isoformat(),
            'gateway_connected': False
        }
        
        try:
            from py4j.java_gateway import JavaGateway, GatewayParameters
            
            gateway = JavaGateway(
                gateway_parameters=GatewayParameters(
                    address=self.gateway_host,
                    port=self.gateway_port,
                    auto_convert=True
                )
            )
            # 尝试简单的连接测试，不调用特定方法
            gateway.jvm.java.lang.System.currentTimeMillis()
            status['gateway_connected'] = True
            status['gateway_version'] = 'connected'
            self.logger.info(f"✅ Gateway 连接成功")
        except Exception as e:
            status['error'] = str(e)
            self.logger.error(f"❌ Gateway 连接失败: {e}")
        
        return status
    
    def create_process_definition(
        self,
        name: str,
        description: str = "",
        schedule: Optional[str] = None,
        warning_group_id: int = 0
    ) -> ProcessDefinition:
        """创建工作流定义"""
        process = ProcessDefinition(
            name=name,
            description=description,
            project=self.project,
            user=self.user,
            tenant=self.tenant,
            warning_group_id=warning_group_id
        )
        
        if schedule:
            process.schedule = schedule
        
        self.logger.info(f"创建工作流定义: {name}")
        return process
    
    def create_shell_task(
        self,
        name: str,
        command: str
    ) -> Shell:
        """创建 Shell 任务"""
        task = Shell(
            name=name,
            command=command
        )
        self.logger.info(f"创建 Shell 任务: {name}")
        return task
    
    def create_python_task(
        self,
        name: str,
        script_path: str,
        **kwargs
    ) -> Python:
        """创建 Python 任务"""
        project_root = get_project_root()
        full_path = project_root / script_path
        
        command = f"cd {project_root} && python {full_path}"
        for key, value in kwargs.items():
            command += f" --{key} {value}"
        
        task = Python(
            name=name,
            definition=command
        )
        self.logger.info(f"创建 Python 任务: {name}")
        return task
    
    def submit_process(self, process: ProcessDefinition) -> Dict[str, Any]:
        """提交工作流到 Gateway"""
        try:
            self.logger.info(f"提交工作流: {process.name}")
            result = process.submit()
            
            return {
                'success': True,
                'process_name': process.name,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"❌ 提交失败: {e}")
            return {
                'success': False,
                'process_name': process.name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


class XCNStockWorkflowBuilder:
    """XCNStock 工作流构建器"""
    
    def __init__(self, client: DSGatewayClient):
        self.client = client
        self.logger = setup_logger("workflow_builder")
        self.project_root = get_project_root()
    
    def build_data_collection_workflow(self) -> ProcessDefinition:
        """构建数据采集工作流"""
        process = self.client.create_process_definition(
            name="data_collection_daily",
            description="每日数据采集工作流",
            schedule="0 30 15 * * ?"  # 每天 15:30
        )
        
        # 任务定义
        task_check = self.client.create_shell_task(
            name="check_market",
            command=f"cd {self.project_root} && python -c 'print(\"Market check\")'"
        )
        
        task_kline = self.client.create_python_task(
            name="collect_kline",
            script_path="scripts/data_collect.py",
            pre_tasks=[task_check]
        )
        
        task_financial = self.client.create_python_task(
            name="collect_financial",
            script_path="scripts/financial_data_fetcher.py",
            pre_tasks=[task_kline]
        )
        
        task_quality = self.client.create_python_task(
            name="quality_check",
            script_path="services/data_service/quality/data_quality_monitor.py",
            pre_tasks=[task_financial]
        )
        
        # 添加任务
        process.add_task(task_check)
        process.add_task(task_kline)
        process.add_task(task_financial)
        process.add_task(task_quality)
        
        self.logger.info("✅ 数据采集工作流构建完成")
        return process
    
    def build_stock_selection_workflow(self) -> ProcessDefinition:
        """构建选股策略工作流"""
        process = self.client.create_process_definition(
            name="stock_selection_daily",
            description="每日选股策略工作流",
            schedule="0 0 16 * * ?"  # 每天 16:00
        )
        
        task_freshness = self.client.create_python_task(
            name="check_freshness",
            script_path="services/data_service/quality/data_freshness_check.py"
        )
        
        task_selection = self.client.create_python_task(
            name="run_selection",
            script_path="workflows/real_stock_selection_workflow.py",
            pre_tasks=[task_freshness],
            strategy="comprehensive",
            top_n="50"
        )
        
        task_report = self.client.create_python_task(
            name="generate_report",
            script_path="scripts/generate_selection_report.py",
            pre_tasks=[task_selection]
        )
        
        process.add_task(task_freshness)
        process.add_task(task_selection)
        process.add_task(task_report)
        
        self.logger.info("✅ 选股策略工作流构建完成")
        return process
    
    def build_backtest_workflow(self) -> ProcessDefinition:
        """构建回测工作流"""
        process = self.client.create_process_definition(
            name="backtest_weekly",
            description="每周回测工作流",
            schedule="0 0 9 ? * MON"  # 每周一 9:00
        )
        
        task_backtest = self.client.create_python_task(
            name="run_backtest",
            script_path="scripts/backtest_strategy.py",
            strategy="comprehensive",
            days="30"
        )
        
        task_report = self.client.create_python_task(
            name="generate_report",
            script_path="scripts/generate_backtest_report.py",
            pre_tasks=[task_backtest]
        )
        
        process.add_task(task_backtest)
        process.add_task(task_report)
        
        self.logger.info("✅ 回测工作流构建完成")
        return process


def test_gateway():
    """测试 Gateway 连接"""
    print("="*70)
    print("DolphinScheduler Gateway 连接测试 (3.1.1)")
    print("="*70)
    
    client = DSGatewayClient()
    status = client.health_check()
    
    print(f"\n连接状态:")
    print(f"  Gateway: {status['gateway_host']}:{status['gateway_port']}")
    print(f"  连接状态: {'✅ 成功' if status['gateway_connected'] else '❌ 失败'}")
    print(f"  版本: {status.get('gateway_version', 'N/A')}")
    print(f"  项目: {status['project']}")
    print(f"  用户: {status['user']}")
    
    return status['gateway_connected']


if __name__ == "__main__":
    connected = test_gateway()
    
    if connected:
        print("\n✅ Gateway 连接成功")
    else:
        print("\n❌ Gateway 连接失败")
        print("  请检查:")
        print("    1. DolphinScheduler Java Gateway 是否启动")
        print("    2. 端口 25333 是否可访问")
        print("    3. 防火墙设置")
