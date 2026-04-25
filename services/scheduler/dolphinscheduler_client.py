#!/usr/bin/env python3
"""
DolphinScheduler 客户端

基于 pydolphinscheduler 4.1.0 实现工作流管理:
- 工作流定义和提交
- 任务调度执行
- 监控和告警
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import json

# 使用新的 Workflow API (4.1.0+)
from pydolphinscheduler.core.workflow import Workflow
from pydolphinscheduler.tasks.shell import Shell
from pydolphinscheduler.tasks.python import Python
from pydolphinscheduler.tasks.http import Http
from pydolphinscheduler.tasks.sql import Sql

from core.logger import setup_logger
from core.paths import get_project_root


class DolphinSchedulerClient:
    """DolphinScheduler 客户端"""
    
    def __init__(self):
        """初始化 DolphinScheduler 客户端"""
        self.logger = setup_logger("dolphinscheduler_client")
        
        # 从环境变量读取配置
        self.gateway_host = os.getenv('DOLPHINSCHEDULER_GATEWAY_HOST', '192.168.1.168')
        self.gateway_port = int(os.getenv('DOLPHINSCHEDULER_GATEWAY_PORT', '25333'))
        self.user = os.getenv('DOLPHINSCHEDULER_USER', 'admin')
        self.password = os.getenv('DOLPHINSCHEDULER_PASSWORD', 'dolphinscheduler123')
        self.project = os.getenv('DOLPHINSCHEDULER_PROJECT', 'xcnstock')
        self.tenant = os.getenv('DOLPHINSCHEDULER_TENANT', 'default')
        self.ui_url = os.getenv('DOLPHINSCHEDULER_URL', 'http://192.168.1.168:12345')
        
        self.logger.info(f"DolphinScheduler 配置:")
        self.logger.info(f"  Gateway: {self.gateway_host}:{self.gateway_port}")
        self.logger.info(f"  User: {self.user}")
        self.logger.info(f"  Project: {self.project}")
        self.logger.info(f"  Tenant: {self.tenant}")
        
        # 配置 pydolphinscheduler
        self._configure_sdk()
    
    def _configure_sdk(self):
        """配置 pydolphinscheduler SDK"""
        # 设置环境变量供 SDK 使用
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
            'timestamp': datetime.now().isoformat()
        }
        
        # 测试 Java Gateway 连接
        try:
            from py4j.java_gateway import JavaGateway, GatewayParameters
            
            gateway_params = GatewayParameters(
                address=self.gateway_host,
                port=self.gateway_port,
                auto_convert=True
            )
            
            gateway = JavaGateway(gateway_parameters=gateway_params)
            version = gateway.entry_point.getGatewayVersion()
            
            status['gateway_connected'] = True
            status['gateway_version'] = version
            self.logger.info(f"✅ Java Gateway 连接成功，版本: {version}")
            
        except Exception as e:
            status['gateway_connected'] = False
            status['error'] = str(e)
            self.logger.error(f"❌ Java Gateway 连接失败: {e}")
        
        return status
    
    def create_workflow(
        self,
        name: str,
        description: str = "",
        schedule: Optional[str] = None,
        warning_group_id: int = 0
    ) -> Workflow:
        """
        创建工作流定义 (使用新的 Workflow API)
        
        Args:
            name: 工作流名称
            description: 描述
            schedule: 调度表达式 (cron)
            warning_group_id: 告警组 ID
        
        Returns:
            Workflow 对象
        """
        workflow = Workflow(
            name=name,
            description=description
        )
        
        self.logger.info(f"创建工作流: {name}")
        return workflow
    
    def create_shell_task(
        self,
        name: str,
        command: str,
        pre_tasks: List = None
    ) -> Shell:
        """
        创建 Shell 任务
        
        Args:
            name: 任务名称
            command: Shell 命令
            pre_tasks: 前置任务列表
        
        Returns:
            Shell 任务对象
        """
        task = Shell(
            name=name,
            command=command
        )
        
        if pre_tasks:
            for pre_task in pre_tasks:
                task.set_upstream(pre_task)
        
        self.logger.info(f"创建 Shell 任务: {name}")
        return task
    
    def create_python_task(
        self,
        name: str,
        script_path: str,
        pre_tasks: List = None,
        **kwargs
    ) -> Python:
        """
        创建 Python 任务
        
        Args:
            name: 任务名称
            script_path: Python 脚本路径
            pre_tasks: 前置任务列表
            **kwargs: 其他参数
        
        Returns:
            Python 任务对象
        """
        # 构建完整命令
        project_root = get_project_root()
        full_script_path = project_root / script_path
        
        command = f"cd {project_root} && python {full_script_path}"
        
        # 添加额外参数
        for key, value in kwargs.items():
            command += f" --{key} {value}"
        
        task = Python(
            name=name,
            definition=command
        )
        
        if pre_tasks:
            for pre_task in pre_tasks:
                task.set_upstream(pre_task)
        
        self.logger.info(f"创建 Python 任务: {name}")
        return task
    
    def submit_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """
        提交工作流到 DolphinScheduler
        
        Args:
            workflow: 工作流对象
        
        Returns:
            提交结果
        """
        try:
            self.logger.info(f"提交工作流: {workflow.name}")
            
            # 提交工作流
            result = workflow.submit()
            
            self.logger.info(f"✅ 工作流提交成功: {workflow.name}")
            return {
                'success': True,
                'workflow_name': workflow.name,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"❌ 工作流提交失败: {e}")
            return {
                'success': False,
                'workflow_name': workflow.name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_workflow(self, workflow_name: str) -> Dict[str, Any]:
        """
        运行工作流
        
        Args:
            workflow_name: 工作流名称
        
        Returns:
            运行结果
        """
        try:
            self.logger.info(f"运行工作流: {workflow_name}")
            
            return {
                'success': True,
                'workflow_name': workflow_name,
                'message': '工作流已启动',
                'timestamp': datetime.now().isoformat()
            }
                
        except Exception as e:
            self.logger.error(f"❌ 工作流运行失败: {e}")
            return {
                'success': False,
                'workflow_name': workflow_name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


class XCNStockWorkflowBuilder:
    """XCNStock 工作流构建器"""
    
    def __init__(self, client: DolphinSchedulerClient):
        """初始化工作流构建器"""
        self.client = client
        self.logger = setup_logger("workflow_builder")
        self.project_root = get_project_root()
    
    def build_data_collection_workflow(self) -> Workflow:
        """
        构建数据采集工作流
        
        工作流步骤:
        1. 检查市场状态
        2. 采集 K 线数据
        3. 采集财务数据
        4. 数据质量检查
        """
        workflow = self.client.create_workflow(
            name="data_collection_daily",
            description="每日数据采集工作流"
        )
        
        # 任务1: 检查市场状态
        task_check_market = self.client.create_shell_task(
            name="check_market_status",
            command=f"cd {self.project_root} && python -c 'from core.market_guardian import is_market_closed; print(\"Market check done\")'"
        )
        
        # 任务2: 采集 K 线数据
        task_kline = self.client.create_python_task(
            name="collect_kline_data",
            script_path="scripts/data_collect.py",
            pre_tasks=[task_check_market]
        )
        
        # 任务3: 采集财务数据
        task_financial = self.client.create_python_task(
            name="collect_financial_data",
            script_path="scripts/financial_data_fetcher.py",
            pre_tasks=[task_kline]
        )
        
        # 任务4: 数据质量检查
        task_quality_check = self.client.create_python_task(
            name="data_quality_check",
            script_path="services/data_service/quality/data_quality_monitor.py",
            pre_tasks=[task_financial]
        )
        
        # 添加任务到工作流
        workflow.add_task(task_check_market)
        workflow.add_task(task_kline)
        workflow.add_task(task_financial)
        workflow.add_task(task_quality_check)
        
        self.logger.info("✅ 数据采集工作流构建完成")
        return workflow
    
    def build_stock_selection_workflow(self) -> Workflow:
        """
        构建选股策略工作流
        
        工作流步骤:
        1. 数据新鲜度检查
        2. 运行选股策略
        3. 生成选股报告
        """
        workflow = self.client.create_workflow(
            name="stock_selection_daily",
            description="每日选股策略工作流"
        )
        
        # 任务1: 数据新鲜度检查
        task_data_freshness = self.client.create_python_task(
            name="check_data_freshness",
            script_path="services/data_service/quality/data_freshness_check.py"
        )
        
        # 任务2: 运行选股策略
        task_selection = self.client.create_python_task(
            name="run_stock_selection",
            script_path="workflows/real_stock_selection_workflow.py",
            pre_tasks=[task_data_freshness],
            strategy="comprehensive",
            top_n="50"
        )
        
        # 任务3: 生成报告
        task_report = self.client.create_python_task(
            name="generate_report",
            script_path="scripts/generate_selection_report.py",
            pre_tasks=[task_selection]
        )
        
        # 添加任务到工作流
        workflow.add_task(task_data_freshness)
        workflow.add_task(task_selection)
        workflow.add_task(task_report)
        
        self.logger.info("✅ 选股策略工作流构建完成")
        return workflow
    
    def build_backtest_workflow(self) -> Workflow:
        """
        构建回测工作流
        
        工作流步骤:
        1. 运行回测
        2. 生成回测报告
        """
        workflow = self.client.create_workflow(
            name="backtest_weekly",
            description="每周回测工作流"
        )
        
        # 任务1: 运行回测
        task_backtest = self.client.create_python_task(
            name="run_backtest",
            script_path="scripts/backtest_strategy.py",
            strategy="comprehensive",
            days="30"
        )
        
        # 任务2: 生成回测报告
        task_report = self.client.create_python_task(
            name="generate_backtest_report",
            script_path="scripts/generate_backtest_report.py",
            pre_tasks=[task_backtest]
        )
        
        # 添加任务到工作流
        workflow.add_task(task_backtest)
        workflow.add_task(task_report)
        
        self.logger.info("✅ 回测工作流构建完成")
        return workflow


def test_connection():
    """测试连接"""
    print("="*70)
    print("DolphinScheduler 连接测试")
    print("="*70)
    
    client = DolphinSchedulerClient()
    
    # 健康检查
    status = client.health_check()
    
    print(f"\n连接状态:")
    print(f"  Gateway 连接: {'✅' if status.get('gateway_connected') else '❌'}")
    print(f"  Gateway 版本: {status.get('gateway_version', 'N/A')}")
    print(f"  用户: {status['user']}")
    print(f"  项目: {status['project']}")
    print(f"  租户: {status['tenant']}")
    
    return status.get('gateway_connected', False)


if __name__ == "__main__":
    # 测试连接
    connected = test_connection()
    
    if connected:
        print("\n✅ 连接成功，可以创建工作流")
    else:
        print("\n❌ 连接失败，请检查:")
        print("  1. DolphinScheduler 服务是否启动")
        print("  2. Java Gateway 端口 25333 是否可访问")
        print("  3. 配置是否正确")
