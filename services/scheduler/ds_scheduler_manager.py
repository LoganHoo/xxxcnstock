#!/usr/bin/env python3
"""
DolphinScheduler 统一调度管理器

整合三种操作方式:
1. Gateway (25333) - 提交工作流定义
2. REST API (12345) - 触发工作流执行
3. DataHub - 采集血缘和监控
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum

from core.logger import setup_logger


class OperationMode(Enum):
    """操作模式"""
    GATEWAY = "gateway"      # 通过 Gateway 提交
    REST_API = "rest_api"    # 通过 REST API 触发
    DATAHUB = "datahub"      # 通过 DataHub 采集


class DSSchedulerManager:
    """DolphinScheduler 统一调度管理器"""
    
    def __init__(self):
        """初始化管理器"""
        self.logger = setup_logger("ds_scheduler_manager")
        
        # 延迟导入，避免循环依赖
        self._gateway_client = None
        self._rest_client = None
        self._datahub_collector = None
        
        self.logger.info("DolphinScheduler 管理器初始化完成")
    
    @property
    def gateway(self):
        """获取 Gateway 客户端"""
        if self._gateway_client is None:
            from services.scheduler.ds_gateway_client import DSGatewayClient
            self._gateway_client = DSGatewayClient()
        return self._gateway_client
    
    @property
    def rest(self):
        """获取 REST API 客户端"""
        if self._rest_client is None:
            from services.scheduler.ds_rest_client import DSRestClient
            self._rest_client = DSRestClient()
        return self._rest_client
    
    @property
    def datahub(self):
        """获取 DataHub 采集器"""
        if self._datahub_collector is None:
            from services.scheduler.ds_datahub_lineage import DSDataHubLineageCollector
            self._datahub_collector = DSDataHubLineageCollector()
        return self._datahub_collector
    
    def health_check(self, mode: OperationMode = None) -> Dict[str, Any]:
        """
        健康检查
        
        Args:
            mode: 指定模式检查，None 则检查所有
        
        Returns:
            健康状态
        """
        status = {
            'timestamp': datetime.now().isoformat(),
            'overall': False
        }
        
        modes_to_check = [mode] if mode else list(OperationMode)
        
        for m in modes_to_check:
            try:
                if m == OperationMode.GATEWAY:
                    status['gateway'] = self.gateway.health_check()
                elif m == OperationMode.REST_API:
                    status['rest_api'] = self.rest.health_check()
                elif m == OperationMode.DATAHUB:
                    status['datahub'] = self.datahub.health_check()
            except Exception as e:
                status[m.value] = {'error': str(e)}
        
        # 整体状态
        status['overall'] = all(
            status.get(m.value, {}).get('gateway_connected') or
            status.get(m.value, {}).get('api_connected') or
            status.get(m.value, {}).get('db_connected')
            for m in modes_to_check
        )
        
        return status
    
    def deploy_workflow(
        self,
        workflow_type: str,
        mode: OperationMode = OperationMode.GATEWAY
    ) -> Dict[str, Any]:
        """
        部署工作流
        
        Args:
            workflow_type: 工作流类型 (collection/selection/backtest)
            mode: 部署模式
        
        Returns:
            部署结果
        """
        self.logger.info(f"部署工作流: {workflow_type} (模式: {mode.value})")
        
        if mode == OperationMode.GATEWAY:
            return self._deploy_via_gateway(workflow_type)
        elif mode == OperationMode.REST_API:
            return self._deploy_via_rest(workflow_type)
        else:
            return {'success': False, 'error': f'不支持的部署模式: {mode.value}'}
    
    def _deploy_via_gateway(self, workflow_type: str) -> Dict[str, Any]:
        """通过 Gateway 部署"""
        from services.scheduler.ds_gateway_client import XCNStockWorkflowBuilder
        
        builder = XCNStockWorkflowBuilder(self.gateway)
        
        if workflow_type == 'collection':
            process = builder.build_data_collection_workflow()
        elif workflow_type == 'selection':
            process = builder.build_stock_selection_workflow()
        elif workflow_type == 'backtest':
            process = builder.build_backtest_workflow()
        else:
            return {'success': False, 'error': f'未知工作流类型: {workflow_type}'}
        
        return self.gateway.submit_process(process)
    
    def _deploy_via_rest(self, workflow_type: str) -> Dict[str, Any]:
        """通过 REST API 部署 (导入 JSON)"""
        # REST API 主要用于触发，部署需要通过 Gateway
        return {
            'success': False,
            'error': 'REST API 不支持直接部署，请使用 Gateway 模式',
            'suggestion': '使用 mode=OperationMode.GATEWAY'
        }
    
    def trigger_workflow(
        self,
        project_name: str,
        workflow_name: str,
        mode: OperationMode = OperationMode.REST_API
    ) -> Dict[str, Any]:
        """
        触发工作流执行
        
        Args:
            project_name: 项目名称
            workflow_name: 工作流名称
            mode: 触发模式
        
        Returns:
            触发结果
        """
        self.logger.info(f"触发工作流: {workflow_name} (模式: {mode.value})")
        
        if mode == OperationMode.REST_API:
            return self.rest.trigger_workflow_by_name(project_name, workflow_name)
        else:
            return {'success': False, 'error': f'不支持的触发模式: {mode.value}'}
    
    def get_lineage(
        self,
        project_name: Optional[str] = None,
        mode: OperationMode = OperationMode.DATAHUB
    ) -> Dict[str, Any]:
        """
        获取工作流血缘
        
        Args:
            project_name: 项目名称
            mode: 采集模式
        
        Returns:
            血缘数据
        """
        self.logger.info(f"获取血缘: {project_name or '全部'} (模式: {mode.value})")
        
        if mode == OperationMode.DATAHUB:
            return self.datahub.export_lineage_to_datahub(project_name)
        else:
            return {'success': False, 'error': f'不支持的采集模式: {mode.value}'}
    
    def get_execution_history(
        self,
        workflow_name: Optional[str] = None,
        limit: int = 100,
        mode: OperationMode = OperationMode.DATAHUB
    ) -> List[Dict]:
        """
        获取执行历史
        
        Args:
            workflow_name: 工作流名称
            limit: 返回记录数
            mode: 查询模式
        
        Returns:
            执行历史列表
        """
        if mode == OperationMode.DATAHUB:
            executions = self.datahub.get_task_execution_history(
                workflow_name=workflow_name,
                limit=limit
            )
            return [{
                'task_name': e.task_name,
                'workflow_name': e.workflow_name,
                'start_time': e.start_time,
                'end_time': e.end_time,
                'status': e.status,
                'duration_ms': e.duration_ms
            } for e in executions]
        elif mode == OperationMode.REST_API:
            # 通过 REST API 获取
            return []
        else:
            return []
    
    def get_statistics(self, mode: OperationMode = OperationMode.DATAHUB) -> Dict[str, Any]:
        """
        获取统计信息
        
        Args:
            mode: 查询模式
        
        Returns:
            统计信息
        """
        if mode == OperationMode.DATAHUB:
            return self.datahub.get_project_statistics()
        else:
            return {'error': f'不支持的统计模式: {mode.value}'}
    
    def close(self):
        """关闭所有连接"""
        if self._rest_client:
            self._rest_client.close()
        if self._datahub_collector:
            self._datahub_collector.close()
        self.logger.info("所有连接已关闭")


def main():
    """主函数 - 演示用法"""
    print("="*70)
    print("DolphinScheduler 统一调度管理器")
    print("="*70)
    
    manager = DSSchedulerManager()
    
    # 健康检查
    print("\n【健康检查】")
    status = manager.health_check()
    print(f"  Gateway (25333): {'✅' if status.get('gateway', {}).get('gateway_connected') else '❌'}")
    print(f"  REST API (12345): {'✅' if status.get('rest_api', {}).get('api_connected') else '❌'}")
    print(f"  DataHub (MySQL): {'✅' if status.get('datahub', {}).get('db_connected') else '❌'}")
    
    # 示例: 部署工作流
    print("\n【部署工作流示例】")
    print("  manager.deploy_workflow('collection', OperationMode.GATEWAY)")
    
    # 示例: 触发工作流
    print("\n【触发工作流示例】")
    print("  manager.trigger_workflow('xcnstock', 'data_collection_daily', OperationMode.REST_API)")
    
    # 示例: 获取血缘
    print("\n【获取血缘示例】")
    print("  manager.get_lineage('xcnstock', OperationMode.DATAHUB)")
    
    # 示例: 获取执行历史
    print("\n【获取执行历史示例】")
    print("  manager.get_execution_history('data_collection_daily', limit=10)")
    
    manager.close()
    
    print("\n" + "="*70)
    print("使用说明:")
    print("  1. 定义作业: 使用 Gateway (25333) 提交工作流定义")
    print("  2. 调度执行: 使用 REST API (12345) 触发工作流执行")
    print("  3. 治理监控: 使用 DataHub 采集 MySQL 元数据获取血缘")
    print("="*70)


if __name__ == "__main__":
    main()
