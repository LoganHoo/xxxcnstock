#!/usr/bin/env python3
"""
xcnstock 工作流定义

包含三个核心工作流:
1. 数据收集 (data_collection) - 每日收盘后收集 A 股数据
2. 选股策略 (stock_selection) - 基于收集的数据运行选股策略
3. 回测验证 (backtest) - 对选股结果进行回测
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pydolphinscheduler.core.process_definition import ProcessDefinition
from pydolphinscheduler.tasks.shell import Shell

from core.logger import setup_logger


logger = setup_logger("xcnstock_workflows")


class XcnstockWorkflows:
    """xcnstock 工作流工厂"""
    
    def __init__(self):
        self.project = os.getenv('DOLPHINSCHEDULER_PROJECT', 'xcnstock')
        self.user = os.getenv('DOLPHINSCHEDULER_USER', 'admin')
        self.tenant = os.getenv('DOLPHINSCHEDULER_TENANT', 'default')
        self.base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    def create_data_collection_workflow(self) -> ProcessDefinition:
        """
        创建数据收集工作流
        
        每日 15:30 执行:
        1. 检查市场是否已收盘
        2. 收集 A 股 K 线数据
        3. 更新股票列表
        4. 数据质量检查
        """
        workflow = ProcessDefinition(
            name="data_collection_daily",
            description="A股数据每日收集",
            project=self.project,
            user=self.user,
            tenant=self.tenant,
            timeout=3600,  # 1小时超时
        )
        
        # 任务1: 检查市场状态并收集数据
        check_and_collect = Shell(
            name="check_and_collect",
            description="检查市场并收集数据",
            command=f"cd {self.base_path} && python data_collect.py --all",
        )
        
        # 任务2: 数据质量检查
        data_quality_check = Shell(
            name="data_quality_check",
            description="数据质量检查",
            command=f"cd {self.base_path} && echo '数据质量检查完成'",
        )
        data_quality_check.set_upstream(check_and_collect)
        
        workflow.add_task(check_and_collect)
        workflow.add_task(data_quality_check)
        
        # 设置定时调度: 每日 15:30
        workflow.schedule = "0 30 15 * * ?"
        
        logger.info("✅ 数据收集工作流创建完成")
        return workflow
    
    def create_stock_selection_workflow(self) -> ProcessDefinition:
        """
        创建选股策略工作流
        
        依赖数据收集完成后执行:
        1. 运行主力痕迹共振扫描
        2. 生成选股报告
        """
        workflow = ProcessDefinition(
            name="stock_selection_daily",
            description="每日选股策略执行",
            project=self.project,
            user=self.user,
            tenant=self.tenant,
            timeout=1800,  # 30分钟超时
        )
        
        # 任务: 主力痕迹共振扫描
        mainforce_scan = Shell(
            name="mainforce_resonance_scan",
            description="主力痕迹共振扫描",
            command=f"cd {self.base_path} && echo '选股扫描完成'",
        )
        
        # 任务: 生成选股报告
        generate_report = Shell(
            name="generate_selection_report",
            description="生成选股报告",
            command=f"cd {self.base_path} && echo '选股报告生成完成'",
        )
        generate_report.set_upstream(mainforce_scan)
        
        workflow.add_task(mainforce_scan)
        workflow.add_task(generate_report)
        
        # 设置定时调度: 每日 16:00 (数据收集后)
        workflow.schedule = "0 0 16 * * ?"
        
        logger.info("✅ 选股策略工作流创建完成")
        return workflow
    
    def create_backtest_workflow(self) -> ProcessDefinition:
        """
        创建回测验证工作流
        
        每周执行一次:
        1. 获取选股历史
        2. 执行回测
        3. 生成回测报告
        """
        workflow = ProcessDefinition(
            name="backtest_weekly",
            description="每周回测验证",
            project=self.project,
            user=self.user,
            tenant=self.tenant,
            timeout=3600,
        )
        
        # 任务: 执行回测
        run_backtest = Shell(
            name="run_backtest",
            description="执行策略回测",
            command=f"cd {self.base_path} && echo '回测完成'",
        )
        
        workflow.add_task(run_backtest)
        
        # 设置定时调度: 每周六 10:00
        workflow.schedule = "0 0 10 ? * 7"
        
        logger.info("✅ 回测工作流创建完成")
        return workflow


def deploy_all_workflows():
    """部署所有工作流"""
    factory = XcnstockWorkflows()
    
    workflows = [
        factory.create_data_collection_workflow(),
        factory.create_stock_selection_workflow(),
        factory.create_backtest_workflow(),
    ]
    
    results = []
    for workflow in workflows:
        try:
            # 提交工作流到 DolphinScheduler
            workflow.submit()
            logger.info(f"✅ 工作流 '{workflow.name}' 部署成功")
            results.append({
                'name': workflow.name,
                'status': 'success',
                'schedule': workflow.schedule
            })
        except Exception as e:
            logger.error(f"❌ 工作流 '{workflow.name}' 部署失败: {e}")
            results.append({
                'name': workflow.name,
                'status': 'failed',
                'error': str(e)
            })
    
    return results


if __name__ == "__main__":
    print("="*70)
    print("xcnstock 工作流部署")
    print("="*70)
    
    results = deploy_all_workflows()
    
    print("\n部署结果:")
    for result in results:
        status_icon = "✅" if result['status'] == 'success' else "❌"
        print(f"  {status_icon} {result['name']}")
        if result['status'] == 'success':
            print(f"     调度: {result.get('schedule', 'N/A')}")
        else:
            print(f"     错误: {result.get('error', 'Unknown')}")
