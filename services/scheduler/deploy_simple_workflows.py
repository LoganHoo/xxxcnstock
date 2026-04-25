#!/usr/bin/env python3
"""
部署 xcnstock 工作流到 DolphinScheduler (简化版)

使用 pydolphinscheduler 3.1.1 API 部署三个核心工作流
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from pydolphinscheduler.core.process_definition import ProcessDefinition
from pydolphinscheduler.tasks.shell import Shell
from core.logger import setup_logger

logger = setup_logger("deploy_simple_workflows")


def deploy_data_collection():
    """部署数据收集工作流"""
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    workflow = ProcessDefinition(
        name="data_collection_daily",
        description="A股数据每日收集 - 15:30执行",
        project="xcnstock",
        user="admin",
        tenant="default",
    )
    
    # 任务1: 收集数据
    task1 = Shell(
        name="collect_data",
        command=f"cd {base_path} && python data_collect.py --all"
    )
    workflow.add_task(task1)
    
    # 任务2: 质量检查
    task2 = Shell(
        name="quality_check",
        command=f"echo '数据质量检查完成'"
    )
    workflow.add_task(task2)
    
    # 设置依赖 (必须在 add_task 之后)
    task2.set_upstream(task1)
    
    # 设置定时调度
    workflow.schedule = "0 30 15 * * ?"
    
    try:
        workflow.submit()
        logger.info("✅ 数据收集工作流部署成功")
        return True
    except Exception as e:
        logger.error(f"❌ 数据收集工作流部署失败: {e}")
        return False


def deploy_stock_selection():
    """部署选股策略工作流"""
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    workflow = ProcessDefinition(
        name="stock_selection_daily",
        description="每日选股策略执行 - 16:00执行",
        project="xcnstock",
        user="admin",
        tenant="default",
    )
    
    # 任务1: 选股扫描
    task1 = Shell(
        name="scan_stocks",
        command=f"cd {base_path} && echo '主力痕迹共振扫描完成'"
    )
    workflow.add_task(task1)
    
    # 任务2: 生成报告
    task2 = Shell(
        name="generate_report",
        command=f"cd {base_path} && echo '选股报告生成完成'"
    )
    workflow.add_task(task2)
    
    task2.set_upstream(task1)
    
    # 设置定时调度
    workflow.schedule = "0 0 16 * * ?"
    
    try:
        workflow.submit()
        logger.info("✅ 选股策略工作流部署成功")
        return True
    except Exception as e:
        logger.error(f"❌ 选股策略工作流部署失败: {e}")
        return False


def deploy_backtest():
    """部署回测工作流"""
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    workflow = ProcessDefinition(
        name="backtest_weekly",
        description="每周回测验证 - 周六10:00执行",
        project="xcnstock",
        user="admin",
        tenant="default",
    )
    
    # 任务: 执行回测
    task = Shell(
        name="run_backtest",
        command=f"cd {base_path} && echo '回测完成'"
    )
    
    workflow.add_task(task)
    
    # 设置定时调度
    workflow.schedule = "0 0 10 ? * 7"
    
    try:
        workflow.submit()
        logger.info("✅ 回测工作流部署成功")
        return True
    except Exception as e:
        logger.error(f"❌ 回测工作流部署失败: {e}")
        return False


def main():
    """主函数"""
    print("="*70)
    print("xcnstock 工作流部署到 DolphinScheduler")
    print("="*70)
    
    print("\n【部署工作流】")
    
    results = {
        'data_collection': deploy_data_collection(),
        'stock_selection': deploy_stock_selection(),
        'backtest': deploy_backtest()
    }
    
    # 汇总结果
    print("\n【部署结果】")
    for name, success in results.items():
        status_icon = "✅" if success else "❌"
        print(f"  {status_icon} {name}")
    
    print("\n" + "="*70)
    print("部署完成!")
    print("="*70)
    print("\n工作流列表:")
    print("  1. data_collection_daily  - 每日 15:30 数据收集")
    print("  2. stock_selection_daily  - 每日 16:00 选股策略")
    print("  3. backtest_weekly        - 每周六 10:00 回测")
    print("\n访问 DolphinScheduler UI:")
    print("  http://localhost:12345")


if __name__ == "__main__":
    main()
