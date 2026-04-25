#!/usr/bin/env python3
"""
工作流部署脚本

用于将 XCNStock 工作流部署到 DolphinScheduler
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
from pathlib import Path

from services.scheduler.dolphinscheduler_client import (
    DolphinSchedulerClient,
    XCNStockWorkflowBuilder
)
from core.logger import setup_logger

logger = setup_logger("deploy_workflows")


def deploy_data_collection_workflow():
    """部署数据采集工作流"""
    logger.info("="*70)
    logger.info("部署数据采集工作流")
    logger.info("="*70)
    
    client = DolphinSchedulerClient()
    
    # 检查连接
    status = client.health_check()
    if not status['gateway_connected']:
        logger.error("❌ DolphinScheduler 连接失败，无法部署")
        return False
    
    # 构建工作流
    builder = XCNStockWorkflowBuilder(client)
    process = builder.build_data_collection_workflow()
    
    # 提交工作流
    result = client.submit_workflow(process)
    
    if result['success']:
        logger.info(f"✅ 数据采集工作流部署成功")
        logger.info(f"   工作流名称: {result['process_name']}")
        logger.info(f"   调度时间: 每天 15:30")
        return True
    else:
        logger.error(f"❌ 部署失败: {result.get('error', 'Unknown error')}")
        return False


def deploy_stock_selection_workflow():
    """部署选股策略工作流"""
    logger.info("="*70)
    logger.info("部署选股策略工作流")
    logger.info("="*70)
    
    client = DolphinSchedulerClient()
    
    # 检查连接
    status = client.health_check()
    if not status['gateway_connected']:
        logger.error("❌ DolphinScheduler 连接失败，无法部署")
        return False
    
    # 构建工作流
    builder = XCNStockWorkflowBuilder(client)
    process = builder.build_stock_selection_workflow()
    
    # 提交工作流
    result = client.submit_workflow(process)
    
    if result['success']:
        logger.info(f"✅ 选股策略工作流部署成功")
        logger.info(f"   工作流名称: {result['process_name']}")
        logger.info(f"   调度时间: 每天 16:00")
        return True
    else:
        logger.error(f"❌ 部署失败: {result.get('error', 'Unknown error')}")
        return False


def deploy_backtest_workflow():
    """部署回测工作流"""
    logger.info("="*70)
    logger.info("部署回测工作流")
    logger.info("="*70)
    
    client = DolphinSchedulerClient()
    
    # 检查连接
    status = client.health_check()
    if not status['gateway_connected']:
        logger.error("❌ DolphinScheduler 连接失败，无法部署")
        return False
    
    # 构建工作流
    builder = XCNStockWorkflowBuilder(client)
    process = builder.build_backtest_workflow()
    
    # 提交工作流
    result = client.submit_workflow(process)
    
    if result['success']:
        logger.info(f"✅ 回测工作流部署成功")
        logger.info(f"   工作流名称: {result['process_name']}")
        logger.info(f"   调度时间: 每周一 9:00")
        return True
    else:
        logger.error(f"❌ 部署失败: {result.get('error', 'Unknown error')}")
        return False


def deploy_all_workflows():
    """部署所有工作流"""
    logger.info("="*70)
    logger.info("部署所有工作流到 DolphinScheduler")
    logger.info("="*70)
    
    results = {
        'data_collection': deploy_data_collection_workflow(),
        'stock_selection': deploy_stock_selection_workflow(),
        'backtest': deploy_backtest_workflow()
    }
    
    logger.info("\n" + "="*70)
    logger.info("部署结果汇总")
    logger.info("="*70)
    
    for name, success in results.items():
        status = "✅ 成功" if success else "❌ 失败"
        logger.info(f"  {name}: {status}")
    
    total = len(results)
    success_count = sum(results.values())
    
    logger.info(f"\n总计: {success_count}/{total} 个工作流部署成功")
    
    return success_count == total


def test_connection():
    """测试 DolphinScheduler 连接"""
    logger.info("="*70)
    logger.info("测试 DolphinScheduler 连接")
    logger.info("="*70)
    
    client = DolphinSchedulerClient()
    status = client.health_check()
    
    logger.info(f"\n连接状态:")
    logger.info(f"  Gateway 连接: {'✅' if status['gateway_connected'] else '❌'}")
    logger.info(f"  Gateway 版本: {status.get('gateway_version', 'N/A')}")
    logger.info(f"  用户: {status['user']}")
    logger.info(f"  项目: {status['project']}")
    logger.info(f"  租户: {status['tenant']}")
    
    return status['gateway_connected']


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='部署 XCNStock 工作流到 DolphinScheduler')
    parser.add_argument('--test', action='store_true', help='测试连接')
    parser.add_argument('--deploy', choices=['all', 'collection', 'selection', 'backtest'],
                       default='all', help='部署指定工作流')
    
    args = parser.parse_args()
    
    if args.test:
        connected = test_connection()
        sys.exit(0 if connected else 1)
    
    if args.deploy == 'all':
        success = deploy_all_workflows()
    elif args.deploy == 'collection':
        success = deploy_data_collection_workflow()
    elif args.deploy == 'selection':
        success = deploy_stock_selection_workflow()
    elif args.deploy == 'backtest':
        success = deploy_backtest_workflow()
    else:
        parser.print_help()
        success = False
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
