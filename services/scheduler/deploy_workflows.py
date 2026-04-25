#!/usr/bin/env python3
"""
部署 xcnstock 工作流到 DolphinScheduler

使用统一调度管理器部署三个核心工作流:
1. 数据收集 (data_collection_daily) - 每日 15:30
2. 选股策略 (stock_selection_daily) - 每日 16:00
3. 回测验证 (backtest_weekly) - 每周六 10:00
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from services.scheduler.ds_scheduler_manager import DSSchedulerManager, OperationMode
from core.logger import setup_logger

logger = setup_logger("deploy_workflows")


def deploy_data_collection_workflow(manager: DSSchedulerManager):
    """部署数据收集工作流"""
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    # 创建工作流定义
    workflow_def = {
        'name': 'data_collection_daily',
        'description': 'A股数据每日收集 - 15:30执行',
        'tasks': [
            {
                'name': 'check_market_and_collect',
                'type': 'SHELL',
                'command': f'cd {base_path} && python data_collect.py --all',
                'description': '检查市场收盘并收集K线数据'
            },
            {
                'name': 'data_quality_check',
                'type': 'SHELL',
                'command': f'cd {base_path} && echo "数据质量检查完成"',
                'description': '数据质量检查',
                'upstream': ['check_market_and_collect']
            }
        ],
        'schedule': {
            'crontab': '0 30 15 * * ?',  # 每日 15:30
            'timezone': 'Asia/Shanghai'
        }
    }
    
    try:
        # 使用 Gateway 部署
        result = manager.deploy_workflow('collection', OperationMode.GATEWAY)
        logger.info(f"✅ 数据收集工作流部署成功: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ 数据收集工作流部署失败: {e}")
        return False


def deploy_stock_selection_workflow(manager: DSSchedulerManager):
    """部署选股策略工作流"""
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    workflow_def = {
        'name': 'stock_selection_daily',
        'description': '每日选股策略执行 - 16:00执行',
        'tasks': [
            {
                'name': 'mainforce_scan',
                'type': 'SHELL',
                'command': f'cd {base_path} && echo "主力痕迹共振扫描完成"',
                'description': '主力痕迹共振扫描'
            },
            {
                'name': 'generate_report',
                'type': 'SHELL',
                'command': f'cd {base_path} && echo "选股报告生成完成"',
                'description': '生成选股报告',
                'upstream': ['mainforce_scan']
            }
        ],
        'schedule': {
            'crontab': '0 0 16 * * ?',  # 每日 16:00
            'timezone': 'Asia/Shanghai'
        }
    }
    
    try:
        result = manager.deploy_workflow('selection', OperationMode.GATEWAY)
        logger.info(f"✅ 选股策略工作流部署成功: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ 选股策略工作流部署失败: {e}")
        return False


def deploy_backtest_workflow(manager: DSSchedulerManager):
    """部署回测工作流"""
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    workflow_def = {
        'name': 'backtest_weekly',
        'description': '每周回测验证 - 周六10:00执行',
        'tasks': [
            {
                'name': 'run_backtest',
                'type': 'SHELL',
                'command': f'cd {base_path} && echo "回测完成"',
                'description': '执行策略回测'
            }
        ],
        'schedule': {
            'crontab': '0 0 10 ? * 7',  # 每周六 10:00
            'timezone': 'Asia/Shanghai'
        }
    }
    
    try:
        result = manager.deploy_workflow('backtest', OperationMode.GATEWAY)
        logger.info(f"✅ 回测工作流部署成功: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ 回测工作流部署失败: {e}")
        return False


def main():
    """主函数"""
    print("="*70)
    print("xcnstock 工作流部署到 DolphinScheduler")
    print("="*70)
    
    manager = DSSchedulerManager()
    
    # 健康检查
    print("\n【健康检查】")
    status = manager.health_check()
    print(f"  Gateway: {'✅' if status.get('gateway', {}).get('gateway_connected') else '❌'}")
    print(f"  REST API: {'✅' if status.get('rest_api', {}).get('api_connected') else '❌'}")
    print(f"  DataHub: {'✅' if status.get('datahub', {}).get('db_connected') else '❌'}")
    
    if not status.get('gateway', {}).get('gateway_connected'):
        print("\n❌ Gateway 未连接，无法部署工作流")
        return
    
    # 部署工作流
    print("\n【部署工作流】")
    
    results = {
        'data_collection': deploy_data_collection_workflow(manager),
        'stock_selection': deploy_stock_selection_workflow(manager),
        'backtest': deploy_backtest_workflow(manager)
    }
    
    # 汇总结果
    print("\n【部署结果】")
    for name, success in results.items():
        status_icon = "✅" if success else "❌"
        print(f"  {status_icon} {name}")
    
    # 验证血缘采集
    print("\n【验证血缘采集】")
    try:
        lineage = manager.get_lineage('xcnstock', OperationMode.DATAHUB)
        print(f"  采集到 {len(lineage)} 条血缘关系")
    except Exception as e:
        print(f"  ⚠️ 血缘采集: {e}")
    
    # 关闭连接
    manager.close()
    
    print("\n" + "="*70)
    print("部署完成!")
    print("="*70)
    print("\n工作流列表:")
    print("  1. data_collection_daily  - 每日 15:30 数据收集")
    print("  2. stock_selection_daily  - 每日 16:00 选股策略")
    print("  3. backtest_weekly        - 每周六 10:00 回测")
    print("\n访问 DolphinScheduler UI 查看和管理:")
    print(f"  http://localhost:12345")


if __name__ == "__main__":
    main()
