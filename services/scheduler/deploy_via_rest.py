#!/usr/bin/env python3
"""
通过 REST API 部署 xcnstock 工作流到 DolphinScheduler

直接调用 DS REST API 创建工作流定义
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import requests
from core.logger import setup_logger

logger = setup_logger("deploy_via_rest")


class DSRestWorkflowDeployer:
    """通过 REST API 部署工作流"""
    
    def __init__(self):
        self.base_url = os.getenv('DOLPHINSCHEDULER_URL', 'http://localhost:12345')
        self.user = os.getenv('DOLPHINSCHEDULER_USER', 'admin')
        self.password = os.getenv('DOLPHINSCHEDULER_PASSWORD', 'dolphinscheduler123')
        self.project = os.getenv('DOLPHINSCHEDULER_PROJECT', 'xcnstock')
        self.session = requests.Session()
        self.token = None
        self.project_code = None
        
    def login(self) -> bool:
        """登录获取 token"""
        url = f"{self.base_url}/dolphinscheduler/login"
        data = {
            'userName': self.user,
            'userPassword': self.password
        }
        try:
            response = self.session.post(url, data=data)
            result = response.json()
            if result.get('code') == 0:
                self.token = result.get('data', {}).get('sessionId')
                logger.info("✅ 登录成功")
                return True
            else:
                logger.error(f"❌ 登录失败: {result}")
                return False
        except Exception as e:
            logger.error(f"❌ 登录异常: {e}")
            return False
    
    def get_project_code(self) -> str:
        """获取项目 code"""
        url = f"{self.base_url}/dolphinscheduler/projects"
        try:
            response = self.session.get(url)
            result = response.json()
            if result.get('code') == 0:
                for project in result.get('data', {}).get('totalList', []):
                    if project.get('name') == self.project:
                        self.project_code = project.get('code')
                        logger.info(f"✅ 获取项目 code: {self.project_code}")
                        return self.project_code
                # 项目不存在，创建
                return self.create_project()
            else:
                logger.error(f"❌ 获取项目失败: {result}")
                return None
        except Exception as e:
            logger.error(f"❌ 获取项目异常: {e}")
            return None
    
    def create_project(self) -> str:
        """创建项目"""
        url = f"{self.base_url}/dolphinscheduler/projects"
        data = {
            'projectName': self.project,
            'description': 'xcnstock 量化交易系统'
        }
        try:
            response = self.session.post(url, data=data)
            result = response.json()
            if result.get('code') == 0:
                self.project_code = result.get('data', {}).get('code')
                logger.info(f"✅ 创建项目成功: {self.project_code}")
                return self.project_code
            else:
                logger.error(f"❌ 创建项目失败: {result}")
                return None
        except Exception as e:
            logger.error(f"❌ 创建项目异常: {e}")
            return None
    
    def create_workflow(self, name: str, description: str, tasks: list, schedule: str = None) -> bool:
        """创建工作流"""
        if not self.project_code:
            logger.error("❌ 项目 code 未获取")
            return False
        
        url = f"{self.base_url}/dolphinscheduler/projects/{self.project_code}/process-definition"
        
        # 构建工作流定义
        process_definition = {
            'name': name,
            'description': description,
            'globalParams': [],
            'tasks': tasks,
            'taskDefinitionJson': json.dumps(tasks),
            'taskRelationJson': json.dumps(self._build_task_relations(tasks)),
            'tenantCode': 'default',
            'timeout': 0
        }
        
        try:
            response = self.session.post(url, data=process_definition)
            result = response.json()
            if result.get('code') == 0:
                logger.info(f"✅ 工作流 '{name}' 创建成功")
                
                # 如果设置了定时调度
                if schedule:
                    process_code = result.get('data', {}).get('code')
                    self.create_schedule(process_code, name, schedule)
                
                return True
            else:
                logger.error(f"❌ 工作流 '{name}' 创建失败: {result}")
                return False
        except Exception as e:
            logger.error(f"❌ 工作流 '{name}' 创建异常: {e}")
            return False
    
    def _build_task_relations(self, tasks: list) -> list:
        """构建任务关系"""
        relations = []
        for i, task in enumerate(tasks):
            relation = {
                'name': task.get('name'),
                'preTaskVersion': [],
                'postTaskVersion': [],
                'preTasks': task.get('preTasks', []),
                'conditionResult': {'successNode': [], 'failedNode': []}
            }
            relations.append(relation)
        return relations
    
    def create_schedule(self, process_code: str, process_name: str, crontab: str) -> bool:
        """创建定时调度"""
        url = f"{self.base_url}/dolphinscheduler/projects/{self.project_code}/schedules"
        
        schedule_data = {
            'processDefinitionCode': process_code,
            'processDefinitionName': process_name,
            'crontab': crontab,
            'timezoneId': 'Asia/Shanghai',
            'warningType': 'NONE',
            'warningGroupId': 0,
            'startTime': '',
            'endTime': '',
            'failureStrategy': 'END',
            'processInstancePriority': 'MEDIUM'
        }
        
        try:
            response = self.session.post(url, data=schedule_data)
            result = response.json()
            if result.get('code') == 0:
                logger.info(f"✅ 定时调度创建成功: {crontab}")
                return True
            else:
                logger.error(f"❌ 定时调度创建失败: {result}")
                return False
        except Exception as e:
            logger.error(f"❌ 定时调度创建异常: {e}")
            return False


def main():
    """主函数"""
    print("="*70)
    print("xcnstock 工作流部署 (REST API)")
    print("="*70)
    
    deployer = DSRestWorkflowDeployer()
    
    # 登录
    if not deployer.login():
        print("❌ 登录失败")
        return
    
    # 获取项目 code
    if not deployer.get_project_code():
        print("❌ 获取项目失败")
        return
    
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    print("\n【部署工作流】")
    
    # 1. 数据收集工作流
    tasks_collection = [
        {
            'id': 'collect_data',
            'name': 'collect_data',
            'type': 'SHELL',
            'description': '收集A股K线数据',
            'taskParams': {
                'rawScript': f'cd {base_path} && python data_collect.py --all',
                'dependence': {},
                'localParams': []
            },
            'preTasks': [],
            'timeout': {'enable': False}
        },
        {
            'id': 'quality_check',
            'name': 'quality_check',
            'type': 'SHELL',
            'description': '数据质量检查',
            'taskParams': {
                'rawScript': 'echo "数据质量检查完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': ['collect_data'],
            'timeout': {'enable': False}
        }
    ]
    
    result1 = deployer.create_workflow(
        name='data_collection_daily',
        description='A股数据每日收集 - 15:30执行',
        tasks=tasks_collection,
        schedule='0 30 15 * * ?'
    )
    
    # 2. 选股策略工作流
    tasks_selection = [
        {
            'id': 'scan_stocks',
            'name': 'scan_stocks',
            'type': 'SHELL',
            'description': '主力痕迹共振扫描',
            'taskParams': {
                'rawScript': f'cd {base_path} && echo "选股扫描完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': [],
            'timeout': {'enable': False}
        },
        {
            'id': 'generate_report',
            'name': 'generate_report',
            'type': 'SHELL',
            'description': '生成选股报告',
            'taskParams': {
                'rawScript': f'cd {base_path} && echo "选股报告生成完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': ['scan_stocks'],
            'timeout': {'enable': False}
        }
    ]
    
    result2 = deployer.create_workflow(
        name='stock_selection_daily',
        description='每日选股策略执行 - 16:00执行',
        tasks=tasks_selection,
        schedule='0 0 16 * * ?'
    )
    
    # 3. 回测工作流
    tasks_backtest = [
        {
            'id': 'run_backtest',
            'name': 'run_backtest',
            'type': 'SHELL',
            'description': '执行策略回测',
            'taskParams': {
                'rawScript': f'cd {base_path} && echo "回测完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': [],
            'timeout': {'enable': False}
        }
    ]
    
    result3 = deployer.create_workflow(
        name='backtest_weekly',
        description='每周回测验证 - 周六10:00执行',
        tasks=tasks_backtest,
        schedule='0 0 10 ? * 7'
    )
    
    # 汇总结果
    print("\n【部署结果】")
    results = {
        'data_collection_daily': result1,
        'stock_selection_daily': result2,
        'backtest_weekly': result3
    }
    
    for name, success in results.items():
        status_icon = "✅" if success else "❌"
        print(f"  {status_icon} {name}")
    
    print("\n" + "="*70)
    print("部署完成!")
    print("="*70)
    print("\n访问 DolphinScheduler UI:")
    print(f"  {deployer.base_url}")


if __name__ == "__main__":
    main()
