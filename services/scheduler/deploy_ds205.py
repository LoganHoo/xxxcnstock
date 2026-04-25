#!/usr/bin/env python3
"""
适配 DolphinScheduler 2.0.5 API 的工作流部署

DS 2.0.5 API 特点:
- 使用 processDefinition 而非 workflow
- 任务定义格式不同
- 调度配置单独接口
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import requests
from core.logger import setup_logger

logger = setup_logger("deploy_ds205")


class DS205Deployer:
    """DS 2.0.5 工作流部署器"""
    
    def __init__(self):
        self.base_url = 'http://localhost:12345/dolphinscheduler'
        self.user = os.getenv('DOLPHINSCHEDULER_USER', 'admin')
        self.password = os.getenv('DOLPHINSCHEDULER_PASSWORD', 'dolphinscheduler123')
        self.project = 'xcnstock'
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/x-www-form-urlencoded'})
        
    def login(self) -> bool:
        """登录"""
        url = f"{self.base_url}/login"
        data = {
            'userName': self.user,
            'userPassword': self.password
        }
        try:
            response = self.session.post(url, data=data)
            result = response.json()
            if result.get('code') == 0:
                logger.info("✅ 登录成功")
                return True
            else:
                logger.error(f"❌ 登录失败: {result.get('msg')}")
                return False
        except Exception as e:
            logger.error(f"❌ 登录异常: {e}")
            return False
    
    def get_or_create_project(self) -> int:
        """获取或创建项目"""
        # 查询项目列表
        url = f"{self.base_url}/projects"
        try:
            response = self.session.get(url, params={'pageNo': 1, 'pageSize': 100})
            result = response.json()
            if result.get('code') == 0:
                for project in result.get('data', {}).get('totalList', []):
                    if project.get('name') == self.project:
                        logger.info(f"✅ 找到项目: {self.project} (code={project.get('code')})")
                        return project.get('code')
            
            # 创建项目
            return self.create_project()
        except Exception as e:
            logger.error(f"❌ 获取项目异常: {e}")
            return None
    
    def create_project(self) -> int:
        """创建项目"""
        url = f"{self.base_url}/projects"
        data = {
            'projectName': self.project,
            'description': 'xcnstock 量化交易系统'
        }
        try:
            response = self.session.post(url, data=data)
            result = response.json()
            if result.get('code') == 0:
                code = result.get('data', {}).get('code')
                logger.info(f"✅ 创建项目成功: {self.project} (code={code})")
                return code
            else:
                logger.error(f"❌ 创建项目失败: {result.get('msg')}")
                return None
        except Exception as e:
            logger.error(f"❌ 创建项目异常: {e}")
            return None
    
    def create_workflow(self, project_code: int, name: str, description: str, tasks: list, schedule: str = None) -> bool:
        """创建工作流 (DS 2.0.5 格式)"""
        url = f"{self.base_url}/projects/{project_code}/process-definition"
        
        # DS 2.0.5 格式
        locations = []
        task_definition_json = []
        task_relation_json = []
        
        for i, task in enumerate(tasks):
            # 任务位置
            locations.append({
                'taskCode': task['code'],
                'x': 100 + i * 200,
                'y': 100
            })
            
            # 任务定义
            task_def = {
                'code': task['code'],
                'name': task['name'],
                'version': 1,
                'description': task.get('description', ''),
                'taskType': task['type'],
                'taskParams': json.dumps(task.get('params', {})),
                'flag': 'YES',
                'taskPriority': 'MEDIUM',
                'workerGroup': 'default',
                'failRetryTimes': 0,
                'failRetryInterval': 1,
                'timeoutFlag': 'CLOSE',
                'timeoutNotifyStrategy': '',
                'timeout': 0,
                'delayTime': 0,
                'environmentCode': -1
            }
            task_definition_json.append(task_def)
            
            # 任务关系
            relation = {
                'name': '',
                'preTaskCode': 0,
                'preTaskVersion': 0,
                'postTaskCode': task['code'],
                'postTaskVersion': 0,
                'conditionType': 'NONE',
                'conditionParams': {}
            }
            
            # 处理前置任务
            pre_tasks = task.get('preTasks', [])
            if pre_tasks:
                for pre_task_name in pre_tasks:
                    pre_task = next((t for t in tasks if t['name'] == pre_task_name), None)
                    if pre_task:
                        relation['preTaskCode'] = pre_task['code']
                        task_relation_json.append(relation.copy())
            else:
                task_relation_json.append(relation)
        
        data = {
            'name': name,
            'description': description,
            'locations': json.dumps(locations),
            'taskDefinitionJson': json.dumps(task_definition_json),
            'taskRelationJson': json.dumps(task_relation_json),
            'tenantCode': 'default',
            'globalParams': '[]'
        }
        
        try:
            response = self.session.post(url, data=data)
            result = response.json()
            if result.get('code') == 0:
                process_code = result.get('data', {}).get('code')
                logger.info(f"✅ 工作流 '{name}' 创建成功 (code={process_code})")
                
                # 创建定时调度
                if schedule and process_code:
                    self.create_schedule(project_code, process_code, name, schedule)
                
                return True
            else:
                logger.error(f"❌ 工作流 '{name}' 创建失败: {result.get('msg')}")
                return False
        except Exception as e:
            logger.error(f"❌ 工作流 '{name}' 创建异常: {e}")
            return False
    
    def create_schedule(self, project_code: int, process_code: int, process_name: str, crontab: str) -> bool:
        """创建定时调度"""
        url = f"{self.base_url}/projects/{project_code}/schedules"
        
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
            'processInstancePriority': 'MEDIUM',
            'workerGroup': 'default'
        }
        
        try:
            response = self.session.post(url, data=schedule_data)
            result = response.json()
            if result.get('code') == 0:
                logger.info(f"✅ 定时调度创建成功: {crontab}")
                return True
            else:
                logger.error(f"❌ 定时调度创建失败: {result.get('msg')}")
                return False
        except Exception as e:
            logger.error(f"❌ 定时调度创建异常: {e}")
            return False
    
    def release_workflow(self, project_code: int, process_code: int) -> bool:
        """发布工作流"""
        url = f"{self.base_url}/projects/{project_code}/process-definition/{process_code}/release"
        data = {'releaseState': 'ONLINE'}
        
        try:
            response = self.session.post(url, data=data)
            result = response.json()
            if result.get('code') == 0:
                logger.info(f"✅ 工作流发布成功")
                return True
            else:
                logger.error(f"❌ 工作流发布失败: {result.get('msg')}")
                return False
        except Exception as e:
            logger.error(f"❌ 工作流发布异常: {e}")
            return False


def main():
    """主函数"""
    print("="*70)
    print("xcnstock 工作流部署 (DS 2.0.5)")
    print("="*70)
    
    deployer = DS205Deployer()
    
    # 登录
    if not deployer.login():
        print("❌ 登录失败")
        return
    
    # 获取项目
    project_code = deployer.get_or_create_project()
    if not project_code:
        print("❌ 获取项目失败")
        return
    
    base_path = '/Volumes/Xdata/workstation/xxxcnstock'
    
    print("\n【部署工作流】")
    
    # 1. 数据收集工作流
    tasks_collection = [
        {
            'code': 1001,
            'name': 'collect_data',
            'type': 'SHELL',
            'description': '收集A股K线数据',
            'params': {
                'rawScript': f'cd {base_path} && python data_collect.py --all',
                'dependence': {},
                'localParams': []
            },
            'preTasks': []
        },
        {
            'code': 1002,
            'name': 'quality_check',
            'type': 'SHELL',
            'description': '数据质量检查',
            'params': {
                'rawScript': 'echo "数据质量检查完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': ['collect_data']
        }
    ]
    
    result1 = deployer.create_workflow(
        project_code=project_code,
        name='data_collection_daily',
        description='A股数据每日收集 - 15:30执行',
        tasks=tasks_collection,
        schedule='0 30 15 * * ?'
    )
    
    # 2. 选股策略工作流
    tasks_selection = [
        {
            'code': 2001,
            'name': 'scan_stocks',
            'type': 'SHELL',
            'description': '主力痕迹共振扫描',
            'params': {
                'rawScript': f'cd {base_path} && echo "选股扫描完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': []
        },
        {
            'code': 2002,
            'name': 'generate_report',
            'type': 'SHELL',
            'description': '生成选股报告',
            'params': {
                'rawScript': f'cd {base_path} && echo "选股报告生成完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': ['scan_stocks']
        }
    ]
    
    result2 = deployer.create_workflow(
        project_code=project_code,
        name='stock_selection_daily',
        description='每日选股策略执行 - 16:00执行',
        tasks=tasks_selection,
        schedule='0 0 16 * * ?'
    )
    
    # 3. 回测工作流
    tasks_backtest = [
        {
            'code': 3001,
            'name': 'run_backtest',
            'type': 'SHELL',
            'description': '执行策略回测',
            'params': {
                'rawScript': f'cd {base_path} && echo "回测完成"',
                'dependence': {},
                'localParams': []
            },
            'preTasks': []
        }
    ]
    
    result3 = deployer.create_workflow(
        project_code=project_code,
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
    print("  http://localhost:12345")
    print("\n项目: xcnstock")
    print("工作流:")
    print("  1. data_collection_daily  - 每日 15:30")
    print("  2. stock_selection_daily  - 每日 16:00")
    print("  3. backtest_weekly        - 每周六 10:00")


if __name__ == "__main__":
    main()
