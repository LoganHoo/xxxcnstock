#!/usr/bin/env python3
"""
DolphinScheduler REST API 客户端

通过 REST API (12345) 触发工作流执行
"""
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from typing import Dict, Any, Optional, List
from datetime import datetime
import json

try:
    import httpx
    HTTP_CLIENT = 'httpx'
except ImportError:
    import requests
    HTTP_CLIENT = 'requests'

from core.logger import setup_logger


class DSRestClient:
    """DolphinScheduler REST API 客户端"""
    
    def __init__(self):
        """初始化 REST API 客户端"""
        self.logger = setup_logger("ds_rest_client")
        
        # 配置
        self.base_url = os.getenv('DOLPHINSCHEDULER_URL', 'http://localhost:12345')
        self.user = os.getenv('DOLPHINSCHEDULER_USER', 'admin')
        self.password = os.getenv('DOLPHINSCHEDULER_PASSWORD', 'dolphinscheduler123')
        
        # 确保 URL 格式正确
        if not self.base_url.endswith('/dolphinscheduler'):
            self.api_base = f"{self.base_url}/dolphinscheduler"
        else:
            self.api_base = self.base_url
        
        self.logger.info(f"REST API 配置: {self.api_base}")
        self.logger.info(f"用户: {self.user}")
        
        # 会话 token
        self.session_token = None
        
        # HTTP 客户端
        self.client = None
        self._init_client()
    
    def _init_client(self):
        """初始化 HTTP 客户端"""
        if HTTP_CLIENT == 'httpx':
            self.client = httpx.Client(timeout=30.0)
        else:
            self.client = requests.Session()
        self.logger.info(f"使用 HTTP 客户端: {HTTP_CLIENT}")
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """发送 HTTP 请求"""
        url = f"{self.api_base}{endpoint}"
        
        try:
            if HTTP_CLIENT == 'httpx':
                response = self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            else:
                response = self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            self.logger.error(f"请求失败 {url}: {e}")
            return {'success': False, 'error': str(e)}
    
    def login(self) -> bool:
        """用户登录获取 session"""
        try:
            self.logger.info(f"登录用户: {self.user}")
            
            data = {
                'userName': self.user,
                'userPassword': self.password
            }
            
            result = self._request('POST', '/login', data=data)
            
            if result.get('success') or result.get('code') == 0:
                self.session_token = result.get('data', {}).get('sessionId')
                self.logger.info("✅ 登录成功")
                return True
            else:
                self.logger.error(f"❌ 登录失败: {result}")
                return False
                
        except Exception as e:
            self.logger.error(f"登录异常: {e}")
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        status = {
            'api_url': self.api_base,
            'user': self.user,
            'timestamp': datetime.now().isoformat(),
            'api_connected': False
        }
        
        try:
            # 尝试访问登录接口
            result = self._request('GET', '/login')
            status['api_connected'] = True
            status['login_page'] = 'accessible'
            self.logger.info("✅ REST API 可访问")
        except Exception as e:
            status['error'] = str(e)
            self.logger.error(f"❌ REST API 不可访问: {e}")
        
        return status
    
    def get_project_list(self) -> List[Dict]:
        """获取项目列表"""
        result = self._request('GET', '/projects')
        if result.get('success'):
            return result.get('data', {}).get('totalList', [])
        return []
    
    def get_process_list(self, project_code: str) -> List[Dict]:
        """获取工作流列表"""
        result = self._request(
            'GET', 
            f'/projects/{project_code}/process-definition',
            params={'pageSize': 100}
        )
        if result.get('success'):
            return result.get('data', {}).get('totalList', [])
        return []
    
    def start_process_instance(
        self,
        project_code: str,
        process_definition_code: str,
        run_mode: str = 'RUN_MODE_SERIAL',
        warning_type: str = 'NONE'
    ) -> Dict[str, Any]:
        """
        启动工作流实例
        
        Args:
            project_code: 项目编码
            process_definition_code: 工作流定义编码
            run_mode: 运行模式 (RUN_MODE_SERIAL / RUN_MODE_PARALLEL)
            warning_type: 告警类型
        
        Returns:
            启动结果
        """
        try:
            self.logger.info(f"启动工作流: {process_definition_code}")
            
            data = {
                'processDefinitionCode': process_definition_code,
                'runMode': run_mode,
                'warningType': warning_type,
                'warningGroupId': 0,
                'execType': 'START_PROCESS',
                'startParamList': [],
                'commandParam': ''
            }
            
            result = self._request(
                'POST',
                f'/projects/{project_code}/executors/start-process-instance',
                json=data
            )
            
            if result.get('success') or result.get('code') == 0:
                self.logger.info(f"✅ 工作流启动成功")
                return {
                    'success': True,
                    'data': result.get('data'),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                self.logger.error(f"❌ 启动失败: {result}")
                return {
                    'success': False,
                    'error': result.get('msg'),
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"启动异常: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_process_instance_list(
        self,
        project_code: str,
        process_definition_code: str = None
    ) -> List[Dict]:
        """获取工作流实例列表"""
        params = {'pageSize': 100}
        if process_definition_code:
            params['processDefinitionCode'] = process_definition_code
        
        result = self._request(
            'GET',
            f'/projects/{project_code}/process-instances',
            params=params
        )
        
        if result.get('success'):
            return result.get('data', {}).get('totalList', [])
        return []
    
    def get_task_instance_list(
        self,
        project_code: str,
        process_instance_id: str
    ) -> List[Dict]:
        """获取任务实例列表"""
        result = self._request(
            'GET',
            f'/projects/{project_code}/task-instances',
            params={'processInstanceId': process_instance_id}
        )
        
        if result.get('success'):
            return result.get('data', {}).get('totalList', [])
        return []
    
    def trigger_workflow_by_name(
        self,
        project_name: str,
        workflow_name: str
    ) -> Dict[str, Any]:
        """
        通过名称触发工作流 (便捷方法)
        
        Args:
            project_name: 项目名称
            workflow_name: 工作流名称
        
        Returns:
            触发结果
        """
        try:
            # 1. 查找项目
            projects = self.get_project_list()
            project = None
            for p in projects:
                if p.get('name') == project_name:
                    project = p
                    break
            
            if not project:
                return {
                    'success': False,
                    'error': f'项目不存在: {project_name}',
                    'timestamp': datetime.now().isoformat()
                }
            
            project_code = project.get('code')
            
            # 2. 查找工作流
            processes = self.get_process_list(project_code)
            process = None
            for p in processes:
                if p.get('name') == workflow_name:
                    process = p
                    break
            
            if not process:
                return {
                    'success': False,
                    'error': f'工作流不存在: {workflow_name}',
                    'timestamp': datetime.now().isoformat()
                }
            
            process_code = process.get('code')
            
            # 3. 启动工作流
            return self.start_process_instance(project_code, process_code)
            
        except Exception as e:
            self.logger.error(f"触发工作流异常: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def close(self):
        """关闭客户端"""
        if self.client:
            if HTTP_CLIENT == 'httpx':
                self.client.close()
            else:
                self.client.close()
        self.logger.info("REST API 客户端已关闭")


def test_rest_api():
    """测试 REST API"""
    print("="*70)
    print("DolphinScheduler REST API 测试")
    print("="*70)
    
    client = DSRestClient()
    
    # 健康检查
    status = client.health_check()
    print(f"\nAPI 状态:")
    print(f"  URL: {status['api_url']}")
    print(f"  连接状态: {'✅ 成功' if status['api_connected'] else '❌ 失败'}")
    
    if status['api_connected']:
        # 尝试登录
        logged_in = client.login()
        if logged_in:
            print("  登录状态: ✅ 成功")
            
            # 获取项目列表
            projects = client.get_project_list()
            print(f"\n项目列表 ({len(projects)} 个):")
            for p in projects:
                print(f"  - {p.get('name')} (code: {p.get('code')})")
        else:
            print("  登录状态: ❌ 失败")
    
    client.close()
    return status['api_connected']


if __name__ == "__main__":
    test_rest_api()
