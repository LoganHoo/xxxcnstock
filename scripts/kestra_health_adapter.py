#!/usr/bin/env python3
"""
Kestra 健康检查适配器
用于双调度器架构中的Kestra状态上报
"""

import sys
import os
import json
import time
import redis
import requests
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KestraHealthAdapter:
    """Kestra健康检查适配器"""
    
    REDIS_KEY_PREFIX = "xcnstock:scheduler:"
    HEARTBEAT_KEY = f"{REDIS_KEY_PREFIX}heartbeat"
    
    def __init__(self):
        self.config = self._load_config()
        self.redis_client = self._init_redis()
        self.scheduler_name = f"kestra_{os.getpid()}"
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置"""
        import yaml
        config_path = project_root / 'config' / 'dual_scheduler.yaml'
        
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        
        return {
            'kestra': {
                'api_url': 'http://localhost:8082/api/v1',
                'timeout': 5
            },
            'redis': {
                'host': 'localhost',
                'port': 6379,
                'db': 0
            }
        }
    
    def _init_redis(self) -> Optional[redis.Redis]:
        """初始化Redis"""
        try:
            redis_config = self.config.get('redis', {})
            client = redis.Redis(
                host=redis_config.get('host', 'localhost'),
                port=redis_config.get('port', 6379),
                db=redis_config.get('db', 0),
                decode_responses=True
            )
            client.ping()
            return client
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            return None
    
    def check_kestra_health(self) -> Dict[str, Any]:
        """检查Kestra健康状态"""
        kestra_config = self.config.get('kestra', {})
        api_url = kestra_config.get('api_url', 'http://localhost:8082/api/v1')
        timeout = kestra_config.get('timeout', 5)
        
        try:
            # 检查API
            response = requests.get(
                f"{api_url}/health",
                timeout=timeout
            )
            
            if response.status_code == 200:
                status = "healthy"
            else:
                status = "degraded"
            
            # 获取工作流列表
            try:
                flows_response = requests.get(
                    f"{api_url}/namespaces/xcnstock/flows",
                    timeout=timeout
                )
                flow_count = len(flows_response.json()) if flows_response.status_code == 200 else 0
            except:
                flow_count = 0
            
            return {
                'status': status,
                'api_reachable': True,
                'flow_count': flow_count,
                'last_check': datetime.now().isoformat()
            }
        
        except requests.exceptions.ConnectionError:
            return {
                'status': 'down',
                'api_reachable': False,
                'flow_count': 0,
                'last_check': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"健康检查异常: {e}")
            return {
                'status': 'unknown',
                'api_reachable': False,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
    
    def report_health(self):
        """上报健康状态"""
        if not self.redis_client:
            logger.error("Redis未连接，无法上报状态")
            return
        
        health_data = self.check_kestra_health()
        
        state = {
            'name': self.scheduler_name,
            'type': 'kestra',
            'role': 'unknown',  # 由双调度器管理器确定
            'status': health_data['status'],
            'last_heartbeat': datetime.now().isoformat(),
            'active_tasks': health_data.get('flow_count', 0),
            'health_details': health_data,
            'is_leader': False  # 由双调度器管理器确定
        }
        
        try:
            self.redis_client.hset(
                self.HEARTBEAT_KEY,
                self.scheduler_name,
                json.dumps(state)
            )
            self.redis_client.expire(self.HEARTBEAT_KEY, 120)
            logger.info(f"✅ 健康状态已上报: {health_data['status']}")
        except Exception as e:
            logger.error(f"上报健康状态失败: {e}")
    
    def run(self, interval: int = 30):
        """持续运行健康检查"""
        logger.info(f"Kestra健康适配器已启动，上报间隔: {interval}秒")
        
        while True:
            try:
                self.report_health()
            except Exception as e:
                logger.error(f"运行异常: {e}")
            
            time.sleep(interval)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kestra健康检查适配器')
    parser.add_argument(
        '--interval',
        type=int,
        default=30,
        help='健康检查间隔(秒)'
    )
    
    args = parser.parse_args()
    
    adapter = KestraHealthAdapter()
    adapter.run(interval=args.interval)


if __name__ == '__main__':
    sys.exit(main())
