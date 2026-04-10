"""
数据版本管理器
用于锁定和管理每日数据的版本，确保后续任务使用一致的数据
"""
import os
import json
import redis
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict


class DataVersionManager:
    """数据版本管理器"""
    
    def __init__(self):
        self.redis_client = self._get_redis()
        self.project_root = Path(__file__).parent.parent
    
    def _get_redis(self):
        """获取Redis连接"""
        try:
            redis_host = os.getenv('REDIS_HOST', '49.233.10.199')
            redis_port = int(os.getenv('REDIS_PORT', '6379'))
            redis_password = os.getenv('REDIS_PASSWORD', '100200')
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=0,
                socket_timeout=5,
                decode_responses=True
            )
            client.ping()
            return client
        except Exception as e:
            print(f"Redis连接失败: {e}")
            return None
    
    def _get_today_key(self) -> str:
        """获取今日数据版本Key"""
        today = datetime.now().strftime('%Y%m%d')
        return f"data:version:{today}"
    
    def lock_version(self, trade_date: str, stock_count: int, quality_passed: bool = True) -> bool:
        """
        锁定数据版本
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)
            stock_count: 股票数量
            quality_passed: 质检是否通过
            
        Returns:
            bool: 是否锁定成功
        """
        version_data = {
            "version": "v1",
            "trade_date": trade_date,
            "locked_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "quality_passed": "true" if quality_passed else "false",
            "stock_count": str(stock_count),
            "status": "locked" if quality_passed else "pending"
        }
        
        if self.redis_client:
            try:
                key = self._get_today_key()
                self.redis_client.hmset(key, version_data)
                self.redis_client.expire(key, 86400 * 3)
                print(f"✅ 数据版本已锁定: {version_data}")
                return True
            except Exception as e:
                print(f"Redis锁定失败: {e}")
                return False
        else:
            self._save_to_file(version_data)
            return True
    
    def get_locked_version(self) -> Optional[Dict]:
        """
        获取已锁定的数据版本
        
        Returns:
            Dict: 版本信息，如果未锁定返回None
        """
        if self.redis_client:
            try:
                key = self._get_today_key()
                data = self.redis_client.hgetall(key)
                if data and data.get('status') == 'locked':
                    return data
            except Exception as e:
                print(f"获取锁定版本失败: {e}")
        
        return self._load_from_file()
    
    def is_version_locked(self) -> bool:
        """检查今日数据版本是否已锁定"""
        version = self.get_locked_version()
        return version is not None and version.get('status') == 'locked'
    
    def get_trade_date(self) -> Optional[str]:
        """获取当前锁定的交易日期"""
        version = self.get_locked_version()
        return version.get('trade_date') if version else None
    
    def _save_to_file(self, data: Dict):
        """保存到文件（Redis不可用时备用）"""
        version_file = self.project_root / "data" / "version.json"
        version_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(version_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _load_from_file(self) -> Optional[Dict]:
        """从文件加载版本信息"""
        version_file = self.project_root / "data" / "version.json"
        
        if version_file.exists():
            try:
                with open(version_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get('status') == 'locked':
                        return data
            except Exception as e:
                print(f"读取版本文件失败: {e}")
        
        return None


def get_version_manager() -> DataVersionManager:
    """获取数据版本管理器实例"""
    return DataVersionManager()
