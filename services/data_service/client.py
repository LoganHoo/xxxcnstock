#!/usr/bin/env python3
"""
数据服务客户端
用于scripts目录下的脚本调用微服务API
"""
import requests
import time
from typing import Optional, Dict, Any
from pathlib import Path

from core.config import get_settings
from core.logger import setup_logger

logger = setup_logger("data_service_client", log_file="system/data_client.log")


class DataServiceClient:
    """数据服务客户端"""
    
    def __init__(self, base_url: str = None):
        settings = get_settings()
        self.base_url = base_url or f"http://localhost:{settings.DATA_SERVICE_PORT}"
        self.timeout = 30
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """发送HTTP请求"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError:
            logger.error(f"无法连接到数据服务: {url}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"请求超时: {url}")
            return None
        except Exception as e:
            logger.error(f"请求失败: {url}, {e}")
            return None
    
    def health_check(self) -> bool:
        """检查服务健康状态"""
        result = self._request("GET", "/health")
        return result is not None and result.get("status") == "ok"
    
    # ========== 数据采集接口 ==========
    
    def collect_stock_list(self) -> bool:
        """触发股票列表采集"""
        logger.info("请求采集股票列表...")
        result = self._request("POST", "/api/v1/collect/stock_list")
        if result and result.get("status") == "started":
            logger.info("股票列表采集任务已启动")
            return True
        return False
    
    def collect_fundamental(self) -> bool:
        """触发基本面数据采集"""
        logger.info("请求采集基本面数据...")
        result = self._request("POST", "/api/v1/collect/fundamental")
        if result and result.get("status") == "started":
            logger.info("基本面数据采集任务已启动")
            return True
        return False
    
    def collect_kline(self, codes: list = None) -> bool:
        """触发K线数据采集"""
        logger.info("请求采集K线数据...")
        data = {"codes": codes} if codes else {}
        result = self._request("POST", "/api/v1/collect/kline", json=data)
        if result and result.get("status") == "started":
            logger.info("K线数据采集任务已启动")
            return True
        return False
    
    def collect_all(self) -> bool:
        """触发完整采集流程"""
        logger.info("请求执行完整采集流程...")
        result = self._request("POST", "/api/v1/collect/all")
        if result and result.get("status") == "started":
            logger.info("完整采集流程已启动")
            return True
        return False
    
    # ========== 数据查询接口 ==========
    
    def get_stock_list(self) -> Optional[Dict]:
        """获取股票列表"""
        return self._request("GET", "/api/v1/stock_list")
    
    def get_fundamental(self, code: str) -> Optional[Dict]:
        """获取单只股票基本面数据"""
        return self._request("GET", f"/api/v1/fundamental/{code}")
    
    def get_realtime_quotes(self) -> Optional[Dict]:
        """获取实时行情"""
        return self._request("GET", "/api/v1/quote/realtime")
    
    def get_kline(self, code: str) -> Optional[Dict]:
        """获取K线数据"""
        return self._request("GET", f"/api/v1/quote/kline/{code}")
    
    def get_limit_up(self) -> Optional[Dict]:
        """获取涨停池"""
        return self._request("GET", "/api/v1/limitup/today")
    
    def get_scheduler_jobs(self) -> Optional[Dict]:
        """获取调度任务状态"""
        return self._request("GET", "/api/v1/scheduler/jobs")


# ========== 便捷函数 ==========

def get_client() -> DataServiceClient:
    """获取客户端实例"""
    return DataServiceClient()


def collect_stock_list() -> bool:
    """采集股票列表（便捷函数）"""
    return get_client().collect_stock_list()


def collect_fundamental() -> bool:
    """采集基本面数据（便捷函数）"""
    return get_client().collect_fundamental()


def collect_kline(codes: list = None) -> bool:
    """采集K线数据（便捷函数）"""
    return get_client().collect_kline(codes)


def collect_all() -> bool:
    """执行完整采集（便捷函数）"""
    return get_client().collect_all()


def check_service() -> bool:
    """检查服务是否运行"""
    return get_client().health_check()


if __name__ == "__main__":
    # 测试客户端
    client = DataServiceClient()
    
    print("检查服务状态...")
    if client.health_check():
        print("✅ 数据服务运行正常")
        
        print("\n获取股票列表...")
        result = client.get_stock_list()
        if result:
            print(f"✅ 获取到 {result.get('count', 0)} 只股票")
        
        print("\n获取实时行情...")
        result = client.get_realtime_quotes()
        if result:
            print(f"✅ 获取到 {result.get('count', 0)} 条行情")
        
        print("\n获取涨停池...")
        result = client.get_limit_up()
        if result:
            print(f"✅ 获取到 {result.get('count', 0)} 只涨停股")
    else:
        print("❌ 数据服务未运行")
        print("请先启动服务: python services/data_service/main.py")
