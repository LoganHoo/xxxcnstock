"""数据采集任务 - 15:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class DataCollectTask(BaseTask):
    """数据采集任务"""
    
    name = "data_collect"
    description = "采集实时行情和K线数据"
    
    def run(self) -> bool:
        try:
            # 1. 采集实时行情
            self._fetch_realtime()
            
            # 2. 采集K线数据
            self._fetch_klines()
            
            return True
        except Exception as e:
            self.logger.error(f"数据采集失败: {e}")
            return False
    
    def _fetch_realtime(self):
        """采集实时行情"""
        import requests
        
        self.logger.info("开始采集实时行情...")
        
        url = "http://82.102.73.198/realtime"
        resp = requests.get(url, timeout=30)
        data = resp.json()
        
        if data.get("code") == 200:
            df = pd.DataFrame(data["data"])
            today = datetime.now().strftime("%Y%m%d")
            os.makedirs("data/realtime", exist_ok=True)
            df.to_parquet(f"data/realtime/{today}.parquet", index=False)
            self.logger.info(f"实时行情采集完成: {len(df)}只")
    
    def _fetch_klines(self):
        """增量更新K线数据"""
        self.logger.info("K线数据采集...")
        # 复用已有的fetch_all_enhanced.py逻辑
