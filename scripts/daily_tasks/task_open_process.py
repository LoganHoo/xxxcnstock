"""开盘处理任务 - 09:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class OpenProcessTask(BaseTask):
    """开盘处理任务 - 标记一字涨停"""
    
    name = "open_process"
    description = "处理一字涨停股票"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            # 读取昨日打板名单
            picks_path = f"data/reports/picks_{today}.json"
            if not os.path.exists(picks_path):
                self.logger.warning("选股报告不存在")
                return True
            
            import json
            with open(picks_path, "r", encoding="utf-8") as f:
                picks = json.load(f)
            
            # 获取今日开盘数据
            realtime_path = f"data/realtime/{today}.parquet"
            if not os.path.exists(realtime_path):
                self.logger.warning("实时行情不存在")
                return True
            
            realtime = pd.read_parquet(realtime_path)
            
            # 处理一字涨停
            processed = self._process_limit_up(picks, realtime)
            
            # 更新报告
            with open(picks_path, "w", encoding="utf-8") as f:
                json.dump(processed, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"开盘处理失败: {e}")
            return False
    
    def _process_limit_up(self, picks, realtime):
        """标记一字涨停"""
        limit_up_stocks = picks.get("limit_up_potential", [])
        
        for stock in limit_up_stocks:
            code = stock["code"]
            
            # 查找实时数据
            rt = realtime[realtime["code"] == code]
            if len(rt) == 0:
                continue
            
            rt = rt.iloc[0]
            change_pct = rt.get("change_pct", 0)
            volume = rt.get("volume", 0)
            
            # 判断一字涨停
            if change_pct >= 9.9 and volume < 10000:
                stock["seal_type"] = "一字涨停"
                stock["excluded"] = True
                stock["exclude_reason"] = "开盘一字涨停，无法买入"
            elif change_pct >= 9.5:
                stock["seal_type"] = "正常涨停"
            else:
                stock["seal_type"] = "未涨停"
        
        return picks
