"""当日复盘任务 - 16:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class DailyReviewTask(BaseTask):
    """当日复盘任务"""
    
    name = "daily_review"
    description = "生成当日复盘报告"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            # 读取数据
            realtime_path = f"data/realtime/{today}.parquet"
            if not os.path.exists(realtime_path):
                self.logger.warning(f"实时行情文件不存在: {realtime_path}")
                return False
            
            realtime = pd.read_parquet(realtime_path)
            scores = pd.read_parquet("data/enhanced_scores_full.parquet")
            
            index_path = f"data/index_analysis_{today}.parquet"
            index_df = pd.read_parquet(index_path) if os.path.exists(index_path) else pd.DataFrame()
            
            # 生成复盘报告
            report = self._generate_report(realtime, scores, index_df)
            
            # 保存报告
            self._save_report(report, today)
            
            return True
        except Exception as e:
            self.logger.error(f"复盘任务失败: {e}")
            return False
    
    def _generate_report(self, realtime, scores, index_df):
        """生成复盘报告"""
        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "market": {},
            "limit_up": {},
            "hot_sectors": {},
            "capital_flow": {}
        }
        
        # 1. 大盘分析
        if len(index_df) > 0:
            for idx_name in ["上证指数", "创业板指"]:
                idx_data = index_df[index_df["name"] == idx_name]
                if len(idx_data) > 0:
                    report["market"][idx_name] = idx_data.iloc[0].to_dict()
        
        # 2. 涨停板分析
        limit_up = realtime[realtime["change_pct"] >= 9.5]
        report["limit_up"] = {
            "total": len(limit_up),
            "stocks": limit_up[["code", "name", "change_pct"]].to_dict("records")[:20]
        }
        
        return report
    
    def _save_report(self, report, today):
        """保存报告"""
        import json
        os.makedirs("data/reports", exist_ok=True)
        path = f"data/reports/review_{today}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info(f"复盘报告已保存: {path}")
