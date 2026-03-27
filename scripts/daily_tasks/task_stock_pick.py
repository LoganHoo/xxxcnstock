"""次日选股任务 - 17:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class StockPickTask(BaseTask):
    """次日选股任务"""
    
    name = "stock_pick"
    description = "生成次日选股报告"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            # 读取分析结果
            scores = pd.read_parquet("data/enhanced_scores_full.parquet")
            
            # 生成选股报告
            picks = self._generate_picks(scores)
            
            # 保存报告
            self._save_picks(picks, today)
            
            return True
        except Exception as e:
            self.logger.error(f"选股任务失败: {e}")
            return False
    
    def _generate_picks(self, scores):
        """生成选股结果"""
        picks = {
            "s_grade": [],
            "a_grade": [],
            "limit_up_potential": []
        }
        
        # S级推荐
        s_stocks = scores[scores["grade"] == "S"].sort_values("enhanced_score", ascending=False)
        cols = ["code", "name", "price", "enhanced_score"]
        if "reasons" in s_stocks.columns:
            cols.append("reasons")
        picks["s_grade"] = s_stocks[cols].head(30).to_dict("records")
        
        # A级推荐
        a_stocks = scores[scores["grade"] == "A"].sort_values("enhanced_score", ascending=False)
        picks["a_grade"] = a_stocks[cols].head(30).to_dict("records")
        
        # 打板潜力 (涨停+多头排列)
        if "change_pct" in scores.columns and "trend" in scores.columns:
            limit_up = scores[(scores["change_pct"] >= 9.5) & (scores["trend"] == 100)]
            picks["limit_up_potential"] = limit_up[["code", "name", "price", "change_pct", "enhanced_score"]].to_dict("records")
        
        return picks
    
    def _save_picks(self, picks, today):
        """保存选股结果"""
        import json
        os.makedirs("data/reports", exist_ok=True)
        path = f"data/reports/picks_{today}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(picks, f, ensure_ascii=False, indent=2)
        self.logger.info(f"选股报告已保存: {path}")
