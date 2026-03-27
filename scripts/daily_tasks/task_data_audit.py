"""数据验证审计任务 - 16:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from datetime import datetime
from .base_task import BaseTask


class DataAuditTask(BaseTask):
    """数据验证任务"""
    
    name = "data_audit"
    description = "验证数据完整性和新鲜度"
    
    def run(self) -> bool:
        try:
            today = datetime.now().strftime("%Y%m%d")
            issues = []
            
            # 1. 检查实时行情
            realtime_path = f"data/realtime/{today}.parquet"
            if os.path.exists(realtime_path):
                df = pd.read_parquet(realtime_path)
                if len(df) < 4000:
                    issues.append(f"实时行情不足: {len(df)}只")
            else:
                issues.append(f"实时行情文件不存在: {realtime_path}")
            
            # 2. 检查分析结果
            scores_path = "data/enhanced_scores_full.parquet"
            if os.path.exists(scores_path):
                df = pd.read_parquet(scores_path)
                if len(df) < 4000:
                    issues.append(f"分析结果不足: {len(df)}只")
            else:
                issues.append(f"分析结果文件不存在: {scores_path}")
            
            # 3. 记录结果
            if issues:
                for issue in issues:
                    self.logger.warning(issue)
                # 可以触发告警通知
            else:
                self.logger.info("数据验证通过")
            
            return True
        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False
