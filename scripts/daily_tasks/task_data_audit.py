"""数据验证审计任务 - 每日16:00执行"""
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

            # 1. 检查K线数据完整性
            kline_dir = "data/kline"
            if os.path.exists(kline_dir):
                kline_files = list(Path(kline_dir).glob("*.parquet"))
                if len(kline_files) < 4000:
                    issues.append(f"K线文件不足: {len(kline_files)}个")
            else:
                issues.append(f"K线目录不存在: {kline_dir}")

            # 2. 检查最新日期数据量
            self._check_latest_data(issues)

            # 3. 记录结果
            if issues:
                for issue in issues:
                    self.logger.warning(issue)
            else:
                self.logger.info("数据验证通过")

            return True
        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False

    def _check_latest_data(self, issues):
        """检查最新日期数据量"""
        import polars as pl
        from pathlib import Path

        kline_dir = Path("data/kline")
        if not kline_dir.exists():
            return

        try:
            dfs = [pl.read_parquet(f) for f in list(kline_dir.glob("*.parquet"))[:100]]
            data = pl.concat(dfs)

            latest_date = data["trade_date"].max()
            day_data = data.filter(pl.col("trade_date") == latest_date)

            if len(day_data) < 4000:
                issues.append(f"最新日期({latest_date})数据不足: {len(day_data)}只")
        except Exception as e:
            self.logger.debug(f"检查最新数据失败: {e}")
