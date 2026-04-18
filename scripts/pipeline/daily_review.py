"""每日复盘分析 - 17:30执行"""
import sys
import subprocess
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
from core.logger import get_logger


class DailyReviewer:
    """每日复盘器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行每日复盘"""
        self.logger.info("开始每日复盘分析...")

        success = True

        # 1. 运行主力行为策略生成复盘数据
        try:
            script_path = self.project_root / "scripts" / "run_fund_behavior_strategy.py"
            if script_path.exists():
                self.logger.info(f"调用主力行为策略脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode == 0:
                    self.logger.info("主力行为策略执行成功")
                else:
                    self.logger.warning(f"主力行为策略执行失败: {result.stderr}")
                    success = False
            else:
                self.logger.warning(f"主力行为策略脚本不存在: {script_path}")
        except Exception as e:
            self.logger.error(f"主力行为策略执行异常: {e}")
            success = False

        # 2. 生成市场复盘数据(market_review.json)
        try:
            self.logger.info("生成市场复盘数据...")
            from scripts.send_review_report import ReviewReportGenerator
            generator = ReviewReportGenerator()
            market_review = generator.generate_market_review()
            if market_review:
                self.logger.info("市场复盘数据生成成功")
            else:
                self.logger.warning("市场复盘数据生成失败")
                success = False
        except Exception as e:
            self.logger.error(f"生成市场复盘数据异常: {e}")
            success = False

        return success


if __name__ == "__main__":
    reviewer = DailyReviewer()
    result = reviewer.run()
    sys.exit(0 if result else 1)