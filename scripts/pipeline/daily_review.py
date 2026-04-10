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

        try:
            script_path = self.project_root / "scripts" / "run_fund_behavior_strategy.py"
            if script_path.exists():
                self.logger.info(f"调用复盘脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode == 0:
                    self.logger.info("每日复盘成功")
                else:
                    self.logger.warning(f"每日复盘失败: {result.stderr}")
            else:
                self.logger.warning(f"复盘脚本不存在: {script_path}")
            return True

        except subprocess.TimeoutExpired:
            self.logger.error(f"每日复盘超时: {script_path}")
            return False
        except Exception as e:
            self.logger.error(f"每日复盘失败: {e}")
            return False


if __name__ == "__main__":
    reviewer = DailyReviewer()
    result = reviewer.run()
    sys.exit(0 if result else 1)