"""复盘报告推送 - 19:00执行"""
import sys
import subprocess
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
from core.logger import get_logger


class ReviewReporter:
    """复盘报告推送器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行复盘报告推送"""
        self.logger.info("开始推送复盘报告...")

        try:
            script_path = self.project_root / "scripts" / "send_review_report.py"
            if script_path.exists():
                self.logger.info(f"调用报告脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result.returncode == 0:
                    self.logger.info("复盘报告推送成功")
                else:
                    self.logger.warning(f"复盘报告推送失败: {result.stderr}")
            else:
                self.logger.warning(f"报告脚本不存在: {script_path}")
            return True

        except subprocess.TimeoutExpired:
            self.logger.error(f"复盘报告推送超时: {script_path}")
            return False
        except Exception as e:
            self.logger.error(f"复盘报告推送失败: {e}")
            return False


if __name__ == "__main__":
    reporter = ReviewReporter()
    result = reporter.run()
    sys.exit(0 if result else 1)