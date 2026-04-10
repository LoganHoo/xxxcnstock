"""晚间选股推荐 - 20:30执行"""
import sys
import subprocess
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
from core.logger import get_logger


class NightPicks:
    """晚间选股"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行晚间选股"""
        self.logger.info("开始晚间选股...")

        try:
            script_path = self.project_root / "scripts" / "tomorrow_picks.py"
            if script_path.exists():
                self.logger.info(f"调用选股脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode == 0:
                    self.logger.info("晚间选股成功")
                else:
                    self.logger.warning(f"晚间选股失败: {result.stderr}")
            else:
                self.logger.warning(f"选股脚本不存在: {script_path}")
            return True

        except subprocess.TimeoutExpired:
            self.logger.error(f"晚间选股超时: {script_path}")
            return False
        except Exception as e:
            self.logger.error(f"晚间选股失败: {e}")
            return False


if __name__ == "__main__":
    picks = NightPicks()
    result = picks.run()
    sys.exit(0 if result else 1)