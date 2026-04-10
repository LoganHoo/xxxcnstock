"""预计算技术指标 - 20:00执行"""
import sys
import subprocess
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
from core.logger import get_logger


class Precomputer:
    """预计算器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行预计算"""
        self.logger.info("开始预计算技术指标...")

        try:
            script_path = self.project_root / "scripts" / "precompute_enhanced_scores.py"
            if script_path.exists():
                self.logger.info(f"调用预计算脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode == 0:
                    self.logger.info("预计算成功")
                else:
                    self.logger.warning(f"预计算失败: {result.stderr}")
            else:
                self.logger.warning(f"预计算脚本不存在: {script_path}")
            return True

        except subprocess.TimeoutExpired:
            self.logger.error(f"预计算超时: {script_path}")
            return False
        except Exception as e:
            self.logger.error(f"预计算失败: {e}")
            return False


if __name__ == "__main__":
    computer = Precomputer()
    result = computer.run()
    sys.exit(0 if result else 1)