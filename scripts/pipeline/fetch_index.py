"""大盘指数采集 - 15:30执行"""
import sys
import subprocess
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
from core.logger import get_logger


class IndexFetcher:
    """指数采集器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行大盘指数采集"""
        self.logger.info("开始采集大盘指数...")

        try:
            script_path = self.project_root / "scripts" / "fetch_index_data.py"
            if script_path.exists():
                self.logger.info(f"调用采集脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result.returncode == 0:
                    self.logger.info("大盘指数采集成功")
                else:
                    self.logger.warning(f"大盘指数采集失败: {result.stderr}")
            else:
                self.logger.warning(f"采集脚本不存在: {script_path}")
            return True

        except subprocess.TimeoutExpired:
            self.logger.error(f"大盘指数采集超时: {script_path}")
            return False
        except Exception as e:
            self.logger.error(f"大盘指数采集失败: {e}")
            return False


if __name__ == "__main__":
    fetcher = IndexFetcher()
    result = fetcher.run()
    sys.exit(0 if result else 1)