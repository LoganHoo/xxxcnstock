"""外盘指数更新 - 06:00执行"""
import sys
import subprocess
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from datetime import datetime
from core.logger import get_logger


class ForeignUpdater:
    """外盘更新器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)
        self._set_proxy()

    def _set_proxy(self):
        """设置代理"""
        proxy_vars = [
            ('https_proxy', 'http://127.0.0.1:7890'),
            ('http_proxy', 'http://127.0.0.1:7890'),
            ('all_proxy', 'socks5://127.0.0.1:7890'),
            ('HTTPS_PROXY', 'http://127.0.0.1:7890'),
            ('HTTP_PROXY', 'http://127.0.0.1:7890'),
            ('ALL_PROXY', 'socks5://127.0.0.1:7890'),
        ]
        for key, value in proxy_vars:
            os.environ.setdefault(key, value)
        self.logger.info("代理已设置: 127.0.0.1:7890")

    def run(self) -> bool:
        """执行外盘指数更新"""
        self.logger.info("开始更新外盘指数...")

        try:
            script_path = self.project_root / "scripts" / "update_foreign_index.py"
            if script_path.exists():
                self.logger.info(f"调用更新脚本: {script_path}")
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result.returncode == 0:
                    self.logger.info("外盘指数更新成功")
                else:
                    self.logger.warning(f"外盘指数更新失败: {result.stderr}")
            else:
                self.logger.warning(f"更新脚本不存在: {script_path}")
            return True

        except subprocess.TimeoutExpired:
            self.logger.error(f"外盘指数更新超时: {script_path}")
            return False
        except Exception as e:
            self.logger.error(f"外盘指数更新失败: {e}")
            return False


if __name__ == "__main__":
    updater = ForeignUpdater()
    result = updater.run()
    sys.exit(0 if result else 1)