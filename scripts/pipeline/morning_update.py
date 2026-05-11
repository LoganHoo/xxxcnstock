"""晨间数据更新 - 08:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.logger import get_logger
from scripts.update_morning_data import MorningDataUpdater


class MorningUpdater:
    """晨间更新器"""

    def __init__(self):
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行晨间更新"""
        self.logger.info("开始晨间数据更新...")

        try:
            updater = MorningDataUpdater()
            morning_data = updater.update_all()

            summary = updater.generate_morning_summary(morning_data)
            self.logger.info(summary)

            self.logger.info("晨间更新成功")
            return True

        except Exception as e:
            self.logger.error(f"晨间更新失败: {e}")
            return False


if __name__ == "__main__":
    updater = MorningUpdater()
    result = updater.run()
    sys.exit(0 if result else 1)