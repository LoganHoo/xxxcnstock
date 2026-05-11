"""晚间选股推荐 - 20:30执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
from core.logger import get_logger
from scripts.tomorrow_picks import StockRecommender


class NightPicks:
    """晚间选股"""

    def __init__(self):
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行晚间选股"""
        self.logger.info("开始晚间选股...")

        try:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "xcn_comm.yaml"

            recommender = StockRecommender(str(config_path))
            recommender.run()

            self.logger.info("晚间选股成功")
            return True

        except Exception as e:
            self.logger.error(f"晚间选股失败: {e}")
            return False


if __name__ == "__main__":
    picks = NightPicks()
    result = picks.run()
    sys.exit(0 if result else 1)