"""复盘报告推送 - 19:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.logger import get_logger
from services.report_db_service import ReportDBService
from scripts.send_review_report import ReviewReportGenerator


class ReviewReporter:
    """复盘报告推送器"""

    def __init__(self):
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """执行复盘报告推送"""
        self.logger.info("开始推送复盘报告...")

        try:
            generator = ReviewReportGenerator()
            success = generator.run()
            if success:
                self.logger.info("复盘报告推送成功")
            else:
                self.logger.warning("复盘报告推送失败")
            return success

        except Exception as e:
            self.logger.error(f"复盘报告推送失败: {e}")
            return False


if __name__ == "__main__":
    reporter = ReviewReporter()
    result = reporter.run()
    sys.exit(0 if result else 1)