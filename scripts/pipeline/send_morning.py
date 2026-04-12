"""晨间报告推送 - 08:45执行"""
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from services.email_sender import EmailService
from services.notify_service.templates import get_template
from services.report_db_service import ReportDBService


class MorningReporter:
    """晨间报告推送器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.data_dir = self.project_root / "data"
        self.reports_dir = self.project_root / "reports"
        self.logger = self._setup_logger()

    def _setup_logger(self):
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def load_foreign_data(self) -> dict:
        """加载外盘数据"""
        foreign_file = self.data_dir / "foreign_index.json"
        if foreign_file.exists():
            with open(foreign_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def load_market_analysis(self) -> dict:
        """加载大盘分析数据"""
        today = datetime.now().strftime('%Y%m%d')
        market_file = self.reports_dir / f"market_analysis_{today}.json"
        if not market_file.exists():
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            market_file = self.reports_dir / f"market_analysis_{yesterday}.json"
        if market_file.exists():
            with open(market_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def load_daily_picks(self) -> dict:
        """加载选股数据"""
        today = datetime.now().strftime('%Y%m%d')
        picks_file = self.reports_dir / f"daily_picks_{today}.json"
        if not picks_file.exists():
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            picks_file = self.reports_dir / f"daily_picks_{yesterday}.json"
        if picks_file.exists():
            with open(picks_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def load_strategy_result(self) -> dict:
        """加载策略结果"""
        strategy_file = self.reports_dir / "strategy_result.json"
        if strategy_file.exists():
            with open(strategy_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def load_fund_behavior_result(self) -> dict:
        """加载资金行为学策略结果"""
        fb_file = self.reports_dir / "fund_behavior_result.json"
        if fb_file.exists():
            with open(fb_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def generate_report(self) -> str:
        """生成完整报告"""
        foreign_data = self.load_foreign_data()
        market_data = self.load_market_analysis()
        picks_data = self.load_daily_picks()
        strategy_data = self.load_strategy_result()
        fb_data = self.load_fund_behavior_result()

        template = get_template('morning_report')
        return template.generate(
            market_data=market_data,
            picks_data=picks_data,
            foreign_data=foreign_data,
            strategy_data=strategy_data,
            fb_result=fb_data
        )

    def run(self) -> bool:
        """执行晨间报告推送"""
        self.logger.info("开始推送晨间报告...")

        try:
            content = self.generate_report()
            self.logger.info(f"报告内容:\n{content}")

            email_service = EmailService()

            recipients_str = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')
            recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

            if not recipients:
                self.logger.error("未配置收件人")
                return False

            today = datetime.now().strftime('%Y-%m-%d')
            subject = f"【晨间参考】A股量化决策 - {today}"

            self.logger.info(f"发送邮件到: {recipients}")
            self.logger.info(f"主题: {subject}")

            result = email_service.send(
                to_addrs=recipients,
                subject=subject,
                content=content
            )

            if result:
                self.logger.info("晨间报告推送成功")
                self.save_report_to_db(today, subject, content)
                return True
            else:
                self.logger.error("晨间报告推送失败")
                return False

        except Exception as e:
            self.logger.error(f"晨间报告推送异常: {e}")
            return False

    def save_report_to_db(self, report_date: str, subject: str, text_content: str):
        """保存报告到MySQL和TXT"""
        try:
            db_service = ReportDBService()
            db_service.init_tables()

            db_service.save_report(
                report_type='morning',
                report_date=report_date,
                subject=subject,
                text_content=text_content
            )

            txt_path = db_service.save_txt_file('morning', report_date, text_content)
            self.logger.info(f"TXT已保存: {txt_path}")

        except Exception as e:
            self.logger.warning(f"保存报告到数据库失败: {e}")


if __name__ == "__main__":
    from datetime import timedelta
    reporter = MorningReporter()
    result = reporter.run()
    sys.exit(0 if result else 1)
