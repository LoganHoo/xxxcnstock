#!/usr/bin/env python3
"""
晚间报告发送脚本
【20:45执行】发送明日策略推荐报告（结合消息面的选股推荐）
注意：如果tomorrow_picks.py已经发送了报告，此脚本可作为补充发送
"""
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.email_sender import EmailSender, EmailAPISender
from core.report_validator import check_report_quality, get_quality_checker
from core.paths import ReportPaths, DATA_DIR, REPORTS_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NightReportGenerator:
    """晚间报告生成器"""

    def __init__(self):
        self.data_dir = DATA_DIR
        self.report_dir = REPORTS_DIR
        self.news_dir = self.data_dir / "news"
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.today_report = None

    def load_today_picks(self) -> Optional[Dict]:
        """加载今日推荐数据"""
        try:
            report_files = list(self.report_dir.glob(f"*_{self.today}.json"))
            if report_files:
                latest_file = max(report_files, key=lambda p: p.stat().st_mtime)
                with open(latest_file, 'r', encoding='utf-8') as f:
                    self.today_report = json.load(f)
                    logger.info(f"已加载今日推荐: {latest_file.name}")
                    return self.today_report
        except Exception as e:
            logger.warning(f"无法加载今日推荐报告: {e}")
        return None

    def load_macro_news(self) -> Optional[Dict]:
        """加载宏观新闻"""
        try:
            news_file = self.news_dir / f"macro_news_{self.today}.json"
            if news_file.exists():
                with open(news_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载宏观新闻: {e}")
        return None

    def load_market_review(self) -> Optional[Dict]:
        """加载复盘数据"""
        try:
            review_file = ReportPaths.market_review()
            if review_file.exists():
                with open(review_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载复盘数据: {e}")
        return None

    def generate_supplement_content(self, picks: Dict = None, news: Dict = None,
                                   review: Dict = None) -> str:
        """生成晚间补充报告内容"""
        lines = []
        lines.append("=" * 70)
        lines.append("【晚间补充报告】明日策略与市场展望")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 70)

        lines.append("\n一、宏观新闻摘要")
        lines.append("-" * 50)
        if news and news.get('market_impact_news'):
            market_news = news['market_impact_news'][:5]
            for i, item in enumerate(market_news, 1):
                lines.append(f"  {i}. {item.get('title', 'N/A')}")
                lines.append(f"     来源: {item.get('source', '未知')} | {item.get('time', '')}")
        else:
            lines.append("  今日无重大宏观政策新闻")

        lines.append("\n二、市场复盘回顾")
        lines.append("-" * 50)
        if review:
            summary = review.get('summary', {})
            lines.append(f"  ● 涨停家数: {summary.get('limit_up_count', 'N/A')}")
            lines.append(f"  ● 跌停家数: {summary.get('limit_down_count', 'N/A')}")

            cvd = review.get('cvd', {})
            signal = cvd.get('signal', 'unknown')
            signal_text = {
                'buy_dominant': '主力净流入',
                'sell_dominant': '主力净流出',
                'neutral': '多空平衡'
            }.get(signal, signal)
            lines.append(f"  ● CVD信号: {signal_text}")
        else:
            lines.append("  复盘数据暂不可用")

        lines.append("\n三、明日操作建议")
        lines.append("-" * 50)

        if review:
            market_status = review.get('market_status', 'unknown')
            cvd_signal = review.get('cvd', {}).get('signal', 'neutral')

            if market_status == 'strong' and cvd_signal == 'buy_dominant':
                lines.append("  ✓ 市场强势，可适当增加仓位")
                lines.append("  ✓ 建议关注主线热点板块")
            elif market_status == 'weak' or cvd_signal == 'sell_dominant':
                lines.append("  ⚠️ 市场偏弱，建议控制仓位")
                lines.append("  ⚠️ 谨慎追高，以防御为主")
            else:
                lines.append("  → 市场震荡，结构性机会为主")
                lines.append("  → 建议控制仓位，精选个股")
        else:
            lines.append("  暂无足够数据给出建议")

        lines.append("\n四、重点关注板块")
        lines.append("-" * 50)
        if review and review.get('top_sectors'):
            for i, sector in enumerate(review['top_sectors'][:5], 1):
                lines.append(f"  {i}. {sector.get('name', 'N/A')}: {sector.get('change', 0):+.2f}%")
        else:
            lines.append("  暂无重点板块数据")

        lines.append("\n五、风险提示")
        lines.append("-" * 50)
        lines.append("  ⚠️ 晚间报告仅供参考，不构成投资建议")
        lines.append("  ⚠️ 明日开盘前请关注外盘动向")
        lines.append("  ⚠️ 控制仓位，股市有风险")

        lines.append("\n" + "=" * 70)
        lines.append("【报告说明】")
        lines.append("  本报告为盘后补充分析，结合:")
        lines.append("  1. 宏观政策新闻（晚间采集）")
        lines.append("  2. 复盘数据（17:00生成）")
        lines.append("  3. 完整推荐请查看 earlier sent 的明日策略报告")
        lines.append("=" * 70)

        return "\n".join(lines)

    def generate_html_report(self, picks: Dict = None, news: Dict = None,
                            review: Dict = None) -> str:
        """生成HTML格式晚间报告"""
        text_content = self.generate_supplement_content(picks, news, review)

        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>晚间补充报告 - {datetime.now().strftime('%Y-%m-%d')}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }}
        h1 {{ color: #333; border-bottom: 3px solid #28a745; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 20px; border-left: 4px solid #28a745; padding-left: 10px; }}
        .section {{ margin: 15px 0; padding: 15px; background: #f8f9fa; border-radius: 5px; }}
        .news-item {{ padding: 10px; margin: 5px 0; background: white; border-radius: 3px; border-left: 3px solid #007bff; }}
        .positive {{ color: #dc3545; }}
        .negative {{ color: #28a745; }}
        .warning-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 10px 0; }}
        .risk-box {{ background-color: #f8d7da; border-left: 4px solid #dc3545; padding: 15px; margin: 10px 0; }}
        .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #dee2e6; text-align: center; color: #666; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🌙 【晚间补充报告】明日策略与市场展望</h1>
        <p style="color: #666;">生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

        <h2>一、宏观新闻摘要</h2>
        <div class="section">
"""

        if news and news.get('market_impact_news'):
            for item in news['market_impact_news'][:5]:
                html += f"""
            <div class="news-item">
                <strong>{item.get('title', 'N/A')}</strong><br>
                <small style="color: #666;">来源: {item.get('source', '未知')} | {item.get('time', '')}</small>
            </div>
"""
        else:
            html += "<p>今日无重大宏观政策新闻</p>"

        html += """
        </div>

        <h2>二、市场复盘回顾</h2>
        <div class="section">
"""

        if review:
            summary = review.get('summary', {})
            html += f"""
            <p>涨停家数: <strong class="positive">{summary.get('limit_up_count', 'N/A')}</strong></p>
            <p>跌停家数: <strong class="negative">{summary.get('limit_down_count', 'N/A')}</strong></p>
            <p>CVD信号: <strong>{review.get('cvd', {}).get('signal', 'N/A')}</strong></p>
"""
        else:
            html += "<p>复盘数据暂不可用</p>"

        html += """
        </div>

        <h2>三、明日操作建议</h2>
        <div class="section">
"""

        if review:
            market_status = review.get('market_status', 'unknown')
            if market_status == 'strong':
                html += """
            <div style="background-color: #d4edda; padding: 10px; border-radius: 5px;">
                <p class="positive">✓ 市场强势，可适当增加仓位</p>
                <p class="positive">✓ 建议关注主线热点板块</p>
            </div>
"""
            elif market_status == 'weak':
                html += """
            <div class="warning-box">
                <p>⚠️ 市场偏弱，建议控制仓位</p>
                <p>⚠️ 谨慎追高，以防御为主</p>
            </div>
"""
            else:
                html += """
            <div>
                <p>→ 市场震荡，结构性机会为主</p>
                <p>→ 建议控制仓位，精选个股</p>
            </div>
"""
        else:
            html += "<p>暂无足够数据给出建议</p>"

        html += """
        </div>

        <h2>四、风险提示</h2>
        <div class="risk-box">
            <p>⚠️ 晚间报告仅供参考，不构成投资建议</p>
            <p>⚠️ 明日开盘前请关注外盘动向</p>
            <p>⚠️ 控制仓位，股市有风险</p>
        </div>

        <div class="footer">
            <p>XCNStock 量化分析系统 | 晚间补充报告</p>
            <p>完整推荐请查看 earlier sent 的明日策略报告</p>
        </div>
    </div>
</body>
</html>
"""
        return html


class NightReportSender:
    """晚间报告发送器"""

    def __init__(self):
        import os
        self.use_api = os.getenv('EMAIL_USE_API', 'true').lower() == 'true'
        if self.use_api:
            self.sender = EmailAPISender()
        else:
            self.sender = EmailSender(
                smtp_host=os.getenv('EMAIL_SMTP_SERVER', 'smtp.qq.com'),
                smtp_port=int(os.getenv('EMAIL_SMTP_PORT', 465)),
                smtp_user=os.getenv('EMAIL_USERNAME', ''),
                smtp_password=os.getenv('EMAIL_PASSWORD', ''),
                use_ssl=True
            )
        self.recipients = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com').split(',')
        self.recipients = [e.strip() for e in self.recipients if e.strip()]
        if not self.recipients:
            logger.warning("未配置NOTIFICATION_EMAILS，将使用默认邮箱: 287363@qq.com")
            self.recipients = ['287363@qq.com']

    def send(self, content: str, html_content: str = None) -> bool:
        """发送报告"""
        if not self.recipients:
            logger.error("未配置收件人邮箱")
            return False

        subject = f"【晚间补充】明日策略与市场展望 - {datetime.now().strftime('%Y-%m-%d')}"

        try:
            for recipient in self.recipients:
                success = self.sender.send(
                    to_addrs=[recipient],
                    subject=subject,
                    content=content,
                    html_content=html_content
                )
                if success:
                    logger.info(f"晚间补充报告已发送至: {recipient}")

            return True
        except Exception as e:
            logger.error(f"发送晚间报告失败: {e}")
            return False


def main():
    logger.info("=" * 60)
    logger.info("开始生成晚间补充报告")
    logger.info("=" * 60)

    generator = NightReportGenerator()
    sender = NightReportSender()

    picks = generator.load_today_picks()
    news = generator.load_macro_news()
    review = generator.load_market_review()

    # 数据质量检查
    quality_check = check_report_quality(
        'night_report',
        picks=picks,
        news=news,
        review=review
    )

    checker = get_quality_checker()
    quality_report = checker.generate_quality_report(quality_check)
    logger.info(f"数据质量检查:\n{quality_report}")

    # 如果有严重问题，记录但不阻止
    if quality_check['critical_issues']:
        logger.warning("数据存在严重问题，但继续生成报告:")
        for issue in quality_check['critical_issues']:
            logger.warning(f"  - {issue}")

    text_report = generator.generate_supplement_content(picks, news, review)
    html_report = generator.generate_html_report(picks, news, review)

    # 检查报告内容
    if not text_report or len(text_report.strip()) == 0:
        logger.error("生成的报告内容为空")
        return 1

    if "数据暂不可用" in text_report or "N/A" in text_report:
        logger.warning("报告内容包含异常数据标记")

    logger.info("\n晚间补充报告内容:")
    logger.info(text_report)

    success = sender.send(text_report, html_report)

    if success:
        logger.info("✅ 晚间补充报告发送成功")
        return 0
    else:
        logger.error("❌ 晚间补充报告发送失败")
        return 1


if __name__ == '__main__':
    sys.exit(main())
