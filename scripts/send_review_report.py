#!/usr/bin/env python3
"""
复盘报告发送脚本
【17:30执行】推送复盘快报：包含今日热点、资金流向及DQ质检摘要
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.email_sender import EmailSender, EmailAPISender
from services.notify_service.templates import get_template
from services.report_db_service import ReportDBService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReviewReportGenerator:
    """复盘报告生成器"""

    def __init__(self):
        self.data_dir = project_root / "data"
        self.log_dir = project_root / "logs"
        self.dq_report_path = self.data_dir / "dq_close.json"
        self.market_review_path = self.data_dir / "market_review.json"
        self.picks_review_path = self.data_dir / "picks_review.json"

    def load_dq_report(self) -> Optional[Dict]:
        """加载数据质检报告"""
        try:
            if self.dq_report_path.exists():
                with open(self.dq_report_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载质检报告: {e}")
        return None

    def load_market_review(self) -> Optional[Dict]:
        """加载复盘分析数据"""
        try:
            if self.market_review_path.exists():
                with open(self.market_review_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载复盘数据: {e}")
        return None

    def load_yesterday_picks(self) -> Optional[Dict]:
        """加载昨日选股复盘数据"""
        try:
            if self.picks_review_path.exists():
                with open(self.picks_review_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载选股复盘数据: {e}")
        return None

    def load_okr_data(self) -> Optional[Dict]:
        """加载OKR数据"""
        try:
            okr_path = self.data_dir / "okr.json"
            if okr_path.exists():
                with open(okr_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载OKR数据: {e}")
        return None

    def load_ai_review_data(self) -> Optional[Dict]:
        """加载AI复盘数据"""
        try:
            ai_review_path = self.data_dir / "ai_review.json"
            if ai_review_path.exists():
                with open(ai_review_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载AI复盘数据: {e}")
        return None

    def generate_market_review(self) -> Optional[Dict]:
        """从增强数据生成市场复盘数据"""
        try:
            import polars as pl
            from datetime import datetime, timedelta

            enhanced_path = self.data_dir / "enhanced_full_temp.parquet"
            cvd_path = self.data_dir / "cvd_latest.parquet"

            if not enhanced_path.exists():
                logger.warning("增强数据文件不存在")
                return None

            df = pl.read_parquet(enhanced_path)
            today = datetime.now().strftime('%Y-%m-%d')
            latest_date = df['trade_date'].max() if 'trade_date' in df.columns else today
            today_df = df.filter(pl.col('trade_date') == latest_date)

            if len(today_df) == 0:
                logger.warning(f"指定日期({latest_date})无数据")
                return None

            rising = len(today_df.filter(pl.col('change_pct') > 0))
            falling = len(today_df.filter(pl.col('change_pct') < 0))
            limit_up = len(today_df.filter(pl.col('change_pct') >= 9.9))
            limit_down = len(today_df.filter(pl.col('change_pct') <= -9.9))
            total_volume = (today_df['volume'].sum() / 1e8) if 'volume' in today_df.columns else 0

            avg_change = today_df['change_pct'].mean() if 'change_pct' in today_df.columns else 0
            market_status = 'strong' if avg_change > 1 else 'weak' if avg_change < -1 else 'oscillating'

            cvd_data = {
                'signal': 'neutral',
                'cvd_cumsum': 0,
                'cvd_trend': 'neutral'
            }
            if cvd_path.exists():
                try:
                    cvd_df = pl.read_parquet(cvd_path)
                    if 'cvd_body_cum' in cvd_df.columns:
                        total_cvd = cvd_df['cvd_body_cum'].sum()
                        cvd_data = {
                            'signal': 'buy_dominant' if total_cvd > 0 else 'sell_dominant',
                            'cvd_cumsum': round(total_cvd / 1e8, 2),
                            'cvd_trend': 'accumulating' if total_cvd > 0 else 'distributing'
                        }
                except Exception as e:
                    logger.warning(f"加载CVD数据失败: {e}")

            price_col = 'price' if 'price' in df.columns else 'close'
            key_levels = {
                'index_close': round(df.filter(pl.col('trade_date') == latest_date)[price_col].mean(), 2) if price_col in df.columns and len(df.filter(pl.col('trade_date') == latest_date)) > 0 else 0
            }

            top_sectors = []
            if 'sector' in today_df.columns:
                sector_data = today_df.group_by('sector').agg([
                    pl.col('change_pct').mean().alias('avg_change'),
                    pl.col('volume').sum().alias('total_volume')
                ]).sort('avg_change', descending=True)
                top_sectors = [
                    {'name': row['sector'], 'change': round(row['avg_change'], 2)}
                    for row in sector_data.head(5).to_dicts()
                ] if len(sector_data) > 0 else []
            else:
                board_map = {
                    '沪市主板': lambda c: c.startswith(('000', '001', '002', '003')),
                    '科创板': lambda c: c.startswith('688'),
                    '创业板': lambda c: c.startswith('300'),
                    '北交所': lambda c: c.startswith('4') or c.startswith('8'),
                }
                board_stats = {name: {'changes': [], 'volumes': []} for name in board_map}
                for row in today_df.to_dicts():
                    code = str(row.get('code', ''))
                    for board_name, check_fn in board_map.items():
                        if check_fn(code):
                            board_stats[board_name]['changes'].append(row.get('change_pct', 0))
                            board_stats[board_name]['volumes'].append(row.get('volume', 0))
                            break
                sector_results = []
                for board_name, stats in board_stats.items():
                    if stats['changes']:
                        avg_change = sum(stats['changes']) / len(stats['changes'])
                        total_vol = sum(stats['volumes'])
                        sector_results.append({'name': board_name, 'change': round(avg_change, 2), 'volume': total_vol})
                sector_results.sort(key=lambda x: x['change'], reverse=True)
                top_sectors = [{'name': r['name'], 'change': r['change']} for r in sector_results[:5]]

            review = {
                'date': today,
                'summary': {
                    'rising_count': rising,
                    'falling_count': falling,
                    'limit_up_count': limit_up,
                    'limit_down_count': limit_down,
                    'total_volume': round(total_volume, 2)
                },
                'market_status': market_status,
                'cvd': cvd_data,
                'key_levels': key_levels,
                'top_sectors': top_sectors
            }

            with open(self.market_review_path, 'w', encoding='utf-8') as f:
                json.dump(review, f, ensure_ascii=False, indent=2)
            logger.info(f"市场复盘数据已生成: {self.market_review_path}")

            return review
        except Exception as e:
            logger.error(f"生成市场复盘数据失败: {e}")
            return None

    def _generate_picks_review_section(self, picks_review: Dict) -> list:
        """生成昨日选股复盘章节"""
        lines = []
        lines.append("\n" + "=" * 70)
        lines.append("【昨日选股复盘】")
        lines.append("=" * 70)

        if not picks_review:
            lines.append("  ⚠️ 昨日选股复盘数据暂不可用")
            return lines

        summary = picks_review.get('summary', {})
        total_picks = summary.get('total_picks', 0)
        reviewed_picks = summary.get('reviewed_picks', 0)
        lines.append(f"\n📊 选股统计:")
        lines.append(f"  ● 昨日推荐: {total_picks}只")
        lines.append(f"  ● 已复盘: {reviewed_picks}只")

        if reviewed_picks > 0:
            win_count = summary.get('win_count', 0)
            loss_count = summary.get('loss_count', 0)
            hold_count = summary.get('hold_count', 0)
            win_rate = (win_count / reviewed_picks * 100) if reviewed_picks > 0 else 0

            lines.append(f"\n📈 复盘结果:")
            lines.append(f"  ● 上涨(+): {win_count}只")
            lines.append(f"  ● 下跌(-): {loss_count}只")
            lines.append(f"  ● 持平(=): {hold_count}只")
            lines.append(f"  ● 胜率: {win_rate:.1f}%")

        top_picks = picks_review.get('top_picks', [])
        if top_picks:
            lines.append(f"\n🏆 昨日推荐表现:")

            for i, pick in enumerate(top_picks[:5], 1):
                stock_code = pick.get('stock_code', 'N/A')
                stock_name = pick.get('stock_name', 'N/A')
                change_pct = pick.get('change_pct', 0)
                change_sign = '+' if change_pct > 0 else ''
                status = pick.get('status', 'N/A')

                status_icon = '✅' if status == 'win' else '❌' if status == 'loss' else '➖'
                lines.append(f"  {i}. {stock_code} {stock_name}")
                lines.append(f"     涨跌幅: {change_sign}{change_pct:.2f}% {status_icon}")
                lines.append(f"     推荐理由: {pick.get('reason', 'N/A')[:30]}...")

        details = picks_review.get('details', [])
        if details:
            lines.append(f"\n📋 详细复盘:")
            for detail in details[:10]:
                code = detail.get('stock_code', 'N/A')
                name = detail.get('stock_name', 'N/A')
                change = detail.get('change_pct', 0)
                open_price = detail.get('open', 0)
                close_price = detail.get('close', 0)
                high_price = detail.get('high', 0)
                low_price = detail.get('low', 0)

                change_sign = '+' if change > 0 else ''
                lines.append(f"  ● {code} {name}: {change_sign}{change:.2f}%")
                lines.append(f"    开盘: {open_price:.2f} 收盘: {close_price:.2f} 最高: {high_price:.2f} 最低: {low_price:.2f}")

        return lines

    def generate_text_report(self, dq_report: Dict = None, market_review: Dict = None, picks_review: Dict = None) -> str:
        """生成文本格式复盘报告"""
        lines = []
        lines.append("=" * 70)
        lines.append("【复盘快报】A股市场今日总结")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 70)

        lines.append("\n一、数据质量报告")
        lines.append("-" * 50)
        if dq_report:
            completeness = dq_report.get('completeness', {})
            validity = dq_report.get('validity', {})
            freshness = dq_report.get('freshness', {})

            total_stocks = completeness.get('total_stocks', 'N/A')
            valid_stocks = completeness.get('valid_stocks', 'N/A')
            completeness_rate = completeness.get('completeness_rate', 0) * 100 if completeness.get('completeness_rate') else 0

            lines.append(f"  ● 采集完整度: {completeness_rate:.1f}% ({valid_stocks}/{total_stocks}只)")
            lines.append(f"  ● 有效数据: {validity.get('valid_count', 'N/A')}只")
            lines.append(f"  ● 无效数据: {validity.get('invalid_count', 0)}只")

            if freshness:
                latest_update = freshness.get('latest_update', 'N/A')
                lines.append(f"  ● 最新更新: {latest_update}")

            if completeness_rate >= 95:
                lines.append("  ✓ 数据质量合格，可进行复盘分析")
            else:
                lines.append("  ⚠️ 数据完整度偏低，分析结果仅供参考")
        else:
            lines.append("  ⚠️ 质检报告暂不可用")

        lines.append("\n二、今日市场概况")
        lines.append("-" * 50)
        if market_review:
            summary = market_review.get('summary', {})
            lines.append(f"  ● 上涨股票: {summary.get('rising_count', 'N/A')}只")
            lines.append(f"  ● 下跌股票: {summary.get('falling_count', 'N/A')}只")
            lines.append(f"  ● 涨停股票: {summary.get('limit_up_count', 'N/A')}只")
            lines.append(f"  ● 跌停股票: {summary.get('limit_down_count', 'N/A')}只")
            lines.append(f"  ● 成交额: {summary.get('total_volume', 'N/A')}亿")

            market_status = market_review.get('market_status', 'unknown')
            status_map = {
                'strong': '强势上涨',
                'weak': '弱势下跌',
                'oscillating': '震荡整理',
                'unknown': '状态未知'
            }
            lines.append(f"  ● 市场状态: {status_map.get(market_status, market_status)}")
        else:
            lines.append("  ⚠️ 复盘数据暂不可用")

        lines.append("\n三、资金流向")
        lines.append("-" * 50)
        if market_review:
            cvd_data = market_review.get('cvd', {})
            cvd_signal = cvd_data.get('signal', 'neutral')
            signal_map = {
                'buy_dominant': '主力净流入（买方占优）',
                'sell_dominant': '主力净流出（卖方占优）',
                'neutral': '多空平衡'
            }
            lines.append(f"  ● CVD信号: {signal_map.get(cvd_signal, cvd_signal)}")
            lines.append(f"  ● CVD累计: {cvd_data.get('cvd_cumsum', 'N/A')}")
            lines.append(f"  ● CVD趋势: {cvd_data.get('cvd_trend', 'N/A')}")
        else:
            lines.append("  ⚠️ 资金流向数据暂不可用")

        lines.append("\n四、关键位分析")
        lines.append("-" * 50)
        if market_review:
            levels = market_review.get('key_levels', {})
            lines.append(f"  ● 上证指数: {levels.get('index_close', 'N/A')}")
            lines.append(f"  ● 60日高点: {levels.get('high_60', 'N/A')}")
            lines.append(f"  ● 60日低点: {levels.get('low_60', 'N/A')}")
            lines.append(f"  ● MA5: {levels.get('ma5', 'N/A')}")
            lines.append(f"  ● MA20: {levels.get('ma20', 'N/A')}")
        else:
            lines.append("  ⚠️ 关键位数据暂不可用")

        lines.append("\n五、热点板块")
        lines.append("-" * 50)
        if market_review:
            sectors = market_review.get('top_sectors', [])
            if sectors:
                for i, sector in enumerate(sectors[:5], 1):
                    lines.append(f"  {i}. {sector.get('name', 'N/A')}: {sector.get('change', 'N/A')}%")
            else:
                lines.append("  暂无热点板块数据")
        else:
            lines.append("  ⚠️ 热点板块数据暂不可用")

        lines.extend(self._generate_picks_review_section(picks_review))

        lines.append("\n" + "=" * 70)
        lines.append("【风险提示】本报告仅供参考，不构成投资建议。")
        lines.append("=" * 70)

        return "\n".join(lines)

    def generate_html_report(self, dq_report: Dict = None, market_review: Dict = None, picks_review: Dict = None) -> str:
        """生成HTML格式复盘报告"""
        text_content = self.generate_text_report(dq_report, market_review, picks_review)

        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>复盘快报 - {datetime.now().strftime('%Y-%m-%d')}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #007bff; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 20px; border-left: 4px solid #007bff; padding-left: 10px; }}
        .section {{ margin: 15px 0; padding: 15px; background: #f8f9fa; border-radius: 5px; }}
        .stat {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }}
        .stat-label {{ font-weight: bold; color: #666; }}
        .stat-value {{ color: #333; }}
        .positive {{ color: #dc3545; }}
        .negative {{ color: #28a745; }}
        .warning {{ color: #ffc107; }}
        .footer {{ margin-top: 30px; padding-top: 20px; border-top: 2px solid #dee2e6; text-align: center; color: #666; font-size: 0.9em; }}
        .quality-ok {{ background-color: #d4edda; border-left: 4px solid #28a745; }}
        .quality-warn {{ background-color: #fff3cd; border-left: 4px solid #ffc107; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 【复盘快报】A股市场今日总结</h1>
        <p style="color: #666;">生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

        <h2>一、数据质量报告</h2>
        <div class="section {'quality-ok' if dq_report and dq_report.get('passed') else 'quality-warn'}">
"""

        if dq_report:
            completeness = dq_report.get('completeness', {})
            completeness_rate = completeness.get('completeness_rate', 0) * 100 if completeness.get('completeness_rate') else 0
            html += f"""
            <div class="stat">
                <span class="stat-label">采集完整度</span>
                <span class="stat-value">{"✓ " if completeness_rate >= 95 else "⚠️ "}{completeness_rate:.1f}%</span>
            </div>
            <div class="stat">
                <span class="stat-label">有效数据</span>
                <span class="stat-value">{completeness.get('valid_stocks', 'N/A')}只</span>
            </div>
"""
        else:
            html += "<p>⚠️ 质检报告暂不可用</p>"

        html += """
        </div>

        <h2>二、今日市场概况</h2>
        <div class="section">
"""

        if market_review:
            summary = market_review.get('summary', {})
            rising = summary.get('rising_count', 0)
            falling = summary.get('falling_count', 0)
            limit_up = summary.get('limit_up_count', 0)
            limit_down = summary.get('limit_down_count', 0)

            html += f"""
            <div class="stat">
                <span class="stat-label">上涨股票</span>
                <span class="stat-value positive">↑ {rising}只</span>
            </div>
            <div class="stat">
                <span class="stat-label">下跌股票</span>
                <span class="stat-value negative">↓ {falling}只</span>
            </div>
            <div class="stat">
                <span class="stat-label">涨停股票</span>
                <span class="stat-value positive">🔥 {limit_up}只</span>
            </div>
            <div class="stat">
                <span class="stat-label">跌停股票</span>
                <span class="stat-value negative">❄️ {limit_down}只</span>
            </div>
"""
        else:
            html += "<p>⚠️ 市场数据暂不可用</p>"

        html += """
        </div>

        <h2>三、资金流向 (CVD)</h2>
        <div class="section">
"""

        if market_review:
            cvd_data = market_review.get('cvd', {})
            signal = cvd_data.get('signal', 'neutral')
            signal_text = {'buy_dominant': '主力净流入', 'sell_dominant': '主力净流出', 'neutral': '多空平衡'}.get(signal, signal)

            html += f"""
            <div class="stat">
                <span class="stat-label">CVD信号</span>
                <span class="stat-value">{'🟢' if signal == 'buy_dominant' else '🔴' if signal == 'sell_dominant' else '⚪'} {signal_text}</span>
            </div>
            <div class="stat">
                <span class="stat-label">CVD累计值</span>
                <span class="stat-value">{cvd_data.get('cvd_cumsum', 'N/A')}</span>
            </div>
"""
        else:
            html += "<p>⚠️ 资金流向数据暂不可用</p>"

        html += """
        </div>

        <h2>四、热点板块</h2>
        <div class="section">
"""

        if market_review:
            sectors = market_review.get('top_sectors', [])
            if sectors:
                for sector in sectors[:5]:
                    html += f"""
            <div class="stat">
                <span class="stat-label">{sector.get('name', 'N/A')}</span>
                <span class="stat-value {'positive' if sector.get('change', 0) > 0 else 'negative'}">{sector.get('change', 0):+.2f}%</span>
            </div>
"""
            else:
                html += "<p>暂无热点板块数据</p>"
        else:
            html += "<p>⚠️ 热点板块数据暂不可用</p>"

        html += """
        </div>

        <div class="footer">
            <p>本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。</p>
            <p>XCNStock 量化分析系统</p>
        </div>
    </div>
</body>
</html>
"""
        return html


class ReviewReportSender:
    """复盘报告发送器"""

    def __init__(self):
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

        subject = f"【复盘快报】A股市场总结 - {datetime.now().strftime('%Y-%m-%d')}"

        try:
            for recipient in self.recipients:
                success = self.sender.send(
                    to_addrs=[recipient],
                    subject=subject,
                    content=content,
                    html_content=html_content
                )
                if success:
                    logger.info(f"复盘报告已发送至: {recipient}")

            return True
        except Exception as e:
            logger.error(f"发送复盘报告失败: {e}")
            return False


def main():
    logger.info("=" * 60)
    logger.info("开始生成完整复盘报告")
    logger.info("=" * 60)

    generator = ReviewReportGenerator()
    sender = ReviewReportSender()

    dq_report = generator.load_dq_report()
    market_review = generator.load_market_review()
    if not market_review:
        logger.info("market_review.json 不存在，尝试生成...")
        market_review = generator.generate_market_review()
    picks_review = generator.load_yesterday_picks()
    okr_data = generator.load_okr_data()
    ai_review_data = generator.load_ai_review_data()

    template = get_template('review_report')
    text_report = template.generate(
        market_data=market_review,
        picks_review_data=picks_review,
        dq_report=dq_report,
        okr_data=okr_data,
        ai_review_data=ai_review_data
    )

    html_report = generator.generate_html_report(dq_report, market_review, picks_review)

    logger.info("\n复盘报告内容:")
    logger.info(text_report)

    success = sender.send(text_report, html_report)

    if success:
        logger.info("✅ 完整复盘报告发送成功")
        save_report_to_db('review', text_report)
        return 0
    else:
        logger.error("❌ 完整复盘报告发送失败")
        return 1


def save_report_to_db(report_type: str, text_content: str):
    """保存报告到MySQL和TXT"""
    try:
        db_service = ReportDBService()
        db_service.init_tables()

        report_date = datetime.now().strftime('%Y-%m-%d')
        subject = f"【完整复盘】A股日终总结 - {report_date}"

        db_service.save_report(
            report_type=report_type,
            report_date=report_date,
            subject=subject,
            text_content=text_content
        )

        txt_path = db_service.save_txt_file(report_type, report_date, text_content)
        logger.info(f"TXT已保存: {txt_path}")

    except Exception as e:
        logger.warning(f"保存报告到数据库失败: {e}")


if __name__ == '__main__':
    sys.exit(main())
