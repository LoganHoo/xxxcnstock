"""
早上报告发送脚本
发送昨日推荐报告和大盘分析
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import os

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.email_sender import EmailService

def get_yesterday_picks_report():
    """获取昨日推荐报告"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    report_file = project_root / 'reports' / f'daily_picks_{yesterday}.json'
    
    if not report_file.exists():
        return None, None
    
    with open(report_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    filters = data.get('filters', {})
    stats = data.get('stats', {})
    
    text = f"【昨日推荐回顾】\n"
    text += f"日期: {yesterday}\n\n"
    
    grade_names = {
        's_grade': 'S级 - 强烈推荐',
        'a_grade': 'A级 - 建议关注',
        'bullish': '多头排列+上涨',
        'macd_volume': 'MACD金叉+量价齐升'
    }
    
    for grade_key, grade_name in grade_names.items():
        grade_data = filters.get(grade_key, {})
        stocks = grade_data.get('stocks', [])
        if stocks:
            text += f"--- {grade_name} ({len(stocks)}只) ---\n"
            for s in stocks[:5]:
                code = s.get('code', '')
                name = s.get('name', '')
                score = s.get('enhanced_score', s.get('score', 0))
                change = s.get('change_pct', 0)
                text += f"  {code} {name} 评分:{score} 涨幅:{change:+.1f}%\n"
            if len(stocks) > 5:
                text += f"  ... 还有{len(stocks)-5}只\n"
            text += "\n"
    
    s_count = stats.get('s_count', len(filters.get('s_grade', {}).get('stocks', [])))
    a_count = stats.get('a_count', len(filters.get('a_grade', {}).get('stocks', [])))
    text += f"统计: S级{s_count}只, A级{a_count}只\n"
    
    return text, data

def get_market_analysis_report():
    """获取大盘分析报告"""
    today = datetime.now().strftime('%Y%m%d')
    report_file = project_root / 'reports' / f'market_analysis_{today}.json'
    
    if not report_file.exists():
        return None, None
    
    with open(report_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    summary = data.get('summary', {})
    indices = data.get('indices', [])
    
    text = f"【今日大盘分析】\n\n"
    
    for idx in indices[:3]:
        text += f"{idx.get('name', '')} ({idx.get('code', '')})\n"
        text += f"  收盘: {idx.get('levels', {}).get('close', 0)}\n"
        text += f"  CVD: {idx.get('cvd', {}).get('signal', '')}\n"
        text += f"  结论: {idx.get('analysis', {}).get('conclusion', '')}\n\n"
    
    text += f"市场观点: {summary.get('market_view', '')}\n"
    
    return text, data

def main():
    print('=' * 50)
    print('早上报告发送')
    print(f'时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 50)
    print()
    
    picks_text, picks_data = get_yesterday_picks_report()
    market_text, market_data = get_market_analysis_report()
    
    if not picks_text and not market_text:
        print('没有可发送的报告')
        return
    
    content = "早安！以下是今日参考报告：\n\n"
    content += "=" * 40 + "\n\n"
    
    if market_text:
        content += market_text
        content += "\n" + "=" * 40 + "\n\n"
    
    if picks_text:
        content += picks_text
    
    content += "\n" + "=" * 40 + "\n"
    content += "\n祝投资顺利！"
    
    print(content)
    print()
    
    email_service = EmailService()
    
    recipients_str = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')
    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    
    if not recipients:
        print('未配置收件人')
        return
    
    subject = f"XCNStock 早报 - {datetime.now().strftime('%Y-%m-%d')}"
    
    print(f'发送邮件到: {recipients}')
    print(f'主题: {subject}')
    
    result = email_service.send(
        to_addrs=recipients,
        subject=subject,
        content=content
    )
    
    if result:
        print('邮件发送成功')
    else:
        print('邮件发送失败')

if __name__ == '__main__':
    main()
