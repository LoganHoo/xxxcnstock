import os
import smtplib
from email.mime.text import MIMEText

report = """【流水线执行报告】2026-04-02 17:00

执行结果：
1. data_collect.py     ✅ 完成 (5393只已是最新)
2. data_audit.py       ✅ 完成 (涨停20只,跌停4只)
3. daily_review.py     ✅ 完成 (震荡/防守,仓位100万)
4. stock_pick.py       ✅ 完成 (S级0只,A级0只)
5. morning_push.py     ✅ 邮件已发送
6. precompute.py       ❌ 失败 (300688.parquet损坏) - 已修复
7. night_picks.py      ✅ 完成 (S级2166只,A级1836只)
8. morning_update.py   ⚠️ 外盘API失败(网络问题)
9. send_morning.py     ✅ 无可发送报告
10. fetch_index.py     ✅ 指数更新完成
11. update_tracking.py ✅ 21只跟踪更新

待处理：
- 外盘数据API网络连接问题

数据摘要：
- 最新数据: 2026-04-02
- 有效股票: 10124只
- 涨停: 20只, 跌停: 4只
"""

smtp_server = os.getenv('EMAIL_SMTP_SERVER', 'smtp.qq.com')
smtp_port = int(os.getenv('EMAIL_SMTP_PORT', 465))
smtp_user = os.getenv('EMAIL_USERNAME', '')
smtp_password = os.getenv('EMAIL_PASSWORD', '')
recipients = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com').split(',')

msg = MIMEText(report, 'plain', 'utf-8')
msg['Subject'] = '【流水线报告】2026-04-02'
msg['From'] = smtp_user
msg['To'] = ', '.join([r.strip() for r in recipients])

try:
    with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, [r.strip() for r in recipients], msg.as_string())
    print('发送成功')
except Exception as e:
    print(f'发送失败: {e}')