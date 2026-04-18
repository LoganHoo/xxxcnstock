#!/usr/bin/env python3
"""
资金行为学策略 - 兜底报告生成
当核心任务失败时，生成简化版报告确保不空窗

使用方法:
    python scripts/pipeline/fund_behavior_fallback.py
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import json

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger

logger = setup_logger(
    name="fund_behavior_fallback",
    level="INFO",
    log_file="system/fund_behavior_fallback.log"
)


def generate_fallback_report():
    """生成兜底报告"""
    logger.info("=" * 60)
    logger.info("生成资金行为学策略兜底报告")
    logger.info("=" * 60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 兜底报告内容
    report = {
        "date": today,
        "type": "fallback",
        "message": "核心任务执行失败，此为兜底报告",
        "recommendations": [
            {
                "type": "warning",
                "content": "⚠️ 今日数据可能不完整，建议谨慎决策"
            },
            {
                "type": "info",
                "content": "📊 建议参考昨日复盘报告和大盘走势"
            },
            {
                "type": "action",
                "content": "🔧 技术团队已收到告警，正在排查问题"
            }
        ],
        "stocks": [],  # 无具体推荐
        "timestamp": datetime.now().isoformat()
    }
    
    # 保存报告
    report_dir = project_root / "data" / "reports"
    report_dir.mkdir(exist_ok=True, parents=True)
    
    report_file = report_dir / f"fund_behavior_fallback_{today}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"兜底报告已保存: {report_file}")
    
    # 发送通知
    send_fallback_notification(report)
    
    return report


def send_fallback_notification(report: dict):
    """发送兜底通知"""
    try:
        from services.notification_sender import send_notification
        
        title = f"🟡 资金行为学策略 - 兜底报告 ({report['date']})"
        
        content = f"""
⚠️ 核心任务执行异常，已生成兜底报告

时间: {report['timestamp']}
状态: 使用兜底策略

建议:
"""
        for rec in report['recommendations']:
            content += f"  {rec['content']}\n"
        
        content += "\n技术团队已收到告警并处理中..."
        
        send_notification(title, content)
        logger.info("兜底通知已发送")
        
    except Exception as e:
        logger.error(f"发送兜底通知失败: {e}")


def main():
    """主函数"""
    logger.info("启动兜底报告生成")
    
    try:
        report = generate_fallback_report()
        logger.info("兜底报告生成完成")
        return 0
    except Exception as e:
        logger.exception("兜底报告生成失败")
        return 1


if __name__ == "__main__":
    sys.exit(main())
