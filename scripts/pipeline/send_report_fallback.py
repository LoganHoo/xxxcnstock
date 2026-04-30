#!/usr/bin/env python3
"""
复盘报告推送兜底脚本
当复盘报告推送失败时，生成简化版报告确保不空窗

使用方法:
    python scripts/pipeline/send_report_fallback.py
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
    name="send_report_fallback",
    level="INFO",
    log_file="system/send_report_fallback.log"
)


def generate_fallback_report():
    """生成兜底复盘报告"""
    logger.info("=" * 60)
    logger.info("生成复盘报告兜底版本")
    logger.info("=" * 60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 兜底报告内容
    report = {
        "date": today,
        "type": "fallback_review",
        "title": "📊 每日复盘报告（简化版）",
        "sections": [
            {
                "title": "⚠️ 系统状态",
                "content": "复盘报告生成任务失败，此为兜底版本"
            },
            {
                "title": "📈 大盘概况",
                "content": "数据暂时不可用，请参考实时行情"
            },
            {
                "title": "💡 操作建议",
                "content": "1. 关注市场热点板块\n2. 控制仓位风险\n3. 等待完整数据恢复"
            }
        ],
        "recommendations": [
            {
                "type": "warning",
                "content": "⚠️ 今日复盘数据可能不完整"
            },
            {
                "type": "info",
                "content": "📊 建议参考昨日完整复盘报告"
            }
        ],
        "timestamp": datetime.now().isoformat()
    }
    
    # 保存报告
    report_dir = project_root / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = report_dir / f"review_report_fallback_{today}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"兜底报告已保存: {report_file}")
    
    # 同时生成文本版本便于查看
    text_report = f"""
{'='*60}
📊 每日复盘报告（简化版）- {today}
{'='*60}

⚠️ 系统状态
  复盘报告生成任务失败，此为兜底版本

📈 大盘概况
  数据暂时不可用，请参考实时行情

💡 操作建议
  1. 关注市场热点板块
  2. 控制仓位风险
  3. 等待完整数据恢复

⚠️ 重要提示
  今日复盘数据可能不完整，建议参考昨日完整复盘报告

{'='*60}
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}
"""
    
    text_file = report_dir / f"review_report_fallback_{today}.txt"
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write(text_report)
    
    logger.info(f"文本报告已保存: {text_file}")
    logger.info("=" * 60)
    
    return report


def main():
    """主函数"""
    try:
        report = generate_fallback_report()
        logger.info("兜底复盘报告生成完成")
        return 0
    except Exception as e:
        logger.error(f"生成兜底报告失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
