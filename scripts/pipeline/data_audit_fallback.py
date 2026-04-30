#!/usr/bin/env python3
"""
数据审计兜底脚本
当智能数据审计失败时，生成简化版审计报告

使用方法:
    python scripts/pipeline/data_audit_fallback.py
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
    name="data_audit_fallback",
    level="INFO",
    log_file="system/data_audit_fallback.log"
)


def generate_fallback_audit():
    """生成兜底审计报告"""
    logger.info("=" * 60)
    logger.info("生成数据审计兜底报告")
    logger.info("=" * 60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 兜底报告内容
    audit_report = {
        "date": today,
        "type": "fallback_audit",
        "status": "warning",
        "message": "智能数据审计失败，此为兜底报告",
        "checks": {
            "freshness": {"status": "unknown", "message": "无法验证数据新鲜度"},
            "completeness": {"status": "unknown", "message": "无法验证数据完整性"},
            "quality": {"status": "unknown", "message": "无法验证数据质量"}
        },
        "recommendations": [
            "⚠️ 数据审计失败，建议手动检查数据状态",
            "📊 检查K线数据目录是否存在最新数据",
            "🔍 验证数据源连接是否正常"
        ],
        "timestamp": datetime.now().isoformat()
    }
    
    # 保存报告
    report_dir = project_root / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = report_dir / f"data_audit_fallback_{today}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(audit_report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"兜底报告已保存: {report_file}")
    logger.info("=" * 60)
    
    return audit_report


def main():
    """主函数"""
    try:
        report = generate_fallback_audit()
        logger.info("兜底审计报告生成完成")
        return 0
    except Exception as e:
        logger.error(f"生成兜底报告失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
