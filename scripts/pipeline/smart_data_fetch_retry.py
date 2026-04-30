#!/usr/bin/env python3
"""
智能数据采集重试脚本
每15分钟检查一次数据审计状态，如未通过则执行断点续传
直到审计通过或达到最大重试次数/截止时间

使用方法:
    python scripts/pipeline/smart_data_fetch_retry.py
"""
import sys
import os
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger
from scripts.pipeline.progress_helper import ProgressReporter

logger = setup_logger(
    name="smart_data_fetch_retry",
    level="INFO",
    log_file="pipeline/smart_data_fetch_retry.log"
)

# 配置
CHECK_INTERVAL = 15 * 60  # 15分钟检查一次
MAX_RETRIES = 8  # 最大重试次数（2小时）
DEADLINE_HOUR = 17  # 截止时间17:00
DEADLINE_MINUTE = 30


def check_audit_status():
    """检查数据审计状态"""
    # 检查审计报告文件
    today = datetime.now().strftime('%Y-%m-%d')
    audit_report_path = project_root / "data" / "reports" / f"data_audit_{today}.json"
    
    if not audit_report_path.exists():
        logger.info(f"审计报告不存在: {audit_report_path}")
        return False, "audit_not_found"
    
    try:
        import json
        with open(audit_report_path, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        status = report.get('status', 'unknown')
        passed = status == 'passed'
        
        if passed:
            logger.info(f"✅ 数据审计已通过")
        else:
            logger.info(f"⚠️ 数据审计未通过: {status}")
        
        return passed, status
    except Exception as e:
        logger.error(f"读取审计报告失败: {e}")
        return False, "read_error"


def run_data_fetch_retry(reporter=None):
    """执行数据断点续传"""
    logger.info("🔄 执行数据断点续传...")
    if reporter is None:
        reporter = ProgressReporter("data_fetch_retry")
        reporter.start("启动智能重试", progress=0)

    reporter.update(60, "执行断点续传")
    script_path = project_root / "scripts" / "pipeline" / "data_collect_with_validation.py"
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path), "--retry-failed", "--batch-size", "100"],
            capture_output=True,
            text=True,
            timeout=3600,  # 60分钟超时
            cwd=str(project_root)
        )
        
        if result.returncode == 0:
            logger.info("✅ 断点续传成功")
            reporter.complete("断点续传成功")
            return True
        else:
            logger.error(f"❌ 断点续传失败: {result.stderr[:500]}")
            reporter.fail("断点续传失败", extra={"stderr": result.stderr[:500]})
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("⏱️ 断点续传超时")
        reporter.fail("断点续传超时")
        return False
    except Exception as e:
        logger.error(f"执行断点续传异常: {e}")
        reporter.fail("断点续传异常", extra={"error": str(e)})
        return False


def check_deadline():
    """检查是否到达截止时间"""
    now = datetime.now()
    deadline = now.replace(hour=DEADLINE_HOUR, minute=DEADLINE_MINUTE, second=0, microsecond=0)
    
    if now > deadline:
        return True, f"已到达截止时间 {DEADLINE_HOUR:02d}:{DEADLINE_MINUTE:02d}"
    
    time_left = deadline - now
    return False, f"距离截止还有 {time_left.seconds // 60} 分钟"


def main():
    """主函数 - 智能重试循环"""
    reporter = ProgressReporter("data_fetch_retry")
    reporter.start("启动智能数据采集重试", progress=0)
    logger.info("=" * 70)
    logger.info("🚀 启动智能数据采集重试")
    logger.info(f"   检查间隔: {CHECK_INTERVAL // 60}分钟")
    logger.info(f"   最大重试: {MAX_RETRIES}次")
    logger.info(f"   截止时间: {DEADLINE_HOUR:02d}:{DEADLINE_MINUTE:02d}")
    logger.info("=" * 70)
    
    retry_count = 0
    
    while retry_count < MAX_RETRIES:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"\n📍 第 {retry_count + 1}/{MAX_RETRIES} 轮检查 [{now}]")
        reporter.update(10, f"开始第 {retry_count + 1} 轮审计检查", extra={"retry_count": retry_count})
        
        # 1. 检查截止时间
        is_deadline, deadline_msg = check_deadline()
        logger.info(f"   截止时间检查: {deadline_msg}")
        
        if is_deadline:
            logger.warning("⏰ 到达截止时间，停止重试")
            reporter.fail(deadline_msg)
            return 1
        
        # 2. 检查审计状态
        audit_passed, audit_status = check_audit_status()
        reporter.update(30, f"审计状态: {audit_status}", extra={"retry_count": retry_count})
        
        if audit_passed:
            logger.info("🎉 数据审计已通过，无需重试")
            reporter.complete("数据审计已通过")
            return 0
        
        # 3. 执行断点续传
        logger.info(f"   审计状态: {audit_status}，执行断点续传...")
        success = run_data_fetch_retry(reporter=reporter)
        
        if not success:
            logger.warning("⚠️ 断点续传执行失败，将在下一轮重试")
        
        retry_count += 1
        
        # 4. 等待下一轮检查
        if retry_count < MAX_RETRIES:
            next_check = (datetime.now() + timedelta(seconds=CHECK_INTERVAL)).strftime('%H:%M:%S')
            logger.info(f"   ⏳ 等待 {CHECK_INTERVAL // 60} 分钟，下次检查: {next_check}")
            time.sleep(CHECK_INTERVAL)
    
    # 达到最大重试次数
    logger.warning(f"⚠️ 达到最大重试次数 {MAX_RETRIES}，停止重试")
    
    # 最后检查一次审计状态
    audit_passed, _ = check_audit_status()
    if audit_passed:
        logger.info("🎉 最终检查: 数据审计已通过")
        reporter.complete("最终检查通过")
        return 0
    else:
        logger.error("❌ 最终检查: 数据审计仍未通过")
        reporter.fail("最终检查未通过")
        return 1


if __name__ == "__main__":
    sys.exit(main())
