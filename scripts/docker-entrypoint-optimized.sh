#!/bin/bash
set -e

echo "=========================================="
echo "XCNStock 数据采集定时任务服务启动 - 优化版"
echo "=========================================="
echo "当前时间: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "时区: $TZ"
echo ""

echo "检查前置条件..."
python scripts/scheduled_fetch_optimized.py --check-only 2>/dev/null || true

echo ""
echo "定时任务配置:"
crontab -l
echo ""

echo "启动 cron 服务..."
service cron start

echo "cron 服务已启动，等待定时任务执行..."
echo "日志文件: /app/logs/cron.log"
echo "优化版日志: /app/logs/scheduled_fetch_optimized.log"
echo "每日报告: /app/logs/daily_report.json"
echo ""

tail -f /app/logs/cron.log /app/logs/scheduled_fetch_optimized.log
