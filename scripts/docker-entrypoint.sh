#!/bin/bash
set -e

echo "=========================================="
echo "XCNStock 数据分析定时任务服务启动"
echo "=========================================="
echo "当前时间: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "时区: $TZ"
echo ""

echo "环境变量:"
echo "  MYSQL_HOST: ${MYSQL_HOST:-未设置}"
echo "  MYSQL_PORT: ${MYSQL_PORT:-未设置}"
echo "  MYSQL_DATABASE: ${MYSQL_DATABASE:-未设置}"
echo ""

echo "定时任务配置:"
cat /etc/cron.d/xcnstock
echo ""

echo "应用 cron 配置..."
crontab /etc/cron.d/xcnstock

echo "当前 crontab:"
crontab -l
echo ""

echo "启动 cron 服务..."
service cron start

echo "cron 服务已启动，等待定时任务执行..."
echo "日志文件: /app/logs/cron.log"
echo ""

touch /app/logs/cron.log
tail -f /app/logs/cron.log
