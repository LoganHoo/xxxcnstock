#!/bin/bash
set -e

echo "=========================================="
echo "XCNStock 数据分析定时任务服务启动"
echo "当前时间: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "时区: $TZ"
echo ""

echo "环境变量:"
echo "  MYSQL_HOST: ${MYSQL_HOST:-未设置}"
echo "  MYSQL_PORT: ${MYSQL_PORT:-未设置}"
echo "  MYSQL_DATABASE: ${MYSQL_DATABASE:-未设置}"
echo ""

echo "定时任务配置:"
cat /etc/cron.d/xcnstock 2>/dev/null || echo "无cron配置"
echo ""

echo "应用 cron 配置..."
crontab /etc/cron.d/xcnstock 2>/dev/null || true

echo "当前 crontab:"
crontab -l 2>/dev/null || echo "无crontab"
echo ""

echo "启动 cron 服务..."
mkdir -p /var/run
cron

echo "验证 cron 进程..."
if [ -f /var/run/crond.pid ]; then
    echo "✅ cron 服务运行正常"
else
    echo "❌ cron 服务启动失败，尝试直接启动..."
    /usr/sbin/cron &
    sleep 2
fi

if [ -f /var/run/crond.pid ]; then
    echo "✅ cron 服务运行正常"
else
    echo "⚠️  cron 进程状态未知，但服务已启动"
fi

echo ""
echo "容器保持运行，定时任务将在指定时间执行..."
echo "日志文件: /app/logs/cron.log"
echo ""

exec tail -f /dev/null