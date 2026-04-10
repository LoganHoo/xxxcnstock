#!/bin/bash
# Cron 容器外部监控脚本
# 在 Docker 主机上运行，监控 cron 容器健康状态
# 建议 crontab: */5 * * * * /path/to/cron_watchdog.sh

CONTAINER_NAME="xcnstock-cron"
LOG_FILE="/var/log/cron_watchdog.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

check_cron() {
    local cron_pid
    local cron_state

    # 检查容器是否运行
    if ! docker ps -q --filter "name=$CONTAINER_NAME" | grep -q .; then
        log "❌ 容器 $CONTAINER_NAME 未运行，尝试启动..."
        docker start "$CONTAINER_NAME"
        log "✅ 容器已启动"
        return 0
    fi

    # 检查 cron PID 文件
    cron_pid=$(docker exec "$CONTAINER_NAME" cat /var/run/crond.pid 2>/dev/null)
    if [ -z "$cron_pid" ]; then
        log "❌ Cron PID 文件不存在，重启容器..."
        docker restart "$CONTAINER_NAME"
        return 1
    fi

    # 检查 cron 进程状态
    cron_state=$(docker exec "$CONTAINER_NAME" cat /proc/$cron_pid/status 2>/dev/null | grep "State:" | awk '{print $2}')
    if [ "$cron_state" != "S" ] && [ "$cron_state" != "R" ]; then
        log "❌ Cron 进程状态异常 (State=$cron_state)，重启容器..."
        docker restart "$CONTAINER_NAME"
        return 1
    fi

    log "✅ Cron 运行正常 (PID: $cron_pid, State: $cron_state)"
    return 0
}

log "========== Cron Watchdog 开始 =========="
check_cron
log "========== Cron Watchdog 完成 =========="