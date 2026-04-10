#!/bin/bash
# Cron Daemon 健康检查脚本
# 功能：监控 cron 进程是否存活，如崩溃则重启容器并告警
# 建议 crontab: */5 * * * * /app/scripts/cron_health_check.sh >> /app/logs/cron_health.log 2>&1

LOG_FILE="/app/logs/cron_health.log"
CONTAINER_NAME="xcnstock-cron"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE" 2>/dev/null
}

check_and_restart() {
    local cron_pid
    local cron_state

    # 检查 cron PID 文件
    if [ ! -f /var/run/crond.pid ]; then
        log "❌ Cron PID 文件不存在，需要重启容器"
        restart_container
        return 1
    fi

    cron_pid=$(cat /var/run/crond.pid 2>/dev/null)
    if [ -z "$cron_pid" ]; then
        log "❌ Cron PID 为空，需要重启容器"
        restart_container
        return 1
    fi

    # 检查 cron 进程状态
    if [ ! -d /proc/$cron_pid ]; then
        log "❌ Cron 进程 $cron_pid 不存在，需要重启容器"
        restart_container
        return 1
    fi

    cron_state=$(cat /proc/$cron_pid/status 2>/dev/null | grep "State:" | awk '{print $2}')
    if [ "$cron_state" != "S" ] && [ "$cron_state" != "R" ]; then
        log "❌ Cron 进程状态异常 (State=$cron_state)，重启容器..."
        restart_container
        return 1
    fi

    log "✅ Cron 运行正常 (PID: $cron_pid, State: $cron_state)"
    return 0
}

restart_container() {
    log "⚠️  尝试重启 cron 容器..."
    # 注意：在容器内部无法执行 docker 命令，需要依赖外部监控
    # 这里只记录日志，由外部 Docker 主机或其他方式处理重启
    log "❌ 容器内部无法执行 docker 命令，请检查外部监控"
}

# 主逻辑
log "========== 开始健康检查 =========="
check_and_restart
log "========== 健康检查完成 =========="