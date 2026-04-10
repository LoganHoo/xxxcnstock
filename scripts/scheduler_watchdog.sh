#!/bin/bash
# APScheduler 心跳监控脚本 - 运行在 Cron 容器内
# 功能：检测 APScheduler 心跳，如超时则接管任务执行
# crontab: */5 * * * * /app/scripts/scheduler_watchdog.sh >> /app/logs/watchdog.log 2>&1

LOG_FILE="/app/logs/watchdog.log"
HEARTBEAT_FILE="/app/logs/scheduler_heartbeat"
TIMEOUT_SECONDS=600
SCHEDULER_CONTAINER="xcnstock-scheduler"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE" 2>/dev/null
}

check_scheduler_heartbeat() {
    if [ ! -f "$HEARTBEAT_FILE" ]; then
        log "❌ 心跳文件不存在: $HEARTBEAT_FILE"
        return 1
    fi

    local last_heartbeat=$(cat "$HEARTBEAT_FILE")
    local heartbeat_time=$(date -d "$last_heartbeat" +%s 2>/dev/null)
    local current_time=$(date +%s)

    if [ -z "$heartbeat_time" ]; then
        log "❌ 心跳时间解析失败: $last_heartbeat"
        return 1
    fi

    local diff=$((current_time - heartbeat_time))
    log "📊 心跳检测: ${diff}秒前 (超时阈值: ${TIMEOUT_SECONDS}秒)"

    if [ $diff -gt $TIMEOUT_SECONDS ]; then
        log "❌ 心跳超时，APScheduler 可能已崩溃"
        return 1
    fi

    log "✅ APScheduler 心跳正常"
    return 0
}

takeover_tasks() {
    log "⚠️  尝试接管任务执行..."

    local container_status=$(docker inspect -f '{{.State.Running}}' $SCHEDULER_CONTAINER 2>/dev/null)
    if [ "$container_status" = "true" ]; then
        log "📦 Scheduler 容器仍在运行，尝试重启..."
        docker restart $SCHEDULER_CONTAINER
        log "✅ Scheduler 容器已重启"
    else
        log "❌ Scheduler 容器已停止，启动新容器..."
        docker start $SCHEDULER_CONTAINER
        log "✅ Scheduler 容器已启动"
    fi
}

log "========== Watchdog 开始 =========="

if check_scheduler_heartbeat; then
    log "✅ Scheduler 运行正常，无需接管"
else
    takeover_tasks
fi

log "========== Watchdog 完成 =========="