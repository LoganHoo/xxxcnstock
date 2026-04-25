#!/bin/bash
# XCNStock 调度器启动脚本

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$PROJECT_DIR/data/scheduler.pid"
LOG_FILE="$PROJECT_DIR/logs/scheduler.log"

# 创建日志目录
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/data"

# 检查是否已在运行
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "调度器已在运行 (PID: $PID)"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

echo "🚀 启动 XCNStock 任务调度器..."

# 启动调度器
cd "$PROJECT_DIR"
nohup python3 scripts/enhanced_scheduler.py >> "$LOG_FILE" 2>&1 &

# 保存PID
echo $! > "$PID_FILE"
echo "✅ 调度器已启动 (PID: $(cat $PID_FILE))"
echo "📋 日志文件: $LOG_FILE"
echo "📊 查看日志: tail -f $LOG_FILE"
