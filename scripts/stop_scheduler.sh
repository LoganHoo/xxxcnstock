#!/bin/bash
# XCNStock 调度器停止脚本

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$PROJECT_DIR/data/scheduler.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    echo "📴 停止调度器 (PID: $PID)..."
    kill "$PID" 2>/dev/null
    
    # 等待进程结束
    for i in {1..10}; do
        if ! ps -p "$PID" > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    
    # 强制终止
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  强制终止..."
        kill -9 "$PID" 2>/dev/null
    fi
    
    rm -f "$PID_FILE"
    echo "✅ 调度器已停止"
else
    echo "调度器未运行"
fi
