"""启动每日任务调度器"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import signal
from services.data_service.scheduler import DailyScheduler

scheduler = DailyScheduler()

def signal_handler(sig, frame):
    scheduler.stop()
    exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    scheduler.start()
    
    # 保持运行
    import time
    while True:
        time.sleep(60)
