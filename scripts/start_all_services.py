#!/usr/bin/env python
"""
启动所有微服务
"""
import subprocess
import sys
import time
import os

# 服务配置
SERVICES = [
    {
        "name": "Data Service",
        "path": "services/data_service/main.py",
        "port": 8001
    },
    {
        "name": "Stock Service", 
        "path": "services/stock_service/main.py",
        "port": 8002
    },
    {
        "name": "Limit Service",
        "path": "services/limit_service/main.py",
        "port": 8003
    },
    {
        "name": "Notify Service",
        "path": "services/notify_service/main.py",
        "port": 8004
    },
    {
        "name": "API Gateway",
        "path": "gateway/main.py",
        "port": 8000
    }
]

processes = []

def start_service(service):
    """启动单个服务"""
    print(f"启动 {service['name']} (端口 {service['port']})...")
    
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", 
         f"{service['path'].replace('/', '.').replace('.py', '')}:app",
         "--host", "0.0.0.0",
         "--port", str(service['port']),
         "--reload"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    
    return process

def main():
    print("=" * 50)
    print("XCNStock 股票分析系统")
    print("=" * 50)
    
    for service in SERVICES:
        process = start_service(service)
        processes.append((service['name'], process))
        time.sleep(1)  # 间隔启动
    
    print("\n所有服务已启动!")
    print("-" * 50)
    print("API网关: http://127.0.0.1:8000")
    print("API文档: http://127.0.0.1:8000/docs")
    print("-" * 50)
    print("\n按 Ctrl+C 停止所有服务")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n正在停止所有服务...")
        for name, process in processes:
            print(f"停止 {name}...")
            process.terminate()
        print("所有服务已停止")

if __name__ == "__main__":
    main()