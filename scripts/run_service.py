#!/usr/bin/env python
"""
启动单个微服务
用法: python scripts/run_service.py [service_name]
service_name: data, stock, limit, notify, gateway
"""
import sys
import subprocess
import os

SERVICES = {
    "data": {
        "module": "services.data_service.main:app",
        "port": 8001
    },
    "stock": {
        "module": "services.stock_service.main:app",
        "port": 8002
    },
    "limit": {
        "module": "services.limit_service.main:app",
        "port": 8003
    },
    "notify": {
        "module": "services.notify_service.main:app",
        "port": 8004
    },
    "gateway": {
        "module": "gateway.main:app",
        "port": 8000
    }
}

def main():
    if len(sys.argv) < 2:
        print("用法: python scripts/run_service.py [service_name]")
        print("可用服务:", ", ".join(SERVICES.keys()))
        sys.exit(1)
    
    service_name = sys.argv[1].lower()
    
    if service_name not in SERVICES:
        print(f"未知服务: {service_name}")
        print("可用服务:", ", ".join(SERVICES.keys()))
        sys.exit(1)
    
    service = SERVICES[service_name]
    print(f"启动 {service_name} 服务 (端口 {service['port']})...")
    
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        service['module'],
        "--host", "0.0.0.0",
        "--port", str(service['port']),
        "--reload"
    ])

if __name__ == "__main__":
    main()
