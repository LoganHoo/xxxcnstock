#!/usr/bin/env python
"""
停止所有微服务
"""
import subprocess
import sys

def stop_services():
    """停止所有服务"""
    print("正在停止所有XCNStock服务...")
    
    # Windows: 使用taskkill停止Python进程
    try:
        # 查找占用端口的进程
        ports = [8000, 8001, 8002, 8003, 8004]
        for port in ports:
            result = subprocess.run(
                f"netstat -ano | findstr :{port}",
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 5:
                        pid = parts[-1]
                        print(f"停止端口 {port} 的进程 (PID: {pid})")
                        subprocess.run(f"taskkill /F /PID {pid}", shell=True)
        
        print("所有服务已停止")
        
    except Exception as e:
        print(f"停止服务时出错: {e}")

if __name__ == "__main__":
    stop_services()
