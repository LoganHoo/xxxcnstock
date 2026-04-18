"""数据采集任务 - 16:00执行（收盘后）

此任务封装了 data_collect.py 的调用，统一使用微服务架构进行数据采集。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import subprocess
from pathlib import Path
from .base_task import BaseTask


class DataCollectTask(BaseTask):
    """数据采集任务 - 调用 data_collect.py"""

    name = "data_collect"
    description = "采集实时行情和K线数据"

    def run(self) -> bool:
        """
        执行数据采集任务
        
        调用 scripts/pipeline/data_collect.py 进行统一采集
        """
        try:
            project_root = Path(__file__).parent.parent.parent
            data_collect_script = project_root / "scripts" / "pipeline" / "data_collect.py"
            
            if not data_collect_script.exists():
                self.logger.error(f"数据采集脚本不存在: {data_collect_script}")
                return False
            
            self.logger.info("启动数据采集 (调用 data_collect.py)...")
            
            # 调用 data_collect.py
            cmd = [
                sys.executable,
                str(data_collect_script)
            ]
            
            self.logger.info(f"执行命令: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200  # 2小时超时
            )
            
            # 记录输出
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        self.logger.info(f"[data_collect] {line}")
            
            if result.stderr:
                for line in result.stderr.strip().split('\n'):
                    if line.strip():
                        self.logger.warning(f"[data_collect] {line}")
            
            if result.returncode == 0:
                self.logger.info("数据采集完成")
                return True
            else:
                self.logger.error(f"数据采集失败，返回码: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("数据采集超时")
            return False
        except Exception as e:
            self.logger.error(f"数据采集异常: {e}")
            return False


# 兼容旧版本的直接调用
def run_data_collect():
    """兼容旧版本的调用方式"""
    task = DataCollectTask()
    return task.run()


if __name__ == "__main__":
    task = DataCollectTask()
    success = task.run()
    sys.exit(0 if success else 1)
