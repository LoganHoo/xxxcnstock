"""任务基类"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from core.logger import setup_logger


class BaseTask(ABC):
    """定时任务基类"""
    
    name: str = "base_task"
    description: str = "基础任务"
    
    def __init__(self):
        self.logger = setup_logger(f"task.{self.name}", log_file="system/tasks.log")
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def execute(self) -> bool:
        """执行任务"""
        self.start_time = datetime.now()
        self.logger.info(f"开始执行任务: {self.name}")
        
        try:
            result = self.run()
            self.end_time = datetime.now()
            elapsed = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"任务完成: {self.name}, 耗时{elapsed:.2f}秒")
            return result
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.error(f"任务失败: {self.name}, 错误: {e}")
            return False
    
    @abstractmethod
    def run(self) -> bool:
        """具体任务逻辑，子类实现"""
        pass
