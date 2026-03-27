import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from core.config import get_settings


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: str = None
) -> logging.Logger:
    """设置日志器"""
    settings = get_settings()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 控制台输出
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # 文件输出
    if log_file:
        log_path = Path(settings.LOG_DIR) / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            backupCount=30,
            encoding="utf-8"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(console_format)
        logger.addHandler(file_handler)
    
    return logger


def get_signal_logger() -> logging.Logger:
    """获取信号专用日志器"""
    settings = get_settings()
    logger = logging.getLogger("signal")
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    # JSON格式文件处理器
    log_path = Path(settings.LOG_DIR) / "signals" / "signals.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=180,
        encoding="utf-8"
    )
    json_format = logging.Formatter(
        '{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}'
    )
    file_handler.setFormatter(json_format)
    logger.addHandler(file_handler)
    
    return logger


def get_alert_logger() -> logging.Logger:
    """获取告警日志器"""
    return setup_logger("alert", log_file="alerts/error.log")
