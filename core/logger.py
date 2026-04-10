import sys
from pathlib import Path
from loguru import logger
from core.config import get_settings


def setup_logger(
    name: str = None,
    level: str = "INFO",
    log_file: str = None,
    rotation: str = "00:00",
    retention: str = "30 days",
    compression: str = "zip"
) -> "logger":
    """
    配置loguru日志器

    Args:
        name: 日志器名称 (可选，默认使用root logger)
        level: 日志级别 (DEBUG, INFO, WARNING, ERROR)
        log_file: 日志文件路径 (可选，不设置则仅输出到控制台)
        rotation: 日志轮转时间 (默认每日00:00)
        retention: 日志保留时间 (默认30天)
        compression: 日志压缩格式 (默认zip)

    Returns:
        配置好的loguru logger实例
    """
    settings = get_settings()

    removal_action = logger.remove

    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    logger.add(
        sys.stdout,
        format=log_format,
        level=level,
        colorize=True,
        backtrace=True,
        diagnose=False
    )

    if log_file:
        log_path = Path(settings.LOG_DIR) / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_path,
            format=log_format,
            level=level,
            rotation=rotation,
            retention=retention,
            compression=compression,
            encoding="utf-8",
            backtrace=True,
            diagnose=False
        )

    if name:
        return logger.bind(name=name)
    return logger


def get_logger(name: str = None) -> "logger":
    """
    获取loguru日志器

    Args:
        name: 日志器名称 (可选)

    Returns:
        loguru logger实例
    """
    if name:
        return logger.bind(name=name)
    return logger


def get_signal_logger(name: str = "signal") -> "logger":
    """
    获取信号专用日志器

    Args:
        name: 日志器名称

    Returns:
        loguru logger实例
    """
    return logger.bind(name=name)


def get_alert_logger(name: str = "alert") -> "logger":
    """
    获取告警专用日志器

    Args:
        name: 日志器名称

    Returns:
        loguru logger实例
    """
    return logger.bind(name=name)


logger = setup_logger(level="INFO")