import logging
from core.logger import setup_logger, get_signal_logger


def test_setup_logger():
    """测试日志器设置"""
    logger = setup_logger("test_logger")
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO


def test_get_signal_logger():
    """测试信号日志器"""
    logger = get_signal_logger()
    assert logger.name == "signal"
    assert len(logger.handlers) > 0
