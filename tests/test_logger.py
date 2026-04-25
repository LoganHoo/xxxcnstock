import logging
from core.logger import setup_logger, get_signal_logger


def test_setup_logger():
    """测试日志器设置"""
    logger = setup_logger("test_logger")
    # loguru logger通过bind设置name，检查logger对象正确返回
    assert logger is not None


def test_get_signal_logger():
    """测试信号日志器"""
    logger = get_signal_logger()
    assert logger is not None
