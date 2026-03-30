#!/usr/bin/env python3
"""
测试数据新鲜度检查装饰器
"""
import logging
from pathlib import Path
from core.freshness_check_decorator import check_data_freshness

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@check_data_freshness
def test_function():
    """测试函数"""
    print("测试函数执行中...")
    print("测试函数执行完成")

if __name__ == '__main__':
    print("开始测试装饰器...")
    test_function()
    print("装饰器测试完成")
