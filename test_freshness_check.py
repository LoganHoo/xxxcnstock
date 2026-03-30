#!/usr/bin/env python3
"""
测试数据新鲜度检查器
"""
import logging
from core.data_freshness_checker import DataFreshnessChecker
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 确定数据目录
project_root = Path(__file__).parent
data_dir = project_root / "data"

# 初始化数据新鲜度检查器
checker = DataFreshnessChecker(str(data_dir))

# 执行数据新鲜度检查
print("开始执行数据新鲜度检查...")
success = checker.ensure_data_freshness()
print(f"数据新鲜度检查结果: {'成功' if success else '失败'}")
