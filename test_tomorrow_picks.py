#!/usr/bin/env python3
"""
测试 tomorrow_picks.py 中的 main 函数
"""
import logging
import sys
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 导入 main 函数
from scripts.tomorrow_picks import main

if __name__ == '__main__':
    print("开始测试 tomorrow_picks.py 中的 main 函数...")
    main()
    print("测试完成")
