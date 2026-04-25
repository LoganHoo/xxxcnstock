#!/usr/bin/env python3
"""调试脚本 - 检查容器环境和数据状态"""
import sys
from pathlib import Path
from datetime import datetime
import os

print("=" * 60)
print("调试信息")
print("=" * 60)

# 1. 检查当前时间
print(f"\n1. 当前时间信息:")
print(f"   datetime.now(): {datetime.now()}")
print(f"   日期: {datetime.now().strftime('%Y-%m-%d')}")

# 2. 检查可用模块
print(f"\n2. 检查可用模块:")
try:
    import pandas
    print(f"   pandas: ✅ {pandas.__version__}")
except ImportError:
    print(f"   pandas: ❌ 未安装")

try:
    import pyarrow
    print(f"   pyarrow: ✅ {pyarrow.__version__}")
except ImportError:
    print(f"   pyarrow: ❌ 未安装")

try:
    import polars
    print(f"   polars: ✅ {polars.__version__}")
except ImportError:
    print(f"   polars: ❌ 未安装")

# 3. 检查数据目录
project_root = Path(__file__).parent.parent.parent
kline_dir = project_root / "data" / "kline"
print(f"\n3. 数据目录:")
print(f"   kline_dir: {kline_dir}")
print(f"   exists: {kline_dir.exists()}")

if kline_dir.exists():
    files = list(kline_dir.glob("*.parquet"))
    print(f"   parquet文件数量: {len(files)}")

print("\n" + "=" * 60)

def main():
    pass
