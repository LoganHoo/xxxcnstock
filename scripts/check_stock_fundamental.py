#!/usr/bin/env python3
"""
个股基本面检查工具
================================================================================
功能：
- 随机抽取股票展示基本面数据
- 支持指定股票代码查询
- 整合估值数据和股票基本信息

使用方法：
    python scripts/check_stock_fundamental.py [--code CODE] [--count N]
================================================================================
"""
import sys
import argparse
import random
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl

from core.logger import setup_logger

logger = setup_logger(
    name="check_fundamental",
    level="INFO",
    log_file="system/check_fundamental.log"
)


def load_data():
    """加载所需数据"""
    data_dir = project_root / "data"
    fundamental_dir = data_dir / "fundamental"
    
    # 加载股票列表
    stock_list = pl.read_parquet(data_dir / "stock_list.parquet")
    
    # 加载估值数据
    valuation = pl.read_parquet(fundamental_dir / "valuation_realistic.parquet")
    
    # 合并数据
    merged = stock_list.join(valuation, on="code", how="left")
    
    return merged


def format_value(value, unit="", decimals=2):
    """格式化数值"""
    if value is None:
        return "--"
    try:
        num = float(value)
        if decimals == 0:
            return f"{int(num)}{unit}"
        return f"{num:.{decimals}f}{unit}"
    except:
        return str(value)


def get_valuation_level(value, indicator):
    """获取估值水平"""
    if value is None:
        return ""
    
    try:
        num = float(value)
        if indicator == "pe":
            if num < 15:
                return "[低估]"
            elif num > 30:
                return "[高估]"
            else:
                return "[合理]"
        elif indicator == "pb":
            if num < 1:
                return "[低估]"
            elif num > 3:
                return "[高估]"
            else:
                return "[合理]"
    except:
        pass
    
    return ""


def display_stock_fundamental(row):
    """显示单只股票基本面"""
    code = row.get('code', 'N/A')
    name = row.get('name', 'N/A')
    industry = row.get('industry', 'N/A')
    
    print(f"\n{'=' * 70}")
    print(f"📈 {code} {name}")
    print(f"{'=' * 70}")
    print(f"  所属行业: {industry}")
    print()
    
    # 估值指标
    print("  【估值指标】")
    pe = row.get('pe_ttm')
    pb = row.get('pb')
    ps = row.get('ps_ttm')
    total_mv = row.get('total_mv')
    
    print(f"    市盈率(PE): {format_value(pe, ' 倍')} {get_valuation_level(pe, 'pe')}")
    print(f"    市净率(PB): {format_value(pb, ' 倍')} {get_valuation_level(pb, 'pb')}")
    print(f"    市销率(PS): {format_value(ps, ' 倍')}")
    print(f"    总市值: {format_value(total_mv, ' 亿元')}")
    
    # 估值分析
    print()
    print("  【估值分析】")
    if pe and pb:
        if pe < 15 and pb < 1:
            print("    💚 整体估值偏低，可能存在投资机会")
        elif pe > 30 and pb > 3:
            print("    🔴 整体估值偏高，注意风险")
        else:
            print("    🟡 估值处于合理区间")
    else:
        print("    ⚪ 数据不足，无法判断")


def check_random_stocks(count=5):
    """随机检查多只股票"""
    print("=" * 70)
    print(f"🎲 随机抽查 {count} 只股票的基本面")
    print("=" * 70)
    
    df = load_data()
    
    # 过滤掉有退市/ST风险的股票
    df_filtered = df.filter(
        ~pl.col('name').str.contains(r'退市|ST|\*ST')
    )
    
    print(f"\n数据范围: {len(df_filtered)} 只正常交易股票")
    
    # 随机抽取
    sample = df_filtered.sample(count, seed=random.randint(1, 10000))
    
    for row in sample.iter_rows(named=True):
        display_stock_fundamental(row)
    
    print(f"\n{'=' * 70}")
    print("📊 估值参考标准:")
    print("  - PE < 15: 低估 | 15-30: 合理 | > 30: 高估")
    print("  - PB < 1: 低估 | 1-3: 合理 | > 3: 高估")
    print("=" * 70)


def check_specific_stock(code):
    """检查指定股票"""
    df = load_data()
    
    # 查找股票
    stock = df.filter(pl.col('code') == code)
    
    if len(stock) == 0:
        print(f"❌ 未找到股票代码: {code}")
        return
    
    row = stock.to_dicts()[0]
    display_stock_fundamental(row)


def main():
    parser = argparse.ArgumentParser(description='个股基本面检查工具')
    parser.add_argument('--code', type=str, help='指定股票代码查询')
    parser.add_argument('--count', type=int, default=5, help='随机抽查数量 (默认5只)')
    args = parser.parse_args()
    
    if args.code:
        check_specific_stock(args.code)
    else:
        check_random_stocks(args.count)


if __name__ == "__main__":
    main()
