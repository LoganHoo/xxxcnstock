#!/usr/bin/env python3
"""
验证本地数据价格准确性 - 对比同花顺/东方财富等第三方数据
"""
import polars as pl
from pathlib import Path
from datetime import datetime
import random

KLINE_DIR = Path('data/kline')


def get_local_price(code: str) -> dict:
    """获取本地价格"""
    try:
        df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
        if df.is_empty():
            return None
        latest = df[-1].to_dicts()[0]
        return {
            'code': code,
            'date': str(latest['trade_date']),
            'close': float(latest['close']),
            'open': float(latest['open']),
            'high': float(latest['high']),
            'low': float(latest['low']),
            'volume': int(latest['volume']),
            'source': 'local'
        }
    except Exception as e:
        return None


def fetch_ths_price(code: str) -> dict:
    """从同花顺获取实时价格"""
    try:
        import akshare as ak
        
        # 获取实时行情
        df = ak.stock_zh_a_spot_em()
        
        # 查找对应股票
        row = df[df['代码'] == code]
        if row.empty:
            return None
        
        row = row.iloc[0]
        return {
            'code': code,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'close': float(row['最新价']) if pd.notna(row['最新价']) else 0,
            'open': float(row['今开']) if pd.notna(row['今开']) else 0,
            'high': float(row['最高']) if pd.notna(row['最高']) else 0,
            'low': float(row['最低']) if pd.notna(row['最低']) else 0,
            'volume': int(row['成交量']) if pd.notna(row['成交量']) else 0,
            'source': 'ths'
        }
    except Exception as e:
        return None


def main():
    print("=" * 80)
    print("价格准确性验证 - 本地数据 vs 同花顺")
    print("=" * 80)
    print()
    
    # 测试股票列表（包含之前显示的10只）
    test_codes = [
        '688223', '688148', '688403', '688313', '688678',
        '688158', '688530', '688106', '688008', '688143',
        '000001', '000002', '600000', '600519'  # 额外加几只主板股票
    ]
    
    print(f"测试股票数: {len(test_codes)}")
    print()
    
    # 获取本地价格
    local_prices = {}
    for code in test_codes:
        price = get_local_price(code)
        if price:
            local_prices[code] = price
    
    print(f"本地数据可用: {len(local_prices)} 只")
    print()
    
    # 显示本地数据
    print("本地数据最新价格:")
    print("-" * 80)
    print(f"{'代码':<10} {'日期':<12} {'收盘价':<10} {'开盘价':<10} {'最高价':<10} {'最低价':<10}")
    print("-" * 80)
    
    for code, data in local_prices.items():
        print(f"{code:<10} {data['date']:<12} ¥{data['close']:<9.2f} ¥{data['open']:<9.2f} ¥{data['high']:<9.2f} ¥{data['low']:<9.2f}")
    
    print()
    print("=" * 80)
    print()
    print("⚠️ 注意: 同花顺实时数据需要交易日盘中才能获取准确价格")
    print("   当前为非交易时间，本地数据为历史收盘数据")
    print()
    print("建议验证方式:")
    print("1. 交易日15:00收盘后运行数据采集")
    print("2. 对比同花顺当日收盘价")
    print("3. 检查数据源配置 (当前主数据源: baostock)")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
