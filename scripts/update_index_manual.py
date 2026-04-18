"""手动更新大盘指数数据

当自动采集因网络问题失败时，使用此脚本手动更新指数数据
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional


def update_index_data(index_code: str, index_name: str, data: Dict) -> bool:
    """更新单个指数数据
    
    Args:
        index_code: 指数代码 (如 '000001')
        index_name: 指数名称
        data: 包含 open, high, low, close, volume 的字典
        
    Returns:
        bool: 更新成功返回True
    """
    try:
        index_dir = Path('data/index')
        index_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_file = index_dir / f"{index_code}.parquet"
        
        # 读取现有数据
        if parquet_file.exists():
            df = pl.read_parquet(parquet_file)
            print(f"  读取现有数据: {len(df)} 条")
        else:
            print(f"  创建新数据文件")
            df = pl.DataFrame(schema={
                'trade_date': pl.Date,
                'open': pl.Float64,
                'high': pl.Float64,
                'low': pl.Float64,
                'close': pl.Float64,
                'volume': pl.Int64,
                'code': pl.Utf8,
                'name': pl.Utf8
            })
        
        today = date.today()
        
        # 检查今天数据是否已存在
        existing_dates = df['trade_date'].to_list()
        if today in existing_dates:
            print(f"  今日数据已存在，更新数据")
            # 删除旧数据
            df = df.filter(pl.col('trade_date') != today)
        
        # 创建新数据行
        new_row = pl.DataFrame({
            'trade_date': [today],
            'open': [float(data.get('open', data['close']))],
            'high': [float(data.get('high', data['close']))],
            'low': [float(data.get('low', data['close']))],
            'close': [float(data['close'])],
            'volume': [int(data.get('volume', 0))],
            'code': [f"sh{index_code}" if index_code.startswith('0') else f"sz{index_code}"],
            'name': [index_name]
        })
        
        # 合并数据
        df = pl.concat([df, new_row])
        df = df.sort('trade_date')
        
        # 保存
        df.write_parquet(parquet_file)
        
        print(f"  ✅ 更新成功: {today} 收盘={data['close']}")
        print(f"  数据总量: {len(df)} 条")
        return True
        
    except Exception as e:
        print(f"  ❌ 更新失败: {e}")
        return False


def main():
    """主函数 - 示例数据更新"""
    print("=== 手动更新大盘指数数据 ===\n")
    
    # 示例：更新上证指数（使用用户提供的数据）
    # 实际使用时，可以修改这些值或从命令行参数传入
    
    today_data = {
        '000001': {
            'name': '上证指数',
            'open': 3939.56,  # 根据昨日数据
            'high': 4066.90,  # 估算
            'low': 3986.40,   # 估算
            'close': 4026.63,
            'volume': 239680000000  # 23968亿
        },
        '399006': {
            'name': '创业板指',
            'open': 3170.00,
            'high': 3250.00,
            'low': 3150.00,
            'close': 3172.65,
            'volume': 85000000000
        },
        '000300': {
            'name': '沪深300',
            'open': 4450.00,
            'high': 4550.00,
            'low': 4420.00,
            'close': 4478.91,
            'volume': 120000000000
        }
    }
    
    print("使用示例数据更新（实际使用时请修改脚本中的数据）\n")
    
    for code, data in today_data.items():
        print(f"更新 {data['name']} ({code})...")
        update_index_data(code, data['name'], data)
        print()
    
    print("=== 更新完成 ===")
    print("\n提示：如需更新为真实数据，请修改脚本中的 today_data 字典")


if __name__ == "__main__":
    main()
