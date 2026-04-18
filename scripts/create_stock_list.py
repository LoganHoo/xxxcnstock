#!/usr/bin/env python3
"""创建股票代码-名称映射文件"""
import polars as pl
from pathlib import Path

def create_stock_list():
    # 读取CSV文件
    csv_path = Path('data/results/all_stocks_20260330.csv')
    if not csv_path.exists():
        print(f"错误: 文件不存在 {csv_path}")
        return

    # 读取CSV，code列作为字符串
    df = pl.read_csv(csv_path)
    print(f'CSV列名: {df.columns}')
    print(f'code列原始类型: {df["code"].dtype}')

    # 将code列转换为字符串并补齐6位
    df = df.with_columns([
        pl.col('code').cast(pl.Utf8).str.zfill(6).alias('code')
    ])
    print(f'code列转换后类型: {df["code"].dtype}')

    # 选择code和name列，去重
    stock_list = df.select(['code', 'name']).unique()
    print(f'股票数量: {len(stock_list)}')

    # 验证几个特定的股票代码
    codes_to_check = ['000617', '002309', '002217', '000100', '002157']
    for code in codes_to_check:
        result = stock_list.filter(pl.col('code') == code)
        if len(result) > 0:
            print(f'{code}: {result["name"][0]}')
        else:
            print(f'{code}: 未找到')

    # 保存为parquet
    output_path = Path('data/stock_list.parquet')
    stock_list.write_parquet(output_path)
    print(f'股票列表已保存: {output_path}')

    # 验证保存的文件
    verify_df = pl.read_parquet(output_path)
    print(f'验证 - 数量: {len(verify_df)}, 类型: {verify_df["code"].dtype}')

if __name__ == '__main__':
    create_stock_list()
