#!/usr/bin/env python3
"""
K线数据增量更新测试

测试内容:
1. 首次全量采集
2. 模拟每日增量更新
3. 验证数据连续性
4. 测试重复采集（应跳过）
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from services.data_service.fetchers.kline_fetcher import (
    get_incremental_date_range,
    fetch_kline_via_service,
    save_with_verification
)
from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_kline_incr", log_file="system/test_kline_incr.log")

TEST_CODE = '000002'
TEST_DAYS = 30


async def test_full_collection():
    """测试全量采集"""
    print("\n" + "="*70)
    print("步骤1: 全量采集")
    print("="*70)
    
    kline_path = DATA_DIR / 'kline'
    kline_file = kline_path / f"{TEST_CODE}.parquet"
    
    # 删除已有数据（模拟首次采集）
    if kline_file.exists():
        kline_file.unlink()
        print(f"已删除旧数据文件: {kline_file}")
    
    # 计算日期范围
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=TEST_DAYS)).strftime('%Y-%m-%d')
    
    print(f"采集范围: {start_date} 至 {end_date}")
    
    # 采集数据
    df = await fetch_kline_via_service(TEST_CODE, start_date, end_date)
    
    if df is None or df.empty:
        print("❌ 全量采集失败")
        return False
    
    # 标准化列名
    if 'date' in df.columns and 'trade_date' not in df.columns:
        df = df.rename(columns={'date': 'trade_date'})
    
    # 保存数据
    output_file = kline_path / f"{TEST_CODE}.parquet"
    success = save_with_verification(df, output_file)
    
    if success:
        print(f"✅ 全量采集成功: {len(df)} 条数据")
        print(f"   日期范围: {df['trade_date'].min()} 至 {df['trade_date'].max()}")
        return True
    else:
        print("❌ 保存数据失败")
        return False


async def test_incremental_update():
    """测试增量更新"""
    print("\n" + "="*70)
    print("步骤2: 增量更新")
    print("="*70)
    
    kline_path = DATA_DIR / 'kline'
    
    # 获取增量日期范围
    start_date, end_date, is_incremental = get_incremental_date_range(
        TEST_CODE, TEST_DAYS, kline_path
    )
    
    print(f"增量模式: {is_incremental}")
    print(f"开始日期: {start_date}")
    print(f"结束日期: {end_date}")
    
    if start_date > end_date:
        print("✅ 数据已是最新，无需更新")
        return True
    
    # 采集新数据
    df_new = await fetch_kline_via_service(TEST_CODE, start_date, end_date)
    
    if df_new is None or df_new.empty:
        print("⚠️ 无新数据")
        return True
    
    # 标准化列名
    if 'date' in df_new.columns and 'trade_date' not in df_new.columns:
        df_new = df_new.rename(columns={'date': 'trade_date'})
    
    print(f"获取到新数据: {len(df_new)} 条")
    
    # 合并现有数据
    output_file = kline_path / f"{TEST_CODE}.parquet"
    if output_file.exists():
        df_existing = pl.read_parquet(output_file).to_pandas()
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['trade_date'], keep='last')
        df_combined = df_combined.sort_values('trade_date')
        print(f"合并后数据: {len(df_combined)} 条 (原有 {len(df_existing)} + 新增 {len(df_new)})")
    else:
        df_combined = df_new
    
    # 保存
    success = save_with_verification(df_combined, output_file)
    
    if success:
        print(f"✅ 增量更新成功")
        print(f"   最新日期: {df_combined['trade_date'].max()}")
        return True
    else:
        print("❌ 保存失败")
        return False


async def test_skip_existing():
    """测试跳过已存在数据"""
    print("\n" + "="*70)
    print("步骤3: 重复采集（应跳过）")
    print("="*70)
    
    kline_path = DATA_DIR / 'kline'
    
    # 再次获取日期范围
    start_date, end_date, is_incremental = get_incremental_date_range(
        TEST_CODE, TEST_DAYS, kline_path
    )
    
    print(f"增量模式: {is_incremental}")
    print(f"开始日期: {start_date}")
    print(f"结束日期: {end_date}")
    
    if start_date > end_date:
        print("✅ 正确跳过: 数据已是最新")
        return True
    else:
        print(f"⚠️ 未跳过: 仍有数据需要采集 ({start_date} 至 {end_date})")
        return False


async def verify_data_continuity():
    """验证数据连续性"""
    print("\n" + "="*70)
    print("步骤4: 验证数据连续性")
    print("="*70)
    
    kline_file = DATA_DIR / 'kline' / f"{TEST_CODE}.parquet"
    
    if not kline_file.exists():
        print("❌ 数据文件不存在")
        return False
    
    df = pl.read_parquet(kline_file).to_pandas()
    
    print(f"总数据行数: {len(df)}")
    print(f"日期范围: {df['trade_date'].min()} 至 {df['trade_date'].max()}")
    
    # 检查日期连续性
    df['date_parsed'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('date_parsed')
    
    # 计算交易日间隔
    df['prev_date'] = df['date_parsed'].shift(1)
    df['days_diff'] = (df['date_parsed'] - df['prev_date']).dt.days
    
    # 找出间隔大于5天的（可能是节假日）
    gaps = df[df['days_diff'] > 5]
    
    if len(gaps) > 0:
        print(f"\n⚠️ 发现 {len(gaps)} 个较大间隔（可能是节假日）:")
        for _, row in gaps.head(5).iterrows():
            print(f"   {row['prev_date'].strftime('%Y-%m-%d')} -> {row['trade_date']} ({row['days_diff']:.0f}天)")
    else:
        print("\n✅ 数据连续性良好")
    
    # 检查缺失值
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        print(f"\n⚠️ 发现缺失值:")
        for col, count in null_counts.items():
            if count > 0:
                print(f"   {col}: {count} 个缺失")
    else:
        print("\n✅ 无缺失值")
    
    return True


async def run_all_tests():
    """运行所有测试"""
    print("="*70)
    print("K线数据增量更新测试")
    print("="*70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {TEST_CODE}")
    print(f"测试天数: {TEST_DAYS}天")
    
    results = []
    
    # 步骤1: 全量采集
    results.append(("全量采集", await test_full_collection()))
    
    # 步骤2: 增量更新
    results.append(("增量更新", await test_incremental_update()))
    
    # 步骤3: 跳过已存在
    results.append(("跳过已存在", await test_skip_existing()))
    
    # 步骤4: 验证连续性
    results.append(("数据连续性", await verify_data_continuity()))
    
    # 汇总
    print("\n" + "="*70)
    print("测试汇总")
    print("="*70)
    
    for name, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {name}")
    
    passed = sum(1 for _, s in results if s)
    total = len(results)
    
    print(f"\n总计: {passed}/{total} 通过 ({passed/total*100:.1f}%)")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(run_all_tests())
