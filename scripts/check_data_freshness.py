#!/usr/bin/env python3
"""
数据检查脚本：检查所有股票数据的新鲜度
"""
import sys
from pathlib import Path
import polars as pl
from datetime import date, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def check_data_freshness(kline_dir: Path, max_age_days: int = 30):
    """检查K线数据新鲜度"""
    print("=" * 80)
    print("数据新鲜度检查")
    print("=" * 80)
    
    cutoff_date = (date.today() - timedelta(days=max_age_days)).isoformat()
    print(f"\n截止日期 (30天前): {cutoff_date}")
    print(f"检查目录: {kline_dir}")
    
    parquet_files = list(kline_dir.glob('*.parquet'))
    total_files = len(parquet_files)
    
    print(f"\n发现 {total_files} 个Parquet文件")
    
    # 统计信息
    fresh_count = 0
    stale_count = 0
    missing_date_count = 0
    stale_stocks = []
    
    print("\n" + "-" * 80)
    print("检查进度")
    print("-" * 80)
    
    for i, parquet_file in enumerate(parquet_files, 1):
        code = parquet_file.stem
        
        try:
            df = pl.read_parquet(parquet_file)
            
            if "trade_date" not in df.columns:
                missing_date_count += 1
                print(f"[{i}/{total_files}] {code}: 缺少trade_date字段")
                continue
            
            latest_date = df["trade_date"].max()
            
            if latest_date >= cutoff_date:
                fresh_count += 1
            else:
                stale_count += 1
                stale_stocks.append({
                    'code': code,
                    'latest_date': latest_date,
                    'days_old': (date.today() - date.fromisoformat(latest_date)).days,
                    'rows': len(df)
                })
            
            if i % 1000 == 0:
                print(f"[{i}/{total_files}] 已检查: 新鲜={fresh_count}, 过旧={stale_count}")
                
        except Exception as e:
            print(f"[{i}/{total_files}] {code}: 错误 - {e}")
    
    # 输出统计
    print("\n" + "=" * 80)
    print("统计结果")
    print("=" * 80)
    
    print(f"\n总文件数: {total_files}")
    print(f"新鲜数据: {fresh_count} 只")
    print(f"过旧数据: {stale_count} 只")
    print(f"缺少日期: {missing_date_count} 只")
    
    if stale_stocks:
        print(f"\n过旧股票列表 (按日期排序):")
        print("-" * 80)
        
        sorted_stocks = sorted(stale_stocks, key=lambda x: x['latest_date'])
        
        for stock in sorted_stocks[:50]:  # 只显示前50个
            print(f"  {stock['code']}: 最新日期={stock['latest_date']}, "
                  f"年龄={stock['days_old']}天, 行数={stock['rows']}")
        
        if len(sorted_stocks) > 50:
            print(f"\n  ... 还有 {len(sorted_stocks) - 50} 只股票未显示")
    
    return stale_stocks

def main():
    kline_dir = PROJECT_ROOT / "data/kline"
    
    if not kline_dir.exists():
        print(f"错误: 目录不存在: {kline_dir}")
        sys.exit(1)
    
    stale_stocks = check_data_freshness(kline_dir)
    
    if stale_stocks:
        print(f"\n⚠️  检测到 {len(stale_stocks)} 只股票数据过旧")
        print(f"建议: 运行数据清理脚本或手动处理这些文件")
    else:
        print(f"\n✅ 所有股票数据新鲜度正常")
    
    sys.exit(0 if not stale_stocks else 1)

if __name__ == "__main__":
    main()
