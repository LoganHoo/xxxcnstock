#!/usr/bin/env python3
"""
数据新鲜度监控脚本：定期检查并报告数据问题
"""
import sys
from pathlib import Path
import polars as pl
from datetime import date, timedelta
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def check_and_alert(kline_dir: Path, max_age_days: int = 30, alert_threshold: int = 100):
    """检查数据新鲜度并发出警报"""
    print("=" * 80)
    print("数据新鲜度监控")
    print("=" * 80)
    
    cutoff_date = (date.today() - timedelta(days=max_age_days)).isoformat()
    print(f"\n截止日期 (30天前): {cutoff_date}")
    print(f"检查目录: {kline_dir}")
    
    parquet_files = list(kline_dir.glob('*.parquet'))
    total_files = len(parquet_files)
    
    print(f"\n发现 {total_files} 个Parquet文件")
    
    # 统计信息
    stale_count = 0
    stale_stocks = []
    
    print("\n" + "-" * 80)
    print("检查进度")
    print("-" * 80)
    
    for i, parquet_file in enumerate(parquet_files, 1):
        code = parquet_file.stem
        
        try:
            df = pl.read_parquet(parquet_file)
            
            if "trade_date" not in df.columns:
                continue
            
            latest_date = df["trade_date"].max()
            
            if latest_date < cutoff_date:
                stale_count += 1
                stale_stocks.append({
                    'code': code,
                    'latest_date': latest_date,
                    'days_old': (date.today() - date.fromisoformat(latest_date)).days
                })
                
        except Exception as e:
            pass
    
    # 输出统计
    print("\n" + "=" * 80)
    print("监控结果")
    print("=" * 80)
    
    print(f"\n总文件数: {total_files}")
    print(f"过旧数据: {stale_count} 只")
    
    if stale_count > alert_threshold:
        print(f"\n⚠️  警报: 过旧股票数量 ({stale_count}) 超过阈值 ({alert_threshold})")
        
        sorted_stocks = sorted(stale_stocks, key=lambda x: x['days_old'], reverse=True)
        
        print(f"\n最新过旧的 {min(10, len(sorted_stocks))} 只股票:")
        for stock in sorted_stocks[:10]:
            print(f"  {stock['code']}: 最新日期={stock['latest_date']}, "
                  f"年龄={stock['days_old']}天")
        
        return False
    else:
        print(f"\n✅ 数据新鲜度正常")
        return True

def main():
    kline_dir = PROJECT_ROOT / "data/kline"
    
    if not kline_dir.exists():
        print(f"错误: 目录不存在: {kline_dir}")
        sys.exit(1)
    
    success = check_and_alert(kline_dir)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
