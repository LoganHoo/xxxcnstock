#!/usr/bin/env python3
"""
数据归档脚本：将过旧的股票数据归档到独立目录
"""
import sys
from pathlib import Path
import shutil
import polars as pl
from datetime import date, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def archive_stale_data(kline_dir: Path, archive_dir: Path, max_age_days: int = 30):
    """归档过旧的股票数据"""
    print("=" * 80)
    print("数据归档")
    print("=" * 80)
    
    cutoff_date = (date.today() - timedelta(days=max_age_days)).isoformat()
    print(f"\n截止日期 (30天前): {cutoff_date}")
    print(f"源目录: {kline_dir}")
    print(f"归档目录: {archive_dir}")
    
    if not kline_dir.exists():
        print(f"错误: 源目录不存在: {kline_dir}")
        return False
    
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_files = list(kline_dir.glob('*.parquet'))
    total_files = len(parquet_files)
    
    print(f"\n发现 {total_files} 个Parquet文件")
    
    # 统计信息
    archived_count = 0
    failed_count = 0
    archived_stocks = []
    
    print("\n" + "-" * 80)
    print("归档进度")
    print("-" * 80)
    
    for i, parquet_file in enumerate(parquet_files, 1):
        code = parquet_file.stem
        
        try:
            df = pl.read_parquet(parquet_file)
            
            if "trade_date" not in df.columns:
                print(f"[{i}/{total_files}] {code}: 缺少trade_date字段，跳过")
                continue
            
            latest_date = df["trade_date"].max()
            
            if latest_date < cutoff_date:
                dest_file = archive_dir / parquet_file.name
                shutil.move(str(parquet_file), str(dest_file))
                
                archived_count += 1
                archived_stocks.append({
                    'code': code,
                    'latest_date': latest_date,
                    'days_old': (date.today() - date.fromisoformat(latest_date)).days
                })
                
                if i % 100 == 0:
                    print(f"[{i}/{total_files}] 已归档: {archived_count} 只")
            else:
                pass
                
        except Exception as e:
            failed_count += 1
            print(f"[{i}/{total_files}] {code}: 归档失败 - {e}")
    
    # 输出统计
    print("\n" + "=" * 80)
    print("归档结果")
    print("=" * 80)
    
    print(f"\n总文件数: {total_files}")
    print(f"已归档: {archived_count} 只")
    print(f"失败: {failed_count} 只")
    
    if archived_stocks:
        print(f"\n归档股票列表:")
        print("-" * 80)
        
        sorted_stocks = sorted(archived_stocks, key=lambda x: x['latest_date'])
        
        for stock in sorted_stocks[:20]:  # 只显示前20个
            print(f"  {stock['code']}: 最新日期={stock['latest_date']}, "
                  f"年龄={stock['days_old']}天")
        
        if len(sorted_stocks) > 20:
            print(f"\n  ... 还有 {len(sorted_stocks) - 20} 只股票未显示")
        
        # 保存归档列表
        archive_list_path = archive_dir / "archived_stocks.txt"
        with open(archive_list_path, 'w', encoding='utf-8') as f:
            f.write("# 归档股票列表\n")
            f.write(f"# 生成时间: {date.today().isoformat()}\n")
            f.write(f"# 截止日期: {cutoff_date}\n")
            f.write(f"# 总数: {len(sorted_stocks)}\n")
            f.write("-" * 80 + "\n")
            for stock in sorted_stocks:
                f.write(f"{stock['code']}\t{stock['latest_date']}\t{stock['days_old']}\n")
        
        print(f"\n归档列表已保存: {archive_list_path}")
    
    return True

def main():
    kline_dir = PROJECT_ROOT / "data/kline"
    archive_dir = PROJECT_ROOT / "data/kline_archived"
    
    success = archive_stale_data(kline_dir, archive_dir)
    
    if success:
        print(f"\n✅ 归档完成")
    else:
        print(f"\n❌ 归档失败")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
