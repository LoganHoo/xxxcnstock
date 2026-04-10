#!/usr/bin/env python3
"""
数据恢复脚本：从归档目录恢复股票数据
"""
import sys
from pathlib import Path
import shutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def restore_data(archive_dir: Path, kline_dir: Path, stock_codes: list = None):
    """恢复归档的股票数据"""
    print("=" * 80)
    print("数据恢复")
    print("=" * 80)
    
    print(f"\n归档目录: {archive_dir}")
    print(f"目标目录: {kline_dir}")
    
    if not archive_dir.exists():
        print(f"错误: 归档目录不存在: {archive_dir}")
        return False
    
    kline_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_files = list(archive_dir.glob('*.parquet'))
    total_files = len(parquet_files)
    
    print(f"\n归档目录中有 {total_files} 个Parquet文件")
    
    if stock_codes:
        parquet_files = [f for f in parquet_files if f.stem in stock_codes]
        print(f"匹配到 {len(parquet_files)} 个指定股票")
    
    restored_count = 0
    failed_count = 0
    
    print("\n" + "-" * 80)
    print("恢复进度")
    print("-" * 80)
    
    for i, parquet_file in enumerate(parquet_files, 1):
        code = parquet_file.stem
        
        try:
            dest_file = kline_dir / parquet_file.name
            
            if dest_file.exists():
                print(f"[{i}/{total_files}] {code}: 已存在，跳过")
                continue
            
            shutil.move(str(parquet_file), str(dest_file))
            restored_count += 1
            
            if i % 100 == 0:
                print(f"[{i}/{total_files}] 已恢复: {restored_count} 只")
                
        except Exception as e:
            failed_count += 1
            print(f"[{i}/{total_files}] {code}: 恢复失败 - {e}")
    
    # 输出统计
    print("\n" + "=" * 80)
    print("恢复结果")
    print("=" * 80)
    
    print(f"\n总文件数: {total_files}")
    print(f"已恢复: {restored_count} 只")
    print(f"失败: {failed_count} 只")
    print(f"跳过: {total_files - restored_count - failed_count} 只")
    
    return True

def main():
    archive_dir = PROJECT_ROOT / "data/kline_archived"
    kline_dir = PROJECT_ROOT / "data/kline"
    
    stock_codes = None
    if len(sys.argv) > 1:
        stock_codes = sys.argv[1].split(',')
        print(f"恢复指定股票: {stock_codes}")
    
    success = restore_data(archive_dir, kline_dir, stock_codes)
    
    if success:
        print(f"\n✅ 恢复完成")
    else:
        print(f"\n❌ 恢复失败")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
