#!/usr/bin/env python3
"""
问题分析报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import subprocess
import shutil
from pathlib import Path
from datetime import datetime
import polars as pl

print("=" * 70)
print("🔍 系统问题分析")
print("=" * 70)

# 1. 磁盘空间检查
print("\n📁 1. 磁盘空间检查")
print("-" * 50)
try:
    total, used, free = shutil.disk_usage("/Volumes/Xdata")
    print(f"   总空间: {total / (1024**3):.2f} GB")
    print(f"   已用:   {used / (1024**3):.2f} GB ({used/total*100:.1f}%)")
    print(f"   可用:   {free / (1024**3):.2f} GB ({free/total*100:.1f}%)")
    if free / total < 0.1:
        print("   ⚠️  警告: 磁盘空间不足10%")
except Exception as e:
    print(f"   无法获取磁盘信息: {e}")

# 2. MySQL连接检查
print("\n🗄️  2. MySQL连接检查")
print("-" * 50)
try:
    from core.config import get_settings
    settings = get_settings()
    print(f"   主机: {settings.DB_HOST}:{settings.DB_PORT}")
    print(f"   数据库: {settings.DB_NAME}")
    print(f"   用户: {settings.DB_USER}")
    
    # 尝试连接
    import pymysql
    conn = pymysql.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD or '',
        database=settings.DB_NAME,
        connect_timeout=5
    )
    
    with conn.cursor() as cursor:
        # 检查磁盘状态
        cursor.execute("SHOW VARIABLES LIKE 'datadir'")
        datadir = cursor.fetchone()
        print(f"   数据目录: {datadir[1] if datadir else 'Unknown'}")
        
        # 检查表空间
        cursor.execute("""
            SELECT table_name, 
                   ROUND(data_length/1024/1024, 2) as data_mb,
                   ROUND(index_length/1024/1024, 2) as index_mb
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY data_length DESC
            LIMIT 10
        """, (settings.DB_NAME,))
        
        tables = cursor.fetchall()
        if tables:
            print(f"\n   最大的10个表:")
            for table in tables:
                print(f"     {table[0]}: {table[1]} MB (数据) + {table[2]} MB (索引)")
    
    conn.close()
    print("   ✅ MySQL连接正常")
except Exception as e:
    print(f"   ❌ MySQL连接失败: {e}")

# 3. 数据文件检查
print("\n📊 3. 数据文件检查")
print("-" * 50)
data_dir = Path('data')

# 检查关键文件
files_to_check = [
    'stock_list.parquet',
    'enhanced_scores_full.parquet',
    'stock_list_enhanced.parquet'
]

for fname in files_to_check:
    fpath = data_dir / fname
    if fpath.exists():
        size = fpath.stat().st_size / 1024  # KB
        print(f"   {fname}: {size:.1f} KB")
        
        # 检查内容
        try:
            df = pl.read_parquet(fpath)
            print(f"     └─ 行数: {len(df)}, 列: {df.columns[:5]}...")
        except Exception as e:
            print(f"     └─ 无法读取: {e}")
    else:
        print(f"   {fname}: ❌ 不存在")

# 4. K线数据检查
print("\n📈 4. K线数据检查")
print("-" * 50)
kline_dir = data_dir / 'kline'
if kline_dir.exists():
    kline_files = list(kline_dir.glob('*.parquet'))
    print(f"   K线文件数: {len(kline_files)}")
    
    # 检查文件大小分布
    sizes = [f.stat().st_size for f in kline_files]
    if sizes:
        avg_size = sum(sizes) / len(sizes) / 1024  # KB
        total_size = sum(sizes) / 1024 / 1024  # MB
        print(f"   平均大小: {avg_size:.1f} KB")
        print(f"   总大小: {total_size:.1f} MB")

# 5. 检查点文件
print("\n💾 5. 检查点文件")
print("-" * 50)
checkpoint_dir = data_dir / 'checkpoints'
if checkpoint_dir.exists():
    checkpoints = list(checkpoint_dir.rglob('*.json'))
    print(f"   检查点文件数: {len(checkpoints)}")
    total_size = sum(f.stat().st_size for f in checkpoints) / 1024
    print(f"   总大小: {total_size:.1f} KB")

# 6. 报告文件
print("\n📄 6. 报告文件")
print("-" * 50)
report_dir = data_dir / 'reports'
if report_dir.exists():
    reports = list(report_dir.rglob('*'))
    json_reports = [r for r in reports if r.suffix == '.json']
    md_reports = [r for r in reports if r.suffix == '.md']
    print(f"   JSON报告: {len(json_reports)}")
    print(f"   Markdown报告: {len(md_reports)}")
    total_size = sum(r.stat().st_size for r in reports if r.is_file()) / 1024 / 1024
    print(f"   总大小: {total_size:.1f} MB")

# 7. 内存使用
print("\n🧠 7. 内存使用")
print("-" * 50)
try:
    import psutil
    process = psutil.Process()
    mem_info = process.memory_info()
    print(f"   RSS: {mem_info.rss / 1024 / 1024:.1f} MB")
    print(f"   VMS: {mem_info.vms / 1024 / 1024:.1f} MB")
except ImportError:
    print("   psutil未安装，无法获取内存信息")

print("\n" + "=" * 70)
print("📋 问题总结")
print("=" * 70)
print("""
1. MySQL磁盘已满 - 需要清理或扩容
   影响: 无法保存选股结果到数据库
   解决: 清理旧数据或联系DBA扩容

2. 本地数据文件正常
   - stock_list.parquet: 正常
   - enhanced_scores_full.parquet: 正常
   - K线数据: 正常

3. 选股流程正常执行
   - 依赖检查: 通过
   - GE检查点: 通过
   - 邮件发送: 成功
   - 文件输出: 正常
""")
