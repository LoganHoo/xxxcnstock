#!/usr/bin/env python3
"""
资金行为学策略数据真实性验证脚本
逐项检查每个阶段的数据
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
from datetime import datetime, date
import yaml
import json

print("=" * 80)
print("资金行为学策略 - 数据真实性验证报告")
print("=" * 80)
print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# =============================================================================
# 阶段1: 检查原始K线数据 (data/kline/)
# =============================================================================
print("【阶段1】原始K线数据检查")
print("-" * 80)

kline_dir = Path('data/kline')
if kline_dir.exists():
    parquet_files = list(kline_dir.glob('*.parquet'))
    print(f"✓ K线数据目录存在: {kline_dir}")
    print(f"  股票数据文件数量: {len(parquet_files)}")
    
    # 抽样检查几个文件
    sample_files = parquet_files[:3]
    for f in sample_files:
        try:
            df = pl.read_parquet(f)
            latest = df['trade_date'].max()
            print(f"  样本 {f.name}: {len(df)} 条记录, 最新日期: {latest}")
        except Exception as e:
            print(f"  ✗ {f.name}: 读取失败 - {e}")
else:
    print(f"✗ K线数据目录不存在: {kline_dir}")

print()

# =============================================================================
# 阶段2: 检查指数数据 (data/index/)
# =============================================================================
print("【阶段2】大盘指数数据检查")
print("-" * 80)

index_dir = Path('data/index')
indices = [
    ('000001', '上证指数'),
    ('399001', '深证成指'),
    ('399006', '创业板指'),
    ('000300', '沪深300'),
    ('000016', '上证50'),
    ('000905', '中证500'),
]

for code, name in indices:
    parquet_file = index_dir / f"{code}.parquet"
    if parquet_file.exists():
        df = pl.read_parquet(parquet_file)
        latest = df['trade_date'].max()
        close = df.filter(pl.col('trade_date') == latest)['close'].to_list()[0] if len(df) > 0 else None
        print(f"✓ {name:12} ({code}): {len(df):>5} 条 | 最新: {latest} | 收盘: {close}")
    else:
        print(f"✗ {name:12} ({code}): 文件不存在")

print()

# =============================================================================
# 阶段3: 检查股票列表数据
# =============================================================================
print("【阶段3】股票列表数据检查")
print("-" * 80)

stock_list_file = Path('data/stock_list.parquet')
if stock_list_file.exists():
    df = pl.read_parquet(stock_list_file)
    print(f"✓ 股票列表存在: {len(df)} 只股票")
    print(f"  列名: {df.columns}")
    if 'code' in df.columns:
        print(f"  样本代码: {df['code'].to_list()[:5]}")
else:
    print(f"✗ 股票列表不存在")

print()

# =============================================================================
# 阶段4: 检查配置文件
# =============================================================================
print("【阶段4】策略配置文件检查")
print("-" * 80)

config_file = Path('config/strategies/fund_behavior_config.yaml')
if config_file.exists():
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    print(f"✓ 策略配置存在")
    
    # 检查关键配置项
    factors = config.get('factors', {})
    print(f"  配置因子数量: {len(factors)}")
    print(f"  因子列表: {list(factors.keys())[:10]}...")
    
    backtest = config.get('backtest', {})
    print(f"  回测配置: 初始资金={backtest.get('initial_capital', 'N/A')}")
else:
    print(f"✗ 策略配置不存在")

# 检查过滤器配置
filter_config_file = Path('config/filters/fund_behavior_filters.yaml')
if filter_config_file.exists():
    with open(filter_config_file, 'r', encoding='utf-8') as f:
        filter_config = yaml.safe_load(f)
    filters = filter_config.get('filters', {})
    enabled_filters = [k for k, v in filters.items() if v.get('enabled', False)]
    print(f"✓ 过滤器配置存在: {len(enabled_filters)}/{len(filters)} 个已启用")
    print(f"  启用的过滤器: {enabled_filters[:5]}...")
else:
    print(f"✗ 过滤器配置不存在")

print()

# =============================================================================
# 阶段5: 检查历史报告数据
# =============================================================================
print("【阶段5】历史报告数据检查")
print("-" * 80)

reports_dir = Path('data/reports')
if reports_dir.exists():
    txt_files = list(reports_dir.glob('fund_behavior_*.txt'))
    html_files = list(reports_dir.glob('html/fund_behavior_*.html'))
    print(f"✓ 报告目录存在")
    print(f"  TXT报告数量: {len(txt_files)}")
    print(f"  HTML报告数量: {len(html_files)}")
    
    # 显示最新报告
    if txt_files:
        latest_report = sorted(txt_files)[-1]
        print(f"  最新报告: {latest_report.name}")
else:
    print(f"✗ 报告目录不存在")

print()

# =============================================================================
# 阶段6: 检查MySQL数据
# =============================================================================
print("【阶段6】MySQL数据库数据检查")
print("-" * 80)

try:
    import pymysql
    from pymysql.cursors import DictCursor
    from dotenv import load_dotenv
    load_dotenv()
    
    conn = pymysql.connect(
        host=os.getenv('DB_HOST', '49.233.10.199'),
        port=int(os.getenv('DB_PORT', '3306')),
        user=os.getenv('DB_USER', 'nextai'),
        password=os.getenv('DB_PASSWORD', '100200'),
        database=os.getenv('DB_NAME', 'xcn_db'),
        charset='utf8mb4',
        cursorclass=DictCursor
    )
    
    cursor = conn.cursor()
    
    # 检查指数数据表
    cursor.execute('SHOW TABLES LIKE "index_daily"')
    if cursor.fetchone():
        cursor.execute('SELECT COUNT(*) as total FROM index_daily')
        total = cursor.fetchone()['total']
        print(f"✓ index_daily 表存在: {total} 条记录")
        
        cursor.execute('SELECT MAX(trade_date) as latest FROM index_daily')
        latest = cursor.fetchone()['latest']
        print(f"  最新数据日期: {latest}")
    else:
        print(f"✗ index_daily 表不存在")
    
    # 检查策略报告表
    cursor.execute('SHOW TABLES LIKE "strategy_reports"')
    if cursor.fetchone():
        cursor.execute('SELECT COUNT(*) as total FROM strategy_reports WHERE strategy_type="fund_behavior"')
        total = cursor.fetchone()['total']
        print(f"✓ strategy_reports 表存在: {total} 条资金行为学报告")
    else:
        print(f"✗ strategy_reports 表不存在")
    
    # 检查选股结果表
    cursor.execute('SHOW TABLES LIKE "stock_selections"')
    if cursor.fetchone():
        cursor.execute('SELECT COUNT(*) as total FROM stock_selections')
        total = cursor.fetchone()['total']
        print(f"✓ stock_selections 表存在: {total} 条选股记录")
    else:
        print(f"✗ stock_selections 表不存在")
    
    conn.close()
except Exception as e:
    print(f"✗ MySQL连接失败: {e}")

print()

# =============================================================================
# 阶段7: 检查缓存数据
# =============================================================================
print("【阶段7】缓存数据检查")
print("-" * 80)

cache_dir = Path('data/cache')
if cache_dir.exists():
    cache_files = list(cache_dir.glob('*.parquet'))
    print(f"✓ 缓存目录存在: {len(cache_files)} 个缓存文件")
    for f in sorted(cache_files)[-5:]:
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"  {f.name}: {size_mb:.2f} MB")
else:
    print(f"✗ 缓存目录不存在")

print()

# =============================================================================
# 阶段8: 数据新鲜度检查
# =============================================================================
print("【阶段8】数据新鲜度检查")
print("-" * 80)

today = date.today()
print(f"当前日期: {today}")

# 检查K线数据最新日期
if kline_dir.exists() and parquet_files:
    latest_dates = []
    for f in parquet_files[:100]:  # 抽样100个文件
        try:
            df = pl.read_parquet(f)
            latest_dates.append(df['trade_date'].max())
        except:
            pass
    if latest_dates:
        overall_latest = max(latest_dates)
        days_diff = (today - overall_latest).days
        status = "✓" if days_diff <= 1 else "△" if days_diff <= 3 else "✗"
        print(f"{status} K线数据最新日期: {overall_latest} (距今 {days_diff} 天)")

# 检查指数数据最新日期
for code, name in [('000001', '上证指数')]:
    parquet_file = index_dir / f"{code}.parquet"
    if parquet_file.exists():
        df = pl.read_parquet(parquet_file)
        latest = df['trade_date'].max()
        days_diff = (today - latest).days
        status = "✓" if days_diff <= 1 else "△" if days_diff <= 3 else "✗"
        print(f"{status} 指数数据最新日期: {latest} (距今 {days_diff} 天)")

print()

# =============================================================================
# 总结
# =============================================================================
print("=" * 80)
print("验证总结")
print("=" * 80)
print("""
数据真实性验证项:
1. 原始K线数据    - 检查股票日线数据文件完整性
2. 大盘指数数据   - 检查指数数据文件完整性
3. 股票列表数据   - 检查股票基础信息
4. 策略配置文件   - 检查策略参数配置
5. 历史报告数据   - 检查生成的报告文件
6. MySQL数据库    - 检查同步的数据库记录
7. 缓存数据       - 检查中间计算缓存
8. 数据新鲜度     - 检查数据更新时效性

建议:
- 如发现有✗标记的项目，需要检查对应的数据源或配置
- 数据文件应每日更新，确保策略基于最新数据运行
- 定期检查MySQL同步状态，确保数据一致性
""")
print("=" * 80)
