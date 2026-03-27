#!/usr/bin/env python3
"""验证 MySQL 数据库中的数据"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text

db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '3306')
db_user = os.getenv('DB_USER', 'root')
db_password = os.getenv('DB_PASSWORD', '')
db_name = os.getenv('DB_NAME', 'quantdb')

conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'
engine = create_engine(conn_str)

print('=' * 70)
print('📊 MySQL 数据库验证')
print('=' * 70)

with engine.connect() as conn:
    tables = conn.execute(text("SHOW TABLES LIKE 'xcn_%'")).fetchall()
    print(f'\n✅ 已创建的表: {[t[0] for t in tables]}')
    
    print('\n' + '=' * 70)
    print('📦 批次记录 (xcn_daily_batch)')
    print('=' * 70)
    
    batches = conn.execute(text("""
        SELECT batch_id, batch_date, status, total_picks, created_at, updated_at
        FROM xcn_daily_batch 
        ORDER BY batch_date DESC 
        LIMIT 10
    """)).fetchall()
    
    if batches:
        print(f'{"批次ID":<12} {"日期":<12} {"状态":<10} {"推荐数":>8} {"创建时间":<20}')
        print('-' * 70)
        for row in batches:
            created = str(row[4])[:19] if row[4] else '-'
            print(f'{row[0]:<12} {row[1]} {row[2]:<10} {row[3]:>8} {created}')
    else:
        print('❌ 无批次记录')
    
    print('\n' + '=' * 70)
    print('📈 每日统计 (xcn_daily_stats)')
    print('=' * 70)
    
    stats = conn.execute(text("""
        SELECT batch_id, stat_date, total_stocks, s_grade_count, a_grade_count, 
               bullish_count, rising_count
        FROM xcn_daily_stats 
        ORDER BY stat_date DESC 
        LIMIT 10
    """)).fetchall()
    
    if stats:
        print(f'{"批次ID":<12} {"日期":<12} {"总数":>8} {"S级":>6} {"A级":>6} {"多头":>6} {"上涨":>6}')
        print('-' * 65)
        for row in stats:
            print(f'{row[0]:<12} {row[1]} {row[2]:>8} {row[3]:>6} {row[4]:>6} {row[5]:>6} {row[6]:>6}')
    else:
        print('❌ 无统计数据')
    
    print('\n' + '=' * 70)
    print('📋 每日推荐记录 (xcn_daily_picks)')
    print('=' * 70)
    
    date_stats = conn.execute(text("""
        SELECT batch_id, pick_date, filter_type, COUNT(*) as cnt
        FROM xcn_daily_picks 
        GROUP BY batch_id, pick_date, filter_type
        ORDER BY pick_date DESC, filter_type
    """)).fetchall()
    
    if date_stats:
        print(f'{"批次ID":<12} {"日期":<12} {"筛选类型":<15} {"数量":>8}')
        print('-' * 50)
        for row in date_stats:
            print(f'{row[0]:<12} {row[1]} {row[2]:<15} {row[3]:>8}')
    else:
        print('❌ 无推荐记录')
    
    print('\n' + '=' * 70)
    print('🔍 最近推荐详情 (前10条)')
    print('=' * 70)
    
    picks = conn.execute(text("""
        SELECT batch_id, pick_date, code, name, price, change_pct, grade, 
               enhanced_score, filter_type
        FROM xcn_daily_picks 
        ORDER BY pick_date DESC, enhanced_score DESC
        LIMIT 10
    """)).fetchall()
    
    if picks:
        print(f'{"批次ID":<12} {"代码":<8} {"名称":<8} {"价格":>8} {"涨幅%":>7} {"级":>2} {"评分":>6}')
        print('-' * 60)
        for row in picks:
            name = (row[3] or '')[:6]
            print(f'{row[0]:<12} {row[2]:<8} {name:<8} {row[4]:>8.2f} {row[5]:>6.2f}% {row[6]:>2} {row[7]:>6.1f}')
    else:
        print('❌ 无推荐详情')

print('\n' + '=' * 70)
print('✅ 验证完成')
print('=' * 70)
