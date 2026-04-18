#!/usr/bin/env python3
"""验证指数数据 - 检查Parquet和MySQL数据一致性"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from pymysql.cursors import DictCursor
from pathlib import Path
import polars as pl
from dotenv import load_dotenv

load_dotenv()


def check_parquet_data():
    """检查Parquet数据文件"""
    print('=' * 70)
    print('Parquet 数据文件检查')
    print('=' * 70)
    
    index_dir = Path('data/index')
    indices = [
        ('000001', '上证指数'),
        ('399001', '深证成指'),
        ('399006', '创业板指'),
        ('000300', '沪深300'),
        ('000016', '上证50'),
        ('000905', '中证500'),
    ]
    
    results = []
    for code, name in indices:
        parquet_file = index_dir / f'{code}.parquet'
        if parquet_file.exists():
            df = pl.read_parquet(parquet_file)
            latest_date = df['trade_date'].max()
            earliest_date = df['trade_date'].min()
            count = len(df)
            print(f"{name:12} ({code}): {count:>5} 条记录 | 最早: {earliest_date} | 最新: {latest_date}")
            results.append({
                'code': code, 'name': name, 'count': count,
                'earliest': earliest_date, 'latest': latest_date
            })
        else:
            print(f"{name:12} ({code}): 文件不存在")
    
    return results


def check_mysql_data():
    """检查MySQL数据"""
    print('\n' + '=' * 70)
    print('MySQL 数据检查')
    print('=' * 70)
    
    try:
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
        
        # 检查表是否存在
        cursor.execute('SHOW TABLES LIKE "index_daily"')
        if not cursor.fetchone():
            print('✗ 表 index_daily 不存在')
            conn.close()
            return []
        
        print('✓ 表 index_daily 存在')
        
        # 统计各指数记录数
        cursor.execute('''
            SELECT name, code, COUNT(*) as count, 
                   MIN(trade_date) as earliest,
                   MAX(trade_date) as latest
            FROM index_daily 
            GROUP BY code, name
            ORDER BY code
        ''')
        results = cursor.fetchall()
        
        print(f'\n共 {len(results)} 个指数：')
        for row in results:
            print(f"{row['name']:12} ({row['code']}): {row['count']:>5} 条记录 | 最早: {row['earliest']} | 最新: {row['latest']}")
        
        # 总记录数
        cursor.execute('SELECT COUNT(*) as total FROM index_daily')
        total = cursor.fetchone()['total']
        print(f'\n总记录数: {total} 条')
        
        # 检查最新日期的数据
        cursor.execute('SELECT MAX(trade_date) as max_date FROM index_daily')
        max_date = cursor.fetchone()['max_date']
        
        if max_date:
            cursor.execute('''
                SELECT name, code, close, change_pct, volume
                FROM index_daily
                WHERE trade_date = %s
                ORDER BY code
            ''', (max_date,))
            latest_data = cursor.fetchall()
            
            print(f"\n最新日期 ({max_date}) 数据：")
            for row in latest_data:
                change_str = f"{row['change_pct']:>6.2f}%" if row['change_pct'] else "N/A"
                print(f"{row['name']:12} ({row['code']}): 收盘={row['close']:>8.2f} | 涨跌={change_str} | 成交量={row['volume']}")
        
        conn.close()
        return results
        
    except Exception as e:
        print(f'✗ MySQL连接失败: {e}')
        return []


def compare_data(parquet_results, mysql_results):
    """比较Parquet和MySQL数据一致性"""
    print('\n' + '=' * 70)
    print('数据一致性检查')
    print('=' * 70)
    
    if not parquet_results or not mysql_results:
        print('数据不足，无法比较')
        return
    
    # 转换为字典方便比较
    parquet_dict = {r['code']: r for r in parquet_results}
    mysql_dict = {r['code']: r for r in mysql_results}
    
    all_codes = set(parquet_dict.keys()) | set(mysql_dict.keys())
    
    consistent = 0
    inconsistent = 0
    
    for code in sorted(all_codes):
        parquet_info = parquet_dict.get(code)
        mysql_info = mysql_dict.get(code)
        
        if not parquet_info:
            print(f"✗ {code}: Parquet数据缺失")
            inconsistent += 1
        elif not mysql_info:
            print(f"✗ {code}: MySQL数据缺失")
            inconsistent += 1
        else:
            p_count = parquet_info['count']
            m_count = mysql_info['count']
            p_latest = str(parquet_info['latest'])
            m_latest = str(mysql_info['latest'])
            
            if p_count == m_count and p_latest == m_latest:
                print(f"✓ {parquet_info['name']:12} ({code}): 一致 | 记录数={p_count} | 最新={p_latest}")
                consistent += 1
            else:
                print(f"✗ {parquet_info['name']:12} ({code}): 不一致")
                print(f"    Parquet: {p_count} 条, 最新={p_latest}")
                print(f"    MySQL:   {m_count} 条, 最新={m_latest}")
                inconsistent += 1
    
    print(f'\n一致性检查: {consistent}/{consistent+inconsistent} 通过')


def main():
    """主函数"""
    print('指数数据验证报告')
    
    # 检查Parquet数据
    parquet_results = check_parquet_data()
    
    # 检查MySQL数据
    mysql_results = check_mysql_data()
    
    # 比较数据一致性
    compare_data(parquet_results, mysql_results)
    
    print('\n' + '=' * 70)
    print('验证完成')
    print('=' * 70)


if __name__ == '__main__':
    main()
