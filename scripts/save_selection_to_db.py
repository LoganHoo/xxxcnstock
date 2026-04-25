#!/usr/bin/env python3
"""
选股结果存入MySQL数据库

表结构:
- stock_selections: 选股记录主表
- stock_selection_details: 选股详情表
"""
import sys
from pathlib import Path
from datetime import datetime
import json

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pymysql
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv(Path(__file__).resolve().parent.parent / '.env')


def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(
        host=os.getenv('DB_HOST', '49.233.10.199'),
        port=int(os.getenv('DB_PORT', 3306)),
        user=os.getenv('DB_USER', 'nextai'),
        password=os.getenv('DB_PASSWORD', '100200'),
        database=os.getenv('DB_NAME', 'xcn_db'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def create_tables():
    """创建选股结果表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 选股记录主表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_selections (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    selection_date DATE NOT NULL COMMENT '选股日期',
                    strategy_type VARCHAR(50) NOT NULL COMMENT '策略类型',
                    total_stocks INT NOT NULL COMMENT '总股票数',
                    selected_stocks INT NOT NULL COMMENT '选中股票数',
                    filtered_out INT NOT NULL COMMENT '过滤掉数量',
                    duration_seconds FLOAT COMMENT '耗时(秒)',
                    status VARCHAR(20) COMMENT '状态',
                    filters_applied JSON COMMENT '应用的过滤器',
                    errors JSON COMMENT '错误信息',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_selection_date (selection_date),
                    INDEX idx_strategy_type (strategy_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='选股记录主表'
            """)
            
            # 选股详情表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_selection_details (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    selection_id INT NOT NULL COMMENT '选股记录ID',
                    rank_num INT NOT NULL COMMENT '排名',
                    code VARCHAR(10) NOT NULL COMMENT '股票代码',
                    name VARCHAR(50) COMMENT '股票名称',
                    total_score DECIMAL(5,2) COMMENT '综合评分',
                    financial_score DECIMAL(5,2) COMMENT '财务评分',
                    market_score DECIMAL(5,2) COMMENT '市场评分',
                    announcement_score DECIMAL(5,2) COMMENT '公告评分',
                    technical_score DECIMAL(5,2) COMMENT '技术评分',
                    roe DECIMAL(8,2) COMMENT 'ROE',
                    gross_margin DECIMAL(8,2) COMMENT '毛利率',
                    revenue_growth DECIMAL(8,2) COMMENT '营收增长率',
                    debt_ratio DECIMAL(8,2) COMMENT '资产负债率',
                    main_force_flow DECIMAL(15,2) COMMENT '主力资金流',
                    dragon_tiger_count INT COMMENT '龙虎榜次数',
                    price_change_5d DECIMAL(8,2) COMMENT '5日涨幅',
                    price_change_20d DECIMAL(8,2) COMMENT '20日涨幅',
                    volume_ratio DECIMAL(8,2) COMMENT '量比',
                    filter_reason VARCHAR(100) COMMENT '过滤原因',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_selection_id (selection_id),
                    INDEX idx_code (code),
                    INDEX idx_rank (rank_num)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='选股详情表'
            """)
            
            conn.commit()
            print("✅ 数据库表创建成功")
    finally:
        conn.close()


def save_selection_to_db(selection_date: str, strategy_type: str = 'comprehensive'):
    """将选股结果存入数据库"""
    # 读取选股结果
    result_file = Path(f'data/workflow_results/real_selection_{strategy_type}_{selection_date}.json')
    if not result_file.exists():
        print(f'❌ 未找到选股记录: {result_file}')
        return False
    
    with open(result_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 1. 插入主表
            cursor.execute("""
                INSERT INTO stock_selections 
                (selection_date, strategy_type, total_stocks, selected_stocks, filtered_out, 
                 duration_seconds, status, filters_applied, errors)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                selection_date,
                data['strategy_type'],
                data['total_stocks'],
                data['selected_stocks'],
                data['filtered_out'],
                data['duration_seconds'],
                data['status'],
                json.dumps(data.get('filters_applied', [])),
                json.dumps(data.get('errors', []))
            ))
            
            selection_id = cursor.lastrowid
            
            # 2. 插入详情表
            top_stocks = data.get('top_stocks', [])
            for i, stock in enumerate(top_stocks, 1):
                cursor.execute("""
                    INSERT INTO stock_selection_details
                    (selection_id, rank_num, code, name, total_score, financial_score, 
                     market_score, announcement_score, technical_score, roe, gross_margin,
                     revenue_growth, debt_ratio, main_force_flow, dragon_tiger_count,
                     price_change_5d, price_change_20d, volume_ratio, filter_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    selection_id,
                    i,
                    stock.get('code'),
                    stock.get('name', ''),
                    stock.get('total_score'),
                    stock.get('financial_score'),
                    stock.get('market_score'),
                    stock.get('announcement_score'),
                    stock.get('technical_score'),
                    stock.get('roe'),
                    stock.get('gross_margin'),
                    stock.get('revenue_growth'),
                    stock.get('debt_ratio'),
                    stock.get('main_force_flow'),
                    stock.get('dragon_tiger_count'),
                    stock.get('price_change_5d'),
                    stock.get('price_change_20d'),
                    stock.get('volume_ratio'),
                    stock.get('filter_reason')
                ))
            
            conn.commit()
            print(f"✅ 选股结果已存入数据库")
            print(f"   选股记录ID: {selection_id}")
            print(f"   选股日期: {selection_date}")
            print(f"   策略类型: {data['strategy_type']}")
            print(f"   总股票数: {data['total_stocks']}")
            print(f"   选中股票: {data['selected_stocks']}")
            print(f"   Top股票: {len(top_stocks)} 只")
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"❌ 存入数据库失败: {e}")
        return False
    finally:
        conn.close()


def query_selection_history(limit: int = 10):
    """查询选股历史"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, selection_date, strategy_type, total_stocks, 
                       selected_stocks, status, created_at
                FROM stock_selections
                ORDER BY selection_date DESC, created_at DESC
                LIMIT %s
            """, (limit,))
            
            results = cursor.fetchall()
            
            if not results:
                print("暂无选股记录")
                return
            
            print("\n📊 选股历史")
            print("=" * 100)
            print(f"{'ID':<6}{'日期':<12}{'策略':<20}{'总股票':<10}{'选中':<10}{'状态':<10}{'创建时间':<20}")
            print("-" * 100)
            
            for row in results:
                print(f"{row['id']:<6}{row['selection_date']:<12}{row['strategy_type']:<20}"
                      f"{row['total_stocks']:<10}{row['selected_stocks']:<10}"
                      f"{row['status']:<10}{row['created_at']}")
            
            return results
    finally:
        conn.close()


def query_selection_details(selection_id: int):
    """查询选股详情"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT rank_num, code, name, total_score, financial_score,
                       market_score, technical_score
                FROM stock_selection_details
                WHERE selection_id = %s
                ORDER BY rank_num
                LIMIT 20
            """, (selection_id,))
            
            results = cursor.fetchall()
            
            if not results:
                print(f"未找到选股记录 ID={selection_id}")
                return
            
            print(f"\n📈 选股详情 (ID={selection_id})")
            print("=" * 100)
            print(f"{'排名':<6}{'代码':<10}{'名称':<12}{'综合':<8}{'财务':<8}{'市场':<8}{'技术':<8}")
            print("-" * 100)
            
            for row in results:
                name = row['name'][:10] if row['name'] else ''
                print(f"{row['rank_num']:<6}{row['code']:<10}{name:<12}"
                      f"{row['total_score']:<8.1f}{row['financial_score']:<8.1f}"
                      f"{row['market_score']:<8.1f}{row['technical_score']:<8.1f}")
            
            return results
    finally:
        conn.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='选股结果数据库管理')
    parser.add_argument('--init', action='store_true', help='初始化数据库表')
    parser.add_argument('--save', help='保存选股结果到数据库 (指定日期 YYYY-MM-DD)')
    parser.add_argument('--query', action='store_true', help='查询选股历史')
    parser.add_argument('--details', type=int, help='查询选股详情 (指定ID)')
    parser.add_argument('--strategy', default='comprehensive', help='策略类型')
    
    args = parser.parse_args()
    
    if args.init:
        create_tables()
    elif args.save:
        save_selection_to_db(args.save, args.strategy)
    elif args.query:
        query_selection_history()
    elif args.details:
        query_selection_details(args.details)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
