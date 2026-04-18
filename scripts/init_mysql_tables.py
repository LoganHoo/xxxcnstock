#!/usr/bin/env python3
"""初始化MySQL数据库表结构"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """获取数据库连接"""
    return pymysql.connect(
        host=os.getenv('DB_HOST', '49.233.10.199'),
        port=int(os.getenv('DB_PORT', '3306')),
        user=os.getenv('DB_USER', 'nextai'),
        password=os.getenv('DB_PASSWORD', '100200'),
        database=os.getenv('DB_NAME', 'xcn_db'),
        charset='utf8mb4',
        cursorclass=DictCursor
    )

def init_strategy_reports_table():
    """初始化策略报告表"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS strategy_reports (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        strategy_type VARCHAR(50) NOT NULL COMMENT '策略类型',
        report_date DATE NOT NULL COMMENT '报告日期',
        subject VARCHAR(200) COMMENT '报告主题',
        text_content LONGTEXT COMMENT '文本内容',
        json_data JSON COMMENT 'JSON数据',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_strategy_date (strategy_type, report_date),
        INDEX idx_report_date (report_date),
        INDEX idx_strategy_type (strategy_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略报告表';
    """
    
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()
        print("✓ strategy_reports 表创建成功")
    except Exception as e:
        print(f"✗ strategy_reports 表创建失败: {e}")
    finally:
        conn.close()

def init_stock_selections_table():
    """初始化选股结果表"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS stock_selections (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        strategy_type VARCHAR(50) NOT NULL COMMENT '策略类型',
        report_date DATE NOT NULL COMMENT '报告日期',
        code VARCHAR(20) NOT NULL COMMENT '股票代码',
        name VARCHAR(100) COMMENT '股票名称',
        selection_type VARCHAR(50) COMMENT '选股类型: trend/short_term',
        score DECIMAL(10, 4) COMMENT '评分',
        factors JSON COMMENT '因子数据',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_strategy_date_code (strategy_type, report_date, code),
        INDEX idx_report_date (report_date),
        INDEX idx_code (code),
        INDEX idx_strategy_type (strategy_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='选股结果表';
    """
    
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()
        print("✓ stock_selections 表创建成功")
    except Exception as e:
        print(f"✗ stock_selections 表创建失败: {e}")
    finally:
        conn.close()

def init_daily_report_table():
    """初始化日报表"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS xcn_daily_report (
        id INT AUTO_INCREMENT PRIMARY KEY,
        report_type VARCHAR(20) NOT NULL COMMENT '报告类型',
        report_date DATE NOT NULL COMMENT '报告日期',
        subject VARCHAR(200) COMMENT '邮件主题',
        text_content LONGTEXT COMMENT '文本内容',
        json_data LONGTEXT COMMENT 'JSON数据',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_type_date (report_type, report_date),
        INDEX idx_report_date (report_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日报表';
    """
    
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()
        print("✓ xcn_daily_report 表创建成功")
    except Exception as e:
        print(f"✗ xcn_daily_report 表创建失败: {e}")
    finally:
        conn.close()

def main():
    """主函数"""
    print("=" * 60)
    print("MySQL数据库表初始化")
    print("=" * 60)
    
    init_strategy_reports_table()
    init_stock_selections_table()
    init_daily_report_table()
    
    print("=" * 60)
    print("初始化完成")
    print("=" * 60)

if __name__ == '__main__':
    main()
