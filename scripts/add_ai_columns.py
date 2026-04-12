#!/usr/bin/env python
import pymysql
import os
from dotenv import load_dotenv
load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST', '49.233.10.199'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'nextai'),
    'password': os.getenv('DB_PASSWORD', '100200'),
    'database': os.getenv('DB_NAME', 'xcn_db'),
    'charset': 'utf8mb4',
}

conn = pymysql.connect(**db_config)
try:
    with conn.cursor() as cursor:
        fields = [
            ('ai_summary', 'TEXT'),
            ('ai_bullish', 'TEXT'),
            ('ai_hot_sectors', 'TEXT'),
            ('ai_leading_stocks', 'TEXT'),
            ('ai_macro_guidance', 'TEXT'),
            ('ai_risk_alerts', 'TEXT'),
            ('ai_sentiment', 'VARCHAR(20)'),
            ('ai_updated_at', 'TIMESTAMP NULL'),
            ('ai_remarks', 'TEXT'),
        ]
        
        for field, col_type in fields:
            try:
                cursor.execute(f'ALTER TABLE cctv_news_broadcast ADD COLUMN {field} {col_type}')
                conn.commit()
                print(f'添加字段: {field}')
            except Exception as e:
                print(f'字段 {field}: {e}')
        
        try:
            cursor.execute('CREATE INDEX idx_ai_updated_at ON cctv_news_broadcast(ai_updated_at)')
            conn.commit()
            print('添加索引: idx_ai_updated_at')
        except Exception as e:
            print(f'索引: {e}')
        
        cursor.execute('DESCRIBE cctv_news_broadcast')
        cols = [row[0] for row in cursor.fetchall()]
        ai_cols = [c for c in cols if c.startswith('ai_')]
        print(f'\nAI相关字段: {ai_cols}')
finally:
    conn.close()
