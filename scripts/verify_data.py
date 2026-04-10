#!/usr/bin/env python3
"""数据验证脚本"""
import json
from pathlib import Path

data_dir = Path('/app/data')

files_to_check = [
    ('foreign_index.json', '外盘数据'),
    ('macro_data.json', '宏观数据'),
    ('oil_dollar_data.json', '石油美元'),
    ('commodities_data.json', '大宗商品'),
    ('sentiment_data.json', '情绪数据'),
    ('news_data.json', '新闻数据'),
    ('market_review.json', '市场复盘'),
]

print('=' * 60)
print('数据验证报告')
print('=' * 60)

for filename, desc in files_to_check:
    filepath = data_dir / filename
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f'✅ {desc} ({filename})')
        print(f'   更新时间: {data.get("update_time", data.get("date", "N/A"))}')

        if filename == 'foreign_index.json':
            us = data.get('us_index', {}).get('data', {})
            print(f'   美股数据: {len(us)} 个指数')
        elif filename == 'macro_data.json':
            dxy = data.get('dxy', {})
            print(f'   美元指数: {dxy.get("value", "N/A")}')
        elif filename == 'oil_dollar_data.json':
            brent = data.get('oil', {}).get('brent', {})
            print(f'   布伦特原油: ${brent.get("price", "N/A")}')
        elif filename == 'commodities_data.json':
            gold = data.get('metals', {}).get('gold', {})
            print(f'   黄金: ${gold.get("price", "N/A")}')
        elif filename == 'sentiment_data.json':
            fg = data.get('fear_greed', {})
            bomb = data.get('bomb_rate', {})
            print(f'   恐慌贪婪: {fg.get("value", "N/A")}')
            print(f'   炸板率: {bomb.get("rate", "N/A")}%')
        elif filename == 'news_data.json':
            news = data.get('all', [])
            print(f'   新闻数量: {len(news)} 条')
        elif filename == 'market_review.json':
            summary = data.get('summary', {})
            print(f'   上涨: {summary.get("rising_count", 0)} | 下跌: {summary.get("falling_count", 0)}')
    else:
        print(f'❌ {desc} ({filename}) - 文件不存在')
    print()

print('=' * 60)
