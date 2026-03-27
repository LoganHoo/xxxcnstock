"""
导入推荐股票数据到数据库
"""
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.stock_pick_verification_service import StockPickVerificationService


def load_picks_from_json(json_file: str) -> list:
    """从JSON文件加载推荐股票"""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    all_stocks = []
    
    filters = data.get('filters', {})
    
    for grade_key, grade_data in filters.items():
        if not isinstance(grade_data, dict):
            continue
        
        stocks = grade_data.get('stocks', [])
        for stock in stocks:
            stock['grade'] = stock.get('grade', grade_key.split('_')[0].upper())
            if stock['grade'] == 'BULLISH':
                stock['grade'] = 'A'
            all_stocks.append(stock)
    
    return all_stocks


def import_picks(json_file: str, recommend_date: str = None):
    """导入推荐股票"""
    if recommend_date is None:
        recommend_date = date(2026, 3, 24)
    else:
        recommend_date = date.fromisoformat(recommend_date)
    
    print(f"📂 加载文件: {json_file}")
    stocks = load_picks_from_json(json_file)
    print(f"📊 共 {len(stocks)} 只股票")
    
    grade_count = {}
    for s in stocks:
        g = s.get('grade', 'Unknown')
        grade_count[g] = grade_count.get(g, 0) + 1
    print(f"📈 评级分布: {grade_count}")
    
    service = StockPickVerificationService()
    
    saved = service.save_recommendations(stocks, recommend_date)
    print(f"✅ 成功保存 {saved} 条记录")
    
    return saved


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='导入推荐股票数据')
    parser.add_argument('json_file', help='JSON文件路径')
    parser.add_argument('--date', help='推荐日期 (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    import_picks(args.json_file, args.date)
