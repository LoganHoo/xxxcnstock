"""
详细分析昨日推荐股票今日表现
"""
import json
import re
import polars as pl
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def parse_report_by_section(file_path: str) -> dict:
    """按分类解析推荐报告"""
    sections = {'S': [], 'A': [], 'BULLISH': []}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    current_grade = None
    
    for line in content.split('\n'):
        if '【S级 - 强烈推荐】' in line:
            current_grade = 'S'
            continue
        if '【A级 - 建议关注】' in line:
            current_grade = 'A'
            continue
        if '【多头排列+今日上涨】' in line:
            current_grade = 'BULLISH'
            continue
        if '【统计摘要】' in line:
            current_grade = None
            continue
        
        if current_grade and line.strip():
            match = re.match(r'\s*(\d{6})\s+(\S+)\s+([\d.]+)元\s+([+-]?[\d.]+)%.*评分(\d+)', line)
            if match:
                code, name, price, change, score = match.groups()
                sections[current_grade].append({
                    'code': code,
                    'name': name,
                    'yesterday_price': float(price),
                    'yesterday_change': float(change),
                    'score': int(score)
                })
    
    return sections

def load_today_prices(codes: list) -> dict:
    """从K线数据加载今日价格"""
    today_prices = {}
    
    kline_dir = Path("data/kline")
    if not kline_dir.exists():
        return today_prices
    
    for code in codes:
        kline_file = kline_dir / f"{code}.parquet"
        if kline_file.exists():
            try:
                df = pl.read_parquet(str(kline_file))
                df = df.sort('trade_date', descending=True)
                latest = df.head(2)
                
                if len(latest) >= 1:
                    rows = latest.to_dicts()
                    today_row = rows[0]
                    prev_close = rows[1]['close'] if len(rows) > 1 else today_row.get('close', 0)
                    
                    close = today_row.get('close', 0)
                    change_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0
                    
                    today_prices[code] = {
                        'price': close,
                        'change_pct': change_pct,
                        'trade_date': str(today_row.get('trade_date', ''))
                    }
            except Exception as e:
                pass
    
    return today_prices

def analyze_section(sections: dict, today_prices: dict, grade_name: str, grade_key: str):
    """分析单个分类"""
    stocks = sections.get(grade_key, [])
    if not stocks:
        return None
    
    results = []
    for stock in stocks:
        code = stock['code']
        if code in today_prices:
            today = today_prices[code]
            profit_pct = round((today['price'] - stock['yesterday_price']) / stock['yesterday_price'] * 100, 2)
            results.append({
                **stock,
                'today_price': today['price'],
                'today_change': today['change_pct'],
                'profit_pct': profit_pct
            })
    
    results.sort(key=lambda x: x['profit_pct'], reverse=True)
    
    winners = len([r for r in results if r['profit_pct'] > 0])
    losers = len([r for r in results if r['profit_pct'] < 0])
    flat = len([r for r in results if r['profit_pct'] == 0])
    total_return = sum([r['profit_pct'] for r in results])
    
    return {
        'stocks': results,
        'count': len(results),
        'winners': winners,
        'losers': losers,
        'flat': flat,
        'avg_return': total_return / len(results) if results else 0,
        'win_rate': winners / (winners + losers) * 100 if (winners + losers) > 0 else 0
    }

def main():
    yesterday_file = Path("reports/daily_picks_20260324.txt")
    
    sections = parse_report_by_section(yesterday_file)
    
    all_codes = []
    for grade_stocks in sections.values():
        all_codes.extend([s['code'] for s in grade_stocks])
    
    today_prices = load_today_prices(list(set(all_codes)))
    
    print("="*100)
    print("3月24日推荐股票 → 3月25日收盘表现详细分析")
    print("="*100)
    
    total_stocks = sum(len(s) for s in sections.values())
    unique_codes = len(set(all_codes))
    print(f"\n📋 推荐统计:")
    print(f"  总推荐数: {total_stocks} 条")
    print(f"  去重后: {unique_codes} 只股票")
    print(f"  获取到今日价格: {len(today_prices)} 只")
    
    grade_names = {'S': '🏆 S级 - 强烈推荐', 'A': '📈 A级 - 建议关注', 'BULLISH': '🐂 多头排列+今日上涨'}
    
    all_results = {}
    
    for grade_key in ['S', 'A', 'BULLISH']:
        result = analyze_section(sections, today_prices, grade_names[grade_key], grade_key)
        if result:
            all_results[grade_key] = result
            
            print(f"\n{'='*100}")
            print(f"{grade_names[grade_key]} ({result['count']}只)")
            print(f"{'='*100}")
            
            print(f"\n{'代码':<8} {'名称':<10} {'评分':<6} {'昨日价':<10} {'今日价':<10} {'今日涨跌%':<12} {'盈亏%':<10} {'状态'}")
            print("-"*100)
            
            for r in result['stocks']:
                emoji = "✅" if r['profit_pct'] > 0 else "❌" if r['profit_pct'] < 0 else "➖"
                print(f"{r['code']:<8} {r['name']:<10} {r['score']:<6} {r['yesterday_price']:<10.2f} {r['today_price']:<10.2f} {r['today_change']:<12.2f} {r['profit_pct']:<10.2f} {emoji}")
            
            print("-"*100)
            print(f"📈 {grade_names[grade_key]} 统计:")
            print(f"  上涨: {result['winners']} 只 | 下跌: {result['losers']} 只 | 持平: {result['flat']} 只")
            print(f"  平均收益: {result['avg_return']:.2f}%")
            print(f"  胜率: {result['win_rate']:.1f}%")
    
    print(f"\n{'='*100}")
    print("📊 总体统计")
    print(f"{'='*100}")
    
    total_winners = sum(r['winners'] for r in all_results.values())
    total_losers = sum(r['losers'] for r in all_results.values())
    total_flat = sum(r['flat'] for r in all_results.values())
    total_count = sum(r['count'] for r in all_results.values())
    
    all_profits = []
    for result in all_results.values():
        for stock in result['stocks']:
            all_profits.append(stock['profit_pct'])
    
    avg_return = sum(all_profits) / len(all_profits) if all_profits else 0
    win_rate = total_winners / (total_winners + total_losers) * 100 if (total_winners + total_losers) > 0 else 0
    
    print(f"\n  总股票数: {total_count} 条")
    print(f"  上涨: {total_winners} 只 ({total_winners/total_count*100:.1f}%)")
    print(f"  下跌: {total_losers} 只 ({total_losers/total_count*100:.1f}%)")
    print(f"  持平: {total_flat} 只 ({total_flat/total_count*100:.1f}%)")
    print(f"  平均收益: {avg_return:.2f}%")
    print(f"  胜率: {win_rate:.1f}%")
    
    print(f"\n{'='*100}")
    print("🎯 Top 5 最佳表现")
    print(f"{'='*100}")
    
    all_stocks = []
    for result in all_results.values():
        all_stocks.extend(result['stocks'])
    all_stocks.sort(key=lambda x: x['profit_pct'], reverse=True)
    
    for i, r in enumerate(all_stocks[:5], 1):
        print(f"  {i}. {r['code']} {r['name']}: +{r['profit_pct']:.2f}% (昨日{r['yesterday_price']:.2f} → 今日{r['today_price']:.2f})")
    
    print(f"\n{'='*100}")
    print("⚠️ Top 5 最差表现")
    print(f"{'='*100}")
    
    for i, r in enumerate(all_stocks[-5:][::-1], 1):
        print(f"  {i}. {r['code']} {r['name']}: {r['profit_pct']:.2f}% (昨日{r['yesterday_price']:.2f} → 今日{r['today_price']:.2f})")

if __name__ == "__main__":
    main()
