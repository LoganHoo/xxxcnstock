"""
对比昨日推荐股票今日表现
"""
import json
import re
import polars as pl
from pathlib import Path
from datetime import datetime

def parse_yesterday_report(file_path: str) -> list:
    """解析昨日推荐报告"""
    picks = []
    
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
            match = re.match(r'\s*(\d{6})\s+(\S+)\s+([\d.]+)元\s+([+-]?[\d.]+)%', line)
            if match:
                code, name, price, change = match.groups()
                picks.append({
                    'code': code,
                    'name': name,
                    'yesterday_price': float(price),
                    'yesterday_change': float(change),
                    'grade': current_grade
                })
    
    return picks

def load_today_prices_from_kline(codes: list) -> dict:
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
                print(f"读取 {code} 失败: {e}")
    
    return today_prices

def load_today_data(file_path: str) -> dict:
    """加载今日推荐数据"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    prices = {}
    for grade, info in data['filters'].items():
        for stock in info.get('stocks', []):
            prices[stock['code']] = {
                'price': stock['price'],
                'change_pct': stock['change_pct'],
                'name': stock['name'],
                'enhanced_score': stock['enhanced_score'],
                'grade': grade,
                'support_strong': stock.get('support_strong', 0),
                'resistance_strong': stock.get('resistance_strong', 0),
                'support_status': stock.get('support_status', '持平'),
                'resistance_status': stock.get('resistance_status', '持平'),
                'ma20': stock.get('ma20', 0),
                'ma60': stock.get('ma60', 0),
                'cvd_signal': stock.get('cvd_signal', 'neutral'),
                'reasons': stock.get('reasons', '')
            }
    return prices

def main():
    yesterday_file = Path("reports/daily_picks_20260324.txt")
    today_file = Path("reports/daily_picks_20260325.json")
    
    yesterday_picks = parse_yesterday_report(yesterday_file)
    print(f"昨日推荐股票数: {len(yesterday_picks)}")
    
    unique_picks = {}
    grade_priority = {'S': 1, 'A': 2, 'BULLISH': 3}
    for pick in yesterday_picks:
        code = pick['code']
        if code not in unique_picks:
            unique_picks[code] = pick
        else:
            existing = unique_picks[code]
            if grade_priority.get(pick['grade'], 99) < grade_priority.get(existing['grade'], 99):
                unique_picks[code] = pick
    
    yesterday_picks = list(unique_picks.values())
    print(f"去重后股票数: {len(yesterday_picks)}")
    
    today_recommend = load_today_data(today_file)
    print(f"今日推荐股票数: {len(today_recommend)}")
    
    codes = [p['code'] for p in yesterday_picks]
    today_kline_prices = load_today_prices_from_kline(codes)
    print(f"从K线获取今日价格: {len(today_kline_prices)} 只")
    
    results = []
    for pick in yesterday_picks:
        code = pick['code']
        
        today_price = None
        today_change = 0
        
        if code in today_recommend:
            today_price = today_recommend[code]['price']
            today_change = today_recommend[code]['change_pct']
        elif code in today_kline_prices:
            today_price = today_kline_prices[code]['price']
            today_change = today_kline_prices[code]['change_pct']
        
        if today_price and today_price > 0:
            profit_pct = round((today_price - pick['yesterday_price']) / pick['yesterday_price'] * 100, 2)
            results.append({
                'code': code,
                'name': pick['name'],
                'yesterday_price': pick['yesterday_price'],
                'today_price': today_price,
                'today_change': today_change,
                'yesterday_grade': pick['grade'],
                'profit_pct': profit_pct,
                'in_today_recommend': code in today_recommend
            })
    
    results.sort(key=lambda x: x['profit_pct'], reverse=True)
    
    print("\n" + "="*100)
    print("昨日推荐股票今日表现对比 (3月24日推荐 → 3月25日收盘)")
    print("="*100)
    
    print(f"\n{'代码':<8} {'名称':<10} {'昨日价':<10} {'今日价':<10} {'今日涨跌%':<12} {'昨日评级':<10} {'盈亏%':<10} {'状态'}")
    print("-"*100)
    
    winners = losers = flat = 0
    total_return = 0
    
    for r in results:
        emoji = "✅" if r['profit_pct'] > 0 else "❌" if r['profit_pct'] < 0 else "➖"
        status = "★今日推荐" if r['in_today_recommend'] else ""
        print(f"{r['code']:<8} {r['name']:<10} {r['yesterday_price']:<10.2f} {r['today_price']:<10.2f} {r['today_change']:<12.2f} {r['yesterday_grade']:<10} {r['profit_pct']:<10.2f} {emoji} {status}")
        if r['profit_pct'] > 0:
            winners += 1
        elif r['profit_pct'] < 0:
            losers += 1
        else:
            flat += 1
        total_return += r['profit_pct']
    
    print("-"*100)
    print(f"\n📊 统计摘要:")
    print(f"  上涨股票: {winners} 只 ({winners/len(results)*100:.1f}%)")
    print(f"  下跌股票: {losers} 只 ({losers/len(results)*100:.1f}%)")
    print(f"  持平股票: {flat} 只 ({flat/len(results)*100:.1f}%)")
    print(f"  平均收益: {total_return/len(results):.2f}%")
    print(f"  胜率: {winners/(winners+losers)*100:.1f}%" if (winners+losers) > 0 else "  胜率: N/A")
    
    s_results = [r for r in results if r['yesterday_grade'] == 'S']
    if s_results:
        s_return = sum([r['profit_pct'] for r in s_results]) / len(s_results)
        s_winners = len([r for r in s_results if r['profit_pct'] > 0])
        s_losers = len([r for r in s_results if r['profit_pct'] < 0])
        print(f"\n🏆 S级股票表现:")
        print(f"  数量: {len(s_results)} 只")
        print(f"  平均收益: {s_return:.2f}%")
        print(f"  胜率: {s_winners/(s_winners+s_losers)*100:.1f}%" if (s_winners+s_losers) > 0 else "  胜率: N/A")
    
    a_results = [r for r in results if r['yesterday_grade'] == 'A']
    if a_results:
        a_return = sum([r['profit_pct'] for r in a_results]) / len(a_results)
        a_winners = len([r for r in a_results if r['profit_pct'] > 0])
        a_losers = len([r for r in a_results if r['profit_pct'] < 0])
        print(f"\n📈 A级股票表现:")
        print(f"  数量: {len(a_results)} 只")
        print(f"  平均收益: {a_return:.2f}%")
        print(f"  胜率: {a_winners/(a_winners+a_losers)*100:.1f}%" if (a_winners+a_losers) > 0 else "  胜率: N/A")
    
    bullish_results = [r for r in results if r['yesterday_grade'] == 'BULLISH']
    if bullish_results:
        b_return = sum([r['profit_pct'] for r in bullish_results]) / len(bullish_results)
        b_winners = len([r for r in bullish_results if r['profit_pct'] > 0])
        b_losers = len([r for r in bullish_results if r['profit_pct'] < 0])
        print(f"\n🐂 多头排列股票表现:")
        print(f"  数量: {len(bullish_results)} 只")
        print(f"  平均收益: {b_return:.2f}%")
        print(f"  胜率: {b_winners/(b_winners+b_losers)*100:.1f}%" if (b_winners+b_losers) > 0 else "  胜率: N/A")

if __name__ == "__main__":
    main()
