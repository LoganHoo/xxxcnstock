#!/usr/bin/env python3
"""
通用股票选股回测对比工具
对比任意两个交易日的选股表现

用法:
    python compare_stock_performance.py --report-date 2026-04-11 --compare-date 2026-04-13
    python compare_stock_performance.py --report-date 2026-04-11 --days 2
"""
import polars as pl
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import json


def get_stock_price(code: str, date_str: str, data_dir: Path):
    """获取指定日期股票价格"""
    file_path = data_dir / f"{code}.parquet"
    if not file_path.exists():
        return None
    
    try:
        df = pl.read_parquet(file_path)
        df = df.filter(pl.col("trade_date") == date_str)
        if df.is_empty():
            return None
        return {
            "open": df["open"][0],
            "close": df["close"][0],
            "high": df["high"][0],
            "low": df["low"][0],
            "volume": df["volume"][0]
        }
    except Exception as e:
        return None


def load_report_stocks(report_date: str, reports_dir: Path):
    """从报告文件中加载选股结果"""
    report_file = reports_dir / f"fund_behavior_{report_date}.txt"
    
    if not report_file.exists():
        print(f"❌ 报告文件不存在: {report_file}")
        return None, None
    
    with open(report_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 解析波段趋势股
    band_stocks = []
    short_stocks = []
    
    for line in content.split('\n'):
        if '前10只波段股' in line:
            # 提取股票代码
            stocks_str = line.split('：')[-1]
            band_stocks = [s.strip() for s in stocks_str.split(',') if s.strip()]
        elif '前5只短线股' in line:
            stocks_str = line.split('：')[-1]
            short_stocks = [s.strip() for s in stocks_str.split(',') if s.strip()]
    
    return band_stocks, short_stocks


def analyze_performance(stocks: list, base_date: str, compare_date: str, 
                        data_dir: Path, stock_type: str):
    """分析股票表现"""
    print(f"\n{'='*100}")
    print(f"📊 {stock_type}（{len(stocks)}只）")
    print(f"{'='*100}")
    print(f"{'代码':<10} {'买入价':<12} {'卖出价':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态':<6} {'评价'}")
    print("-"*100)
    
    total_change = 0
    count = 0
    up_count = 0
    down_count = 0
    
    results = []
    
    for code in stocks:
        base_price = get_stock_price(code, base_date, data_dir)
        compare_price = get_stock_price(code, compare_date, data_dir)
        
        if base_price and compare_price:
            change = compare_price["close"] - base_price["close"]
            change_pct = (change / base_price["close"]) * 100
            total_change += change_pct
            count += 1
            
            if change > 0:
                up_count += 1
            elif change < 0:
                down_count += 1
            
            status = "📈" if change > 0 else "📉" if change < 0 else "➡️"
            
            if change_pct >= 9:
                eval_text = "🔥涨停"
            elif change_pct >= 5:
                eval_text = "强势"
            elif change_pct > 0:
                eval_text = "良好"
            elif change_pct > -3:
                eval_text = "一般"
            else:
                eval_text = "弱势"
            
            print(f"{code:<10} {base_price['close']:<12.2f} {compare_price['close']:<12.2f} "
                  f"{change:<+10.2f} {change_pct:<+10.2f}% {status:<6} {eval_text}")
            
            results.append({
                "code": code,
                "base_price": base_price['close'],
                "compare_price": compare_price['close'],
                "change": change,
                "change_pct": change_pct,
                "status": "up" if change > 0 else "down" if change < 0 else "flat"
            })
        else:
            missing = "买入日" if not base_price else "卖出日"
            print(f"{code:<10} {'数据缺失':<12} {'数据缺失':<12} ({missing}无数据)")
    
    if count > 0:
        avg_change = total_change / count
        print("-"*100)
        print(f"{'统计':<70} 涨:{up_count} 跌:{down_count} 平均:{avg_change:+.2f}%")
        return {
            "count": count,
            "up": up_count,
            "down": down_count,
            "avg_change": avg_change,
            "details": results
        }
    
    return None


def get_previous_trading_day(date: datetime) -> datetime:
    """获取前一个交易日（跳过周末）"""
    prev = date - timedelta(days=1)
    while prev.weekday() >= 5:  # 5=周六, 6=周日
        prev -= timedelta(days=1)
    return prev


def main():
    parser = argparse.ArgumentParser(description='股票选股回测对比工具')
    parser.add_argument('--report-date', type=str, default=None,
                        help='报告日期 (格式: YYYY-MM-DD)，默认为今天')
    parser.add_argument('--compare-date', type=str, default=None,
                        help='对比日期 (格式: YYYY-MM-DD)，默认为报告日期的前一个交易日')
    parser.add_argument('--days', type=int, default=None,
                        help='持有天数，设置则覆盖compare-date')
    parser.add_argument('--data-dir', type=str, 
                        default='/Volumes/Xdata/workstation/xxxcnstock/data/kline',
                        help='K线数据目录')
    parser.add_argument('--reports-dir', type=str,
                        default='/Volumes/Xdata/workstation/xxxcnstock/data/reports',
                        help='报告文件目录')
    parser.add_argument('--output', type=str, default=None,
                        help='输出JSON结果文件')
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    reports_dir = Path(args.reports_dir)
    
    # 确定报告日期（默认为今天）
    if args.report_date:
        report_date = args.report_date
    else:
        report_date = datetime.now().strftime('%Y-%m-%d')
    
    report_dt = datetime.strptime(report_date, '%Y-%m-%d')
    
    # 确定对比日期
    if args.days:
        # 按持有天数计算
        compare = report_dt + timedelta(days=args.days)
        while compare.weekday() >= 5:
            compare += timedelta(days=1)
        compare_date = compare.strftime('%Y-%m-%d')
    elif args.compare_date:
        compare_date = args.compare_date
    else:
        # 默认：报告日期的前一个交易日
        compare = get_previous_trading_day(report_dt)
        compare_date = compare.strftime('%Y-%m-%d')
    
    # 确定买入日期（对比日期的前一个交易日）
    compare_dt = datetime.strptime(compare_date, '%Y-%m-%d')
    buy_date = get_previous_trading_day(compare_dt)
    buy_date_str = buy_date.strftime('%Y-%m-%d')
    
    # 计算持有天数
    if args.days:
        hold_days = args.days
    else:
        hold_days = (datetime.strptime(compare_date, '%Y-%m-%d') - datetime.strptime(buy_date_str, '%Y-%m-%d')).days
    
    print("="*100)
    print(f"📈 选股回测报告")
    print(f"="*100)
    print(f"报告日期: {report_date}")
    print(f"买入日期: {buy_date_str}")
    print(f"卖出日期: {compare_date} (持有{hold_days}个交易日)")
    print(f"="*100)
    
    # 加载选股结果
    band_stocks, short_stocks = load_report_stocks(report_date, reports_dir)
    
    if not band_stocks and not short_stocks:
        print("❌ 未能从报告中解析出股票列表")
        return
    
    print(f"\n✅ 从报告加载:")
    print(f"   波段趋势股: {len(band_stocks)}只 - {band_stocks}")
    print(f"   短线打板股: {len(short_stocks)}只 - {short_stocks}")
    
    # 分析表现
    all_results = {
        "report_date": report_date,
        "buy_date": buy_date_str,
        "sell_date": compare_date,
        "band_trend": None,
        "short_term": None
    }
    
    if band_stocks:
        all_results["band_trend"] = analyze_performance(
            band_stocks, buy_date_str, compare_date, data_dir, "波段趋势股"
        )
    
    if short_stocks:
        all_results["short_term"] = analyze_performance(
            short_stocks, buy_date_str, compare_date, data_dir, "短线打板股"
        )
    
    # 总体统计
    print(f"\n{'='*100}")
    print("📊 总体表现汇总")
    print(f"{'='*100}")
    
    total_count = 0
    total_up = 0
    total_down = 0
    total_change = 0
    
    if all_results["band_trend"]:
        bt = all_results["band_trend"]
        print(f"\n波段趋势股（{bt['count']}只）:")
        print(f"  上涨: {bt['up']}只 | 下跌: {bt['down']}只 | 平均: {bt['avg_change']:+.2f}%")
        total_count += bt['count']
        total_up += bt['up']
        total_down += bt['down']
        total_change += bt['avg_change'] * bt['count']
    
    if all_results["short_term"]:
        st = all_results["short_term"]
        print(f"\n短线打板股（{st['count']}只）:")
        print(f"  上涨: {st['up']}只 | 下跌: {st['down']}只 | 平均: {st['avg_change']:+.2f}%")
        total_count += st['count']
        total_up += st['up']
        total_down += st['down']
        total_change += st['avg_change'] * st['count']
    
    if total_count > 0:
        overall_avg = total_change / total_count
        print(f"\n{'='*100}")
        print(f"总体表现（{total_count}只）:")
        print(f"  上涨: {total_up}只 | 下跌: {total_down}只 | 平均: {overall_avg:+.2f}%")
        
        if overall_avg > 0:
            print(f"\n✅ 选股策略盈利: +{overall_avg:.2f}%")
        else:
            print(f"\n❌ 选股策略亏损: {overall_avg:.2f}%")
        
        all_results["overall"] = {
            "count": total_count,
            "up": total_up,
            "down": total_down,
            "avg_change": overall_avg
        }
    
    print(f"{'='*100}")
    
    # 保存结果
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"\n💾 结果已保存: {args.output}")


if __name__ == "__main__":
    main()
