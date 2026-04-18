#!/usr/bin/env python3
"""
策略回测框架 - 批量回测选股策略表现

用法:
    python backtest_strategy.py --start-date 2026-04-01 --end-date 2026-04-11
    python backtest_strategy.py --start-date 2026-04-01 --end-date 2026-04-11 --hold-days 3
"""
import polars as pl
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import json
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class TradeRecord:
    """交易记录"""
    code: str
    buy_date: str
    buy_price: float
    sell_date: str
    sell_price: float
    change_pct: float
    stock_type: str  # 'band' or 'short'


@dataclass
class DailyResult:
    """每日回测结果"""
    report_date: str
    buy_date: str
    sell_date: str
    band_stocks: List[str]
    short_stocks: List[str]
    band_return: float
    short_return: float
    overall_return: float
    trades: List[TradeRecord]


class StrategyBacktester:
    """策略回测器"""
    
    def __init__(self, data_dir: Path, reports_dir: Path):
        self.data_dir = data_dir
        self.reports_dir = reports_dir
        self.results: List[DailyResult] = []
    
    def get_stock_price(self, code: str, date_str: str) -> Optional[Dict]:
        """获取指定日期股票价格"""
        file_path = self.data_dir / f"{code}.parquet"
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
            }
        except Exception:
            return None
    
    def load_report_stocks(self, report_date: str) -> tuple:
        """从报告文件中加载选股结果"""
        report_file = self.reports_dir / f"fund_behavior_{report_date}.txt"
        
        if not report_file.exists():
            return None, None
        
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        band_stocks = []
        short_stocks = []
        
        for line in content.split('\n'):
            if '前10只波段股' in line:
                stocks_str = line.split('：')[-1]
                band_stocks = [s.strip() for s in stocks_str.split(',') if s.strip()]
            elif '前5只短线股' in line:
                stocks_str = line.split('：')[-1]
                short_stocks = [s.strip() for s in stocks_str.split(',') if s.strip()]
        
        return band_stocks, short_stocks
    
    def get_previous_trading_day(self, date: datetime) -> datetime:
        """获取前一个交易日"""
        prev = date - timedelta(days=1)
        while prev.weekday() >= 5:
            prev -= timedelta(days=1)
        return prev
    
    def backtest_single_day(self, report_date: str, hold_days: int = 1) -> Optional[DailyResult]:
        """回测单日表现"""
        band_stocks, short_stocks = self.load_report_stocks(report_date)
        
        if not band_stocks and not short_stocks:
            return None
        
        report_dt = datetime.strptime(report_date, '%Y-%m-%d')
        
        # 确定买入和卖出日期
        if hold_days == 1:
            # 默认：报告日期的前一个交易日买入，报告日期卖出
            sell_date = self.get_previous_trading_day(report_dt)
            buy_date = self.get_previous_trading_day(sell_date)
        else:
            # 指定持有天数
            buy_date = self.get_previous_trading_day(report_dt)
            sell_date = report_dt + timedelta(days=hold_days)
            while sell_date.weekday() >= 5:
                sell_date += timedelta(days=1)
        
        buy_date_str = buy_date.strftime('%Y-%m-%d')
        sell_date_str = sell_date.strftime('%Y-%m-%d')
        
        trades = []
        band_returns = []
        short_returns = []
        
        # 回测波段趋势股
        for code in band_stocks:
            buy_price = self.get_stock_price(code, buy_date_str)
            sell_price = self.get_stock_price(code, sell_date_str)
            
            if buy_price and sell_price:
                change_pct = (sell_price["close"] - buy_price["close"]) / buy_price["close"] * 100
                band_returns.append(change_pct)
                trades.append(TradeRecord(
                    code=code,
                    buy_date=buy_date_str,
                    buy_price=buy_price["close"],
                    sell_date=sell_date_str,
                    sell_price=sell_price["close"],
                    change_pct=change_pct,
                    stock_type='band'
                ))
        
        # 回测短线打板股
        for code in short_stocks:
            buy_price = self.get_stock_price(code, buy_date_str)
            sell_price = self.get_stock_price(code, sell_date_str)
            
            if buy_price and sell_price:
                change_pct = (sell_price["close"] - buy_price["close"]) / buy_price["close"] * 100
                short_returns.append(change_pct)
                trades.append(TradeRecord(
                    code=code,
                    buy_date=buy_date_str,
                    buy_price=buy_price["close"],
                    sell_date=sell_date_str,
                    sell_price=sell_price["close"],
                    change_pct=change_pct,
                    stock_type='short'
                ))
        
        band_return = sum(band_returns) / len(band_returns) if band_returns else 0
        short_return = sum(short_returns) / len(short_returns) if short_returns else 0
        
        all_returns = band_returns + short_returns
        overall_return = sum(all_returns) / len(all_returns) if all_returns else 0
        
        return DailyResult(
            report_date=report_date,
            buy_date=buy_date_str,
            sell_date=sell_date_str,
            band_stocks=band_stocks,
            short_stocks=short_stocks,
            band_return=band_return,
            short_return=short_return,
            overall_return=overall_return,
            trades=trades
        )
    
    def run_backtest(self, start_date: str, end_date: str, hold_days: int = 1):
        """运行批量回测"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current = start
        while current <= end:
            if current.weekday() < 5:  # 只处理工作日
                date_str = current.strftime('%Y-%m-%d')
                result = self.backtest_single_day(date_str, hold_days)
                if result:
                    self.results.append(result)
            current += timedelta(days=1)
    
    def generate_report(self) -> Dict:
        """生成回测报告"""
        if not self.results:
            return {"error": "没有回测结果"}
        
        total_days = len(self.results)
        
        # 总体统计
        all_returns = [r.overall_return for r in self.results]
        band_returns = [r.band_return for r in self.results]
        short_returns = [r.short_return for r in self.results]
        
        # 胜率统计
        win_days = sum(1 for r in all_returns if r > 0)
        loss_days = sum(1 for r in all_returns if r < 0)
        
        # 累计收益
        cumulative_return = 1.0
        for r in all_returns:
            cumulative_return *= (1 + r / 100)
        cumulative_return = (cumulative_return - 1) * 100
        
        # 所有交易
        all_trades = []
        for result in self.results:
            all_trades.extend(result.trades)
        
        win_trades = [t for t in all_trades if t.change_pct > 0]
        loss_trades = [t for t in all_trades if t.change_pct < 0]
        
        # 最佳/最差个股
        if all_trades:
            best_trade = max(all_trades, key=lambda x: x.change_pct)
            worst_trade = min(all_trades, key=lambda x: x.change_pct)
        else:
            best_trade = worst_trade = None
        
        return {
            "summary": {
                "total_days": total_days,
                "win_days": win_days,
                "loss_days": loss_days,
                "win_rate_days": win_days / total_days * 100 if total_days > 0 else 0,
                "avg_daily_return": sum(all_returns) / len(all_returns) if all_returns else 0,
                "cumulative_return": cumulative_return,
                "max_daily_gain": max(all_returns) if all_returns else 0,
                "max_daily_loss": min(all_returns) if all_returns else 0,
            },
            "by_type": {
                "band": {
                    "avg_return": sum(band_returns) / len(band_returns) if band_returns else 0,
                    "win_days": sum(1 for r in band_returns if r > 0),
                },
                "short": {
                    "avg_return": sum(short_returns) / len(short_returns) if short_returns else 0,
                    "win_days": sum(1 for r in short_returns if r > 0),
                }
            },
            "trades": {
                "total": len(all_trades),
                "win": len(win_trades),
                "loss": len(loss_trades),
                "win_rate": len(win_trades) / len(all_trades) * 100 if all_trades else 0,
                "avg_win": sum(t.change_pct for t in win_trades) / len(win_trades) if win_trades else 0,
                "avg_loss": sum(t.change_pct for t in loss_trades) / len(loss_trades) if loss_trades else 0,
            },
            "best_trade": {
                "code": best_trade.code,
                "change_pct": best_trade.change_pct,
                "buy_date": best_trade.buy_date,
                "sell_date": best_trade.sell_date,
            } if best_trade else None,
            "worst_trade": {
                "code": worst_trade.code,
                "change_pct": worst_trade.change_pct,
                "buy_date": worst_trade.buy_date,
                "sell_date": worst_trade.sell_date,
            } if worst_trade else None,
            "daily_results": [
                {
                    "date": r.report_date,
                    "band_return": r.band_return,
                    "short_return": r.short_return,
                    "overall_return": r.overall_return,
                }
                for r in self.results
            ]
        }
    
    def print_report(self):
        """打印回测报告"""
        report = self.generate_report()
        
        if "error" in report:
            print(f"❌ {report['error']}")
            return
        
        s = report["summary"]
        t = report["trades"]
        
        print("\n" + "="*100)
        print("📊 策略回测报告")
        print("="*100)
        
        print(f"\n【回测概况】")
        print(f"  回测天数: {s['total_days']}天")
        print(f"  盈利天数: {s['win_days']}天")
        print(f"  亏损天数: {s['loss_days']}天")
        print(f"  日胜率: {s['win_rate_days']:.1f}%")
        
        print(f"\n【收益表现】")
        print(f"  日均收益率: {s['avg_daily_return']:+.2f}%")
        print(f"  累计收益率: {s['cumulative_return']:+.2f}%")
        print(f"  最大单日盈利: {s['max_daily_gain']:+.2f}%")
        print(f"  最大单日亏损: {s['max_daily_loss']:+.2f}%")
        
        print(f"\n【交易统计】")
        print(f"  总交易次数: {t['total']}次")
        print(f"  盈利次数: {t['win']}次")
        print(f"  亏损次数: {t['loss']}次")
        print(f"  交易胜率: {t['win_rate']:.1f}%")
        print(f"  平均盈利: {t['avg_win']:+.2f}%")
        print(f"  平均亏损: {t['avg_loss']:+.2f}%")
        
        b = report["by_type"]["band"]
        sh = report["by_type"]["short"]
        print(f"\n【分类型表现】")
        print(f"  波段趋势: 平均{b['avg_return']:+.2f}%, 盈利{b['win_days']}天")
        print(f"  短线打板: 平均{sh['avg_return']:+.2f}%, 盈利{sh['win_days']}天")
        
        if report["best_trade"]:
            bt = report["best_trade"]
            print(f"\n【最佳交易】")
            print(f"  {bt['code']}: {bt['change_pct']:+.2f}% ({bt['buy_date']} -> {bt['sell_date']})")
        
        if report["worst_trade"]:
            wt = report["worst_trade"]
            print(f"\n【最差交易】")
            print(f"  {wt['code']}: {wt['change_pct']:+.2f}% ({wt['buy_date']} -> {wt['sell_date']})")
        
        print("\n【每日明细】")
        print("-"*100)
        print(f"{'日期':<12} {'波段收益':<12} {'短线收益':<12} {'总体收益':<12} {'状态'}")
        print("-"*100)
        for d in report["daily_results"]:
            status = "📈" if d['overall_return'] > 0 else "📉" if d['overall_return'] < 0 else "➡️"
            print(f"{d['date']:<12} {d['band_return']:<+12.2f}% {d['short_return']:<+12.2f}% {d['overall_return']:<+12.2f}% {status}")
        
        print("="*100)
        
        return report


def main():
    parser = argparse.ArgumentParser(description='策略批量回测工具')
    parser.add_argument('--start-date', type=str, required=True,
                        help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                        help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--hold-days', type=int, default=1,
                        help='持有天数 (默认: 1)')
    parser.add_argument('--data-dir', type=str,
                        default='/Volumes/Xdata/workstation/xxxcnstock/data/kline',
                        help='K线数据目录')
    parser.add_argument('--reports-dir', type=str,
                        default='/Volumes/Xdata/workstation/xxxcnstock/data/reports',
                        help='报告目录')
    parser.add_argument('--output', type=str, default=None,
                        help='输出JSON文件')
    
    args = parser.parse_args()
    
    backtester = StrategyBacktester(
        data_dir=Path(args.data_dir),
        reports_dir=Path(args.reports_dir)
    )
    
    print(f"🔄 开始回测: {args.start_date} ~ {args.end_date}, 持有{args.hold_days}天")
    backtester.run_backtest(args.start_date, args.end_date, args.hold_days)
    
    report = backtester.print_report()
    
    if args.output and report:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\n💾 报告已保存: {args.output}")


if __name__ == "__main__":
    main()
