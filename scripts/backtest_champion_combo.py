#!/usr/bin/env python3
"""
冠军组合回测验证

回测选股策略的历史表现

使用方法:
    python scripts/backtest_champion_combo.py --start-date 2025-01-01 --end-date 2025-04-11 --top-n 20
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """回测结果"""
    date: str
    selected_codes: List[str]
    next_day_returns: List[float]
    avg_return: float
    win_rate: float
    cumulative_return: float


class ChampionBacktester:
    """冠军组合回测器"""
    
    CHAMPION_FACTORS = {
        'rsi': 0.30,
        'macd': 0.30,
        'bollinger': 0.20,
        'atr': 0.20
    }
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.kline_dir = self.data_dir / "kline"
        self.all_dates = self._get_all_trade_dates()
        
    def _get_all_trade_dates(self) -> List[str]:
        """获取所有交易日期"""
        dates = set()
        for parquet_file in self.kline_dir.glob("*.parquet"):
            try:
                df = pl.read_parquet(parquet_file)
                if 'trade_date' in df.columns:
                    dates.update(df['trade_date'].cast(pl.Utf8).to_list())
            except:
                pass
        return sorted(list(dates))
    
    def get_next_trade_date(self, date: str) -> Optional[str]:
        """获取下一个交易日"""
        try:
            idx = self.all_dates.index(date)
            if idx + 1 < len(self.all_dates):
                return self.all_dates[idx + 1]
        except ValueError:
            pass
        return None
    
    def load_stock_data(self, code: str, date: str) -> Optional[pl.DataFrame]:
        """加载单只股票数据"""
        file_path = self.kline_dir / f"{code}.parquet"
        
        if not file_path.exists():
            return None
        
        try:
            df = pl.read_parquet(file_path)
            
            df = df.with_columns([
                pl.col("code").cast(pl.Utf8),
                pl.col("trade_date").cast(pl.Utf8),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ])
            
            df = df.filter(pl.col("trade_date") <= date)
            
            if len(df) < 30:
                return None
            
            return df.sort("trade_date")
        except:
            return None
    
    def get_next_day_return(self, code: str, current_date: str) -> Optional[float]:
        """获取次日收益率"""
        next_date = self.get_next_trade_date(current_date)
        if not next_date:
            return None
        
        file_path = self.kline_dir / f"{code}.parquet"
        if not file_path.exists():
            return None
        
        try:
            df = pl.read_parquet(file_path)
            df = df.with_columns([
                pl.col("trade_date").cast(pl.Utf8),
                pl.col("close").cast(pl.Float64),
            ])
            
            # 获取当前日期和次日的收盘价
            current_row = df.filter(pl.col("trade_date") == current_date)
            next_row = df.filter(pl.col("trade_date") == next_date)
            
            if len(current_row) == 0 or len(next_row) == 0:
                return None
            
            current_close = current_row["close"][0]
            next_close = next_row["close"][0]
            
            if current_close > 0:
                return (next_close - current_close) / current_close
            return None
        except:
            return None
    
    def apply_filters(self, df: pl.DataFrame) -> bool:
        """应用过滤器"""
        if len(df) == 0:
            return False
        
        latest = df.tail(1)
        
        volume = latest["volume"][0] if "volume" in latest.columns else 0
        if volume < 1000000:
            return False
        
        close = latest["close"][0] if "close" in latest.columns else 0
        if close < 3.0 or close > 200.0:
            return False
        
        code = latest["code"][0] if "code" in latest.columns else ""
        if str(code).startswith("688") or str(code).startswith("8") or \
           str(code).startswith("4") or str(code).startswith("300"):
            return False
        
        return True
    
    def calculate_factors(self, df: pl.DataFrame) -> Dict[str, float]:
        """计算因子"""
        factors = {}
        
        if len(df) < 30:
            return factors
        
        close = df["close"].to_numpy()
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()
        
        # RSI
        try:
            delta = np.diff(close)
            gain = np.where(delta > 0, delta, 0)
            loss = np.where(delta < 0, -delta, 0)
            avg_gain = np.mean(gain[-14:])
            avg_loss = np.mean(loss[-14:])
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                factors['rsi'] = 100 - (100 / (1 + rs))
            else:
                factors['rsi'] = 50
        except:
            factors['rsi'] = 50
        
        # MACD
        try:
            ema12 = df["close"].ewm_mean(span=12).tail(1)[0]
            ema26 = df["close"].ewm_mean(span=26).tail(1)[0]
            factors['macd'] = ema12 - ema26
        except:
            factors['macd'] = 0
        
        # Bollinger
        try:
            ma20 = np.mean(close[-20:])
            std20 = np.std(close[-20:])
            if std20 > 0:
                factors['bollinger'] = (close[-1] - ma20) / (2 * std20)
            else:
                factors['bollinger'] = 0
        except:
            factors['bollinger'] = 0
        
        # ATR
        try:
            tr1 = high[-1] - low[-1]
            tr2 = abs(high[-1] - close[-2])
            tr3 = abs(low[-1] - close[-2])
            tr = max(tr1, tr2, tr3)
            factors['atr'] = tr
        except:
            factors['atr'] = 0
        
        return factors
    
    def calculate_score(self, factors: Dict[str, float]) -> float:
        """计算综合评分"""
        if not factors:
            return 0
        
        score = 0
        total_weight = sum(abs(w) for w in self.CHAMPION_FACTORS.values())
        
        for factor_name, weight in self.CHAMPION_FACTORS.items():
            if factor_name in factors:
                score += factors[factor_name] * (weight / total_weight)
        
        return score
    
    def select_stocks_for_date(self, date: str, top_n: int = 20) -> List[Tuple[str, float]]:
        """为指定日期选股"""
        results = []
        
        for parquet_file in self.kline_dir.glob("*.parquet"):
            code = parquet_file.stem
            
            df = self.load_stock_data(code, date)
            if df is None:
                continue
            
            if not self.apply_filters(df):
                continue
            
            factors = self.calculate_factors(df)
            if not factors:
                continue
            
            score = self.calculate_score(factors)
            results.append((code, score))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_n]
    
    def run_backtest(self, start_date: str, end_date: str, top_n: int = 20) -> List[BacktestResult]:
        """运行回测"""
        # 筛选日期范围内的交易日（需要留一天计算收益）
        test_dates = [d for d in self.all_dates if start_date <= d <= end_date]
        # 去掉最后一天（无法计算次日收益）
        test_dates = test_dates[:-1]
        
        logger.info(f"回测区间: {start_date} ~ {end_date}")
        logger.info(f"回测天数: {len(test_dates)}")
        
        results = []
        cumulative_return = 1.0
        
        for date in test_dates:
            logger.info(f"回测日期: {date}")
            
            # 选股
            selected = self.select_stocks_for_date(date, top_n)
            selected_codes = [s[0] for s in selected]
            
            if not selected_codes:
                continue
            
            # 计算次日收益
            next_day_returns = []
            for code in selected_codes:
                ret = self.get_next_day_return(code, date)
                if ret is not None:
                    next_day_returns.append(ret)
            
            if not next_day_returns:
                continue
            
            avg_return = np.mean(next_day_returns)
            win_rate = sum(1 for r in next_day_returns if r > 0) / len(next_day_returns)
            cumulative_return = cumulative_return * (1 + avg_return)
            
            result = BacktestResult(
                date=date,
                selected_codes=selected_codes,
                next_day_returns=next_day_returns,
                avg_return=avg_return,
                win_rate=win_rate,
                cumulative_return=cumulative_return
            )
            results.append(result)
        
        return results


def print_backtest_results(results: List[BacktestResult], top_n: int):
    """打印回测结果"""
    print("\n" + "=" * 140)
    print(f"📊 冠军组合回测结果 ({len(results)} 个交易日)")
    print("=" * 140)
    
    print(f"\n{'日期':<12} {'选中数':>8} {'次日收益':>12} {'胜率':>8} {'累计收益':>12} {'前5只股票'}")
    print("-" * 140)
    
    for r in results:
        top5_str = ",".join(r.selected_codes[:5])
        print(f"{r.date:<12} {len(r.selected_codes):>8} {r.avg_return:>11.2%} {r.win_rate:>7.1%} "
              f"{r.cumulative_return-1:>11.2%} {top5_str}")
    
    print("=" * 140)
    
    # 统计信息
    if results:
        daily_returns = [r.avg_return for r in results]
        win_rates = [r.win_rate for r in results]
        
        total_return = results[-1].cumulative_return - 1
        annualized_return = (results[-1].cumulative_return ** (252 / len(results))) - 1
        volatility = np.std(daily_returns) * np.sqrt(252)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        max_drawdown = min((r.cumulative_return - 1) for r in results)
        avg_win_rate = np.mean(win_rates)
        
        print("\n📈 回测统计:")
        print(f"  回测天数: {len(results)}")
        print(f"  总收益率: {total_return:.2%}")
        print(f"  年化收益率: {annualized_return:.2%}")
        print(f"  年化波动率: {volatility:.2%}")
        print(f"  夏普比率: {sharpe_ratio:.2f}")
        print(f"  最大回撤: {max_drawdown:.2%}")
        print(f"  平均胜率: {avg_win_rate:.1%}")
        print(f"  日胜率: {sum(1 for r in daily_returns if r > 0) / len(daily_returns):.1%}")
        
        # 收益分布
        print(f"\n📊 收益分布:")
        print(f"  最高日收益: {max(daily_returns):.2%}")
        print(f"  最低日收益: {min(daily_returns):.2%}")
        print(f"  平均日收益: {np.mean(daily_returns):.2%}")
        print(f"  收益中位数: {np.median(daily_returns):.2%}")


def main():
    parser = argparse.ArgumentParser(description='冠军组合回测验证')
    parser.add_argument('--start-date', type=str, default='2025-03-01')
    parser.add_argument('--end-date', type=str, default='2025-04-11')
    parser.add_argument('--top-n', type=int, default=20)
    
    args = parser.parse_args()
    
    print("=" * 140)
    print("📊 冠军组合回测验证")
    print("=" * 140)
    print(f"回测区间: {args.start_date} ~ {args.end_date}")
    print(f"选股数量: {args.top_n}")
    print()
    
    backtester = ChampionBacktester()
    results = backtester.run_backtest(args.start_date, args.end_date, args.top_n)
    
    if not results:
        print("❌ 无回测结果")
        return
    
    print_backtest_results(results, args.top_n)
    
    # 保存结果
    output_data = []
    for r in results:
        output_data.append({
            'date': r.date,
            'selected_count': len(r.selected_codes),
            'avg_return': r.avg_return,
            'win_rate': r.win_rate,
            'cumulative_return': r.cumulative_return - 1,
            'selected_codes': ','.join(r.selected_codes)
        })
    
    df = pl.DataFrame(output_data)
    output_file = f"backtest_champion_{args.start_date}_{args.end_date}.csv"
    df.write_csv(output_file)
    print(f"\n✅ 结果已保存: {output_file}")


if __name__ == "__main__":
    main()
