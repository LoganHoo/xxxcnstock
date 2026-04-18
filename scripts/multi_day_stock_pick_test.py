#!/usr/bin/env python3
"""
多日选股测试

测试冠军组合在多个交易日的选股稳定性

使用方法:
    python scripts/multi_day_stock_pick_test.py --start-date 2025-04-01 --end-date 2025-04-11 --top-n 20
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class DailyPickResult:
    """每日选股结果"""
    date: str
    total_stocks: int
    selected_count: int
    top_score: float
    avg_score: float
    top_codes: List[str]


class MultiDayStockPicker:
    """多日选股器"""
    
    CHAMPION_FILTERS = [
        'min_volume',
        'price_range',
        'exclude_kcb',
        'exclude_bse',
        'exclude_cyb'
    ]
    
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
        except Exception as e:
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
    
    def select_stocks_for_date(self, date: str, top_n: int = 20) -> DailyPickResult:
        """为指定日期选股"""
        logger.info(f"选股日期: {date}")
        
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
        
        # 排序
        results.sort(key=lambda x: x[1], reverse=True)
        
        return DailyPickResult(
            date=date,
            total_stocks=len(list(self.kline_dir.glob("*.parquet"))),
            selected_count=len(results),
            top_score=results[0][1] if results else 0,
            avg_score=np.mean([r[1] for r in results[:top_n]]) if results else 0,
            top_codes=[r[0] for r in results[:top_n]]
        )
    
    def run_multi_day_test(self, start_date: str, end_date: str, top_n: int = 20) -> List[DailyPickResult]:
        """运行多日测试"""
        # 筛选日期范围内的交易日
        test_dates = [d for d in self.all_dates if start_date <= d <= end_date]
        
        logger.info(f"测试区间: {start_date} ~ {end_date}")
        logger.info(f"交易日数量: {len(test_dates)}")
        
        results = []
        for date in test_dates:
            result = self.select_stocks_for_date(date, top_n)
            results.append(result)
        
        return results


def print_multi_day_results(results: List[DailyPickResult], top_n: int):
    """打印多日测试结果"""
    print("\n" + "=" * 120)
    print(f"📅 多日选股测试结果 ({len(results)} 个交易日)")
    print("=" * 120)
    
    print(f"\n{'日期':<12} {'总股票':>8} {'通过过滤':>10} {'Top1评分':>12} {'Top{top_n}平均':>12} {'前5只股票'}")
    print("-" * 120)
    
    for r in results:
        top5_str = ",".join(r.top_codes[:5])
        print(f"{r.date:<12} {r.total_stocks:>8} {r.selected_count:>10} {r.top_score:>12.4f} "
              f"{r.avg_score:>12.4f} {top5_str}")
    
    print("=" * 120)
    
    # 统计信息
    if results:
        selected_counts = [r.selected_count for r in results]
        top_scores = [r.top_score for r in results]
        avg_scores = [r.avg_score for r in results]
        
        print("\n📊 统计汇总:")
        print(f"  测试天数: {len(results)}")
        print(f"  平均通过过滤股票数: {np.mean(selected_counts):.0f} (±{np.std(selected_counts):.0f})")
        print(f"  平均Top1评分: {np.mean(top_scores):.4f} (±{np.std(top_scores):.4f})")
        print(f"  平均Top{top_n}评分: {np.mean(avg_scores):.4f} (±{np.std(avg_scores):.4f})")
        
        # 稳定性分析
        print(f"\n📈 稳定性分析:")
        print(f"  Top1评分波动率: {np.std(top_scores)/np.mean(top_scores)*100:.2f}%")
        print(f"  通过过滤股票数波动率: {np.std(selected_counts)/np.mean(selected_counts)*100:.2f}%")
        
        # 重复股票分析
        all_top_codes = []
        for r in results:
            all_top_codes.extend(r.top_codes)
        
        from collections import Counter
        code_counts = Counter(all_top_codes)
        most_common = code_counts.most_common(10)
        
        print(f"\n🔄 重复出现股票 (Top 10):")
        for code, count in most_common:
            print(f"  {code}: 出现 {count} 次 ({count/len(results)*100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='多日选股测试')
    parser.add_argument('--start-date', type=str, default='2025-04-01')
    parser.add_argument('--end-date', type=str, default='2025-04-11')
    parser.add_argument('--top-n', type=int, default=20)
    
    args = parser.parse_args()
    
    print("=" * 120)
    print("📅 多日选股测试")
    print("=" * 120)
    print(f"测试区间: {args.start_date} ~ {args.end_date}")
    print(f"选股数量: {args.top_n}")
    print()
    
    picker = MultiDayStockPicker()
    results = picker.run_multi_day_test(args.start_date, args.end_date, args.top_n)
    
    if not results:
        print("❌ 无测试结果")
        return
    
    print_multi_day_results(results, args.top_n)
    
    # 保存结果
    output_data = []
    for r in results:
        output_data.append({
            'date': r.date,
            'total_stocks': r.total_stocks,
            'selected_count': r.selected_count,
            'top_score': r.top_score,
            'avg_score': r.avg_score,
            'top_codes': ','.join(r.top_codes)
        })
    
    df = pl.DataFrame(output_data)
    output_file = f"multi_day_test_{args.start_date}_{args.end_date}.csv"
    df.write_csv(output_file)
    print(f"\n✅ 结果已保存: {output_file}")


if __name__ == "__main__":
    main()
