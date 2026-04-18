#!/usr/bin/env python3
"""
冠军组合实盘选股测试

使用冠军组合进行实际选股，并输出详细的选股结果

使用方法:
    python scripts/live_stock_pick_champion.py --date 2025-04-11 --top-n 20
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class StockPickResult:
    """选股结果"""
    code: str
    name: str
    score: float
    grade: str
    rank: int
    factors: Dict[str, float]
    close_price: float
    volume: float
    
    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'name': self.name,
            'score': self.score,
            'grade': self.grade,
            'rank': self.rank,
            'close_price': self.close_price,
            'volume': self.volume,
            **{f'factor_{k}': v for k, v in self.factors.items()}
        }


class ChampionStockPicker:
    """冠军组合选股器"""
    
    # 🏆 冠军组合配置
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
            
            # 获取指定日期及之前的数据
            df = df.filter(pl.col("trade_date") <= date)
            
            if len(df) < 30:  # 需要至少30天数据计算指标
                return None
            
            return df.sort("trade_date")
        except Exception as e:
            logger.debug(f"加载 {code} 失败: {e}")
            return None
    
    def apply_filters(self, df: pl.DataFrame) -> bool:
        """应用过滤器，返回是否通过"""
        if len(df) == 0:
            return False
        
        latest = df.tail(1)
        
        # min_volume: 成交量 >= 100万
        volume = latest["volume"][0] if "volume" in latest.columns else 0
        if volume < 1000000:
            return False
        
        # price_range: 3 <= 股价 <= 200
        close = latest["close"][0] if "close" in latest.columns else 0
        if close < 3.0 or close > 200.0:
            return False
        
        # exclude_kcb: 过滤688
        code = latest["code"][0] if "code" in latest.columns else ""
        if str(code).startswith("688"):
            return False
        
        # exclude_bse: 过滤8/4开头
        if str(code).startswith("8") or str(code).startswith("4"):
            return False
        
        # exclude_cyb: 过滤300
        if str(code).startswith("300"):
            return False
        
        return True
    
    def calculate_factors(self, df: pl.DataFrame) -> Dict[str, float]:
        """计算冠军组合因子"""
        factors = {}
        
        if len(df) < 30:
            return factors
        
        close = df["close"].to_numpy()
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()
        volume = df["volume"].to_numpy()
        
        # 1. RSI
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
        
        # 2. MACD
        try:
            ema12 = df["close"].ewm_mean(span=12).tail(1)[0]
            ema26 = df["close"].ewm_mean(span=26).tail(1)[0]
            factors['macd'] = ema12 - ema26
        except:
            factors['macd'] = 0
        
        # 3. 布林带位置
        try:
            ma20 = np.mean(close[-20:])
            std20 = np.std(close[-20:])
            if std20 > 0:
                factors['bollinger'] = (close[-1] - ma20) / (2 * std20)
            else:
                factors['bollinger'] = 0
        except:
            factors['bollinger'] = 0
        
        # 4. ATR
        try:
            tr1 = high[-1] - low[-1]
            tr2 = abs(high[-1] - close[-2])
            tr3 = abs(low[-1] - close[-2])
            tr = max(tr1, tr2, tr3)
            factors['atr'] = tr
        except:
            factors['atr'] = 0
        
        return factors
    
    def calculate_composite_score(self, factors: Dict[str, float]) -> float:
        """计算综合评分"""
        if not factors:
            return 0
        
        score = 0
        total_weight = sum(abs(w) for w in self.CHAMPION_FACTORS.values())
        
        for factor_name, weight in self.CHAMPION_FACTORS.items():
            if factor_name in factors:
                score += factors[factor_name] * (weight / total_weight)
        
        return score
    
    def get_grade(self, score: float) -> str:
        """根据评分确定等级"""
        if score > 1.5:
            return "S"
        elif score > 0.5:
            return "A"
        elif score > 0:
            return "B"
        elif score > -0.5:
            return "C"
        else:
            return "D"
    
    def select_stocks(self, date: str, top_n: int = 20) -> List[StockPickResult]:
        """选股主函数"""
        logger.info(f"开始选股: 日期={date}, 选股数量={top_n}")
        
        if not self.kline_dir.exists():
            logger.error(f"数据目录不存在: {self.kline_dir}")
            return []
        
        results = []
        
        # 遍历所有股票
        for i, parquet_file in enumerate(self.kline_dir.glob("*.parquet")):
            if i % 500 == 0:
                logger.info(f"已处理 {i} 只股票...")
            
            code = parquet_file.stem
            
            # 加载数据
            df = self.load_stock_data(code, date)
            if df is None:
                continue
            
            # 应用过滤器
            if not self.apply_filters(df):
                continue
            
            # 计算因子
            factors = self.calculate_factors(df)
            if not factors:
                continue
            
            # 计算评分
            score = self.calculate_composite_score(factors)
            
            # 获取最新数据
            latest = df.tail(1)
            close_price = latest["close"][0] if "close" in latest.columns else 0
            volume = latest["volume"][0] if "volume" in latest.columns else 0
            
            result = StockPickResult(
                code=code,
                name=code,  # 简化处理
                score=score,
                grade=self.get_grade(score),
                rank=0,  # 稍后排序后设置
                factors=factors,
                close_price=close_price,
                volume=volume
            )
            results.append(result)
        
        # 排序
        results.sort(key=lambda x: x.score, reverse=True)
        
        # 设置排名
        for i, r in enumerate(results, 1):
            r.rank = i
        
        logger.info(f"选股完成: 共筛选 {len(results)} 只股票，返回前 {top_n} 只")
        
        return results[:top_n]
    
    def save_results(self, results: List[StockPickResult], date: str, output_dir: str = None):
        """保存选股结果"""
        if output_dir is None:
            output_dir = self.data_dir / "champion_picks"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 转换为DataFrame
        data = [r.to_dict() for r in results]
        df = pl.DataFrame(data)
        
        # 保存CSV
        csv_file = output_dir / f"champion_picks_{date}.csv"
        df.write_csv(csv_file)
        
        # 保存Parquet
        parquet_file = output_dir / f"champion_picks_{date}.parquet"
        df.write_parquet(parquet_file)
        
        logger.info(f"选股结果已保存: {csv_file}")
        
        return csv_file


def print_results(results: List[StockPickResult], date: str):
    """打印选股结果"""
    print("\n" + "=" * 120)
    print(f"🏆 冠军组合实盘选股结果 | 日期: {date}")
    print("=" * 120)
    print(f"\n{'排名':<4} {'代码':<10} {'评分':>10} {'等级':<4} {'收盘价':>10} {'成交量':>15} {'RSI':>8} {'MACD':>10} {'布林带':>10} {'ATR':>10}")
    print("-" * 120)
    
    for r in results:
        rsi = r.factors.get('rsi', 0)
        macd = r.factors.get('macd', 0)
        bollinger = r.factors.get('bollinger', 0)
        atr = r.factors.get('atr', 0)
        
        print(f"{r.rank:<4} {r.code:<10} {r.score:>10.4f} {r.grade:<4} "
              f"{r.close_price:>10.2f} {r.volume:>15,.0f} "
              f"{rsi:>8.2f} {macd:>10.4f} {bollinger:>10.4f} {atr:>10.4f}")
    
    print("=" * 120)
    
    # 统计信息
    if results:
        scores = [r.score for r in results]
        print(f"\n📊 统计信息:")
        print(f"  选中股票数: {len(results)}")
        print(f"  平均评分: {np.mean(scores):.4f}")
        print(f"  最高评分: {max(scores):.4f}")
        print(f"  最低评分: {min(scores):.4f}")
        print(f"  S级股票: {sum(1 for r in results if r.grade == 'S')} 只")
        print(f"  A级股票: {sum(1 for r in results if r.grade == 'A')} 只")
        print(f"  B级股票: {sum(1 for r in results if r.grade == 'B')} 只")


def main():
    parser = argparse.ArgumentParser(description='冠军组合实盘选股测试')
    parser.add_argument('--date', type=str, required=True, help='选股日期 (YYYY-MM-DD)')
    parser.add_argument('--top-n', type=int, default=20, help='选股数量')
    parser.add_argument('--output', type=str, default=None, help='输出目录')
    
    args = parser.parse_args()
    
    print("=" * 120)
    print("🏆 冠军组合实盘选股测试")
    print("=" * 120)
    print(f"选股日期: {args.date}")
    print(f"选股数量: {args.top_n}")
    print()
    
    # 显示冠军组合配置
    print("📋 冠军组合配置:")
    print("  过滤器:", ", ".join(ChampionStockPicker.CHAMPION_FILTERS))
    print("  因子权重:")
    for factor, weight in ChampionStockPicker.CHAMPION_FACTORS.items():
        print(f"    - {factor}: {weight:.0%}")
    print()
    
    # 选股
    picker = ChampionStockPicker()
    results = picker.select_stocks(args.date, args.top_n)
    
    if not results:
        print("❌ 未选出任何股票")
        return
    
    # 打印结果
    print_results(results, args.date)
    
    # 保存结果
    output_file = picker.save_results(results, args.date, args.output)
    print(f"\n✅ 结果已保存: {output_file}")


if __name__ == "__main__":
    main()
