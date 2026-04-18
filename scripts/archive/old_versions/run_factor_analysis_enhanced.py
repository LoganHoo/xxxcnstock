#!/usr/bin/env python3
"""
增强版因子分析流程
1. 多周期收益测试
2. 分层测试（按市值/行业）
3. 因子组合分析
4. 滚动窗口稳定性测试
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import polars as pl
import numpy as np
from typing import List, Dict, Tuple
import logging

from core.factor_library import FactorRegistry
import factors.technical
import factors.volume_price
import factors.market

from core.factor_analyzer import FactorAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_raw_data(start_date: str, end_date: str, max_stocks: int = 500) -> pl.DataFrame:
    """加载原始K线数据"""
    data_path = PROJECT_ROOT / "data" / "kline"
    
    if not data_path.exists():
        logger.error(f"数据目录不存在: {data_path}")
        return pl.DataFrame()
    
    all_data = []
    count = 0
    
    for parquet_file in data_path.glob("*.parquet"):
        if count >= max_stocks:
            break
            
        try:
            df = pl.read_parquet(parquet_file)
            df = df.with_columns([
                pl.col("code").cast(pl.Utf8),
                pl.col("trade_date").cast(pl.Utf8),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ])
            
            df = df.filter(
                (pl.col("trade_date") >= start_date) &
                (pl.col("trade_date") <= end_date)
            )
            
            if len(df) > 0:
                all_data.append(df)
                count += 1
        except Exception as e:
            logger.debug(f"加载文件失败 {parquet_file}: {e}")
    
    if not all_data:
        return pl.DataFrame()
    
    return pl.concat(all_data)


def calculate_all_factors(data: pl.DataFrame) -> pl.DataFrame:
    """计算所有因子"""
    registry = FactorRegistry()
    factor_classes = registry.list_all()
    
    logger.info(f"计算 {len(factor_classes)} 个因子...")
    
    for name, factor_class in factor_classes.items():
        try:
            factor = factor_class()
            data = factor.calculate(data)
        except Exception as e:
            logger.warning(f"计算因子失败 {name}: {e}")
    
    # 填充缺失值
    factor_cols = [col for col in data.columns if col.startswith("factor_")]
    for col in factor_cols:
        data = data.with_columns([
            pl.when(pl.col(col).is_nan() | pl.col(col).is_null())
            .then(0)
            .otherwise(pl.col(col))
            .alias(col)
        ])
    
    return data


def calculate_forward_returns(data: pl.DataFrame, periods: List[int] = [1, 5, 10, 20]) -> pl.DataFrame:
    """计算多周期未来收益率"""
    for period in periods:
        data = data.with_columns([
            pl.col("close").shift(-period).over("code").alias(f"future_close_{period}d")
        ])
        data = data.with_columns([
            pl.when(pl.col(f"future_close_{period}d").is_null())
            .then(None)
            .otherwise((pl.col(f"future_close_{period}d") - pl.col("close")) / pl.col("close"))
            .alias(f"forward_return_{period}d")
        ])
    return data


def analyze_factor_multi_period(
    data: pl.DataFrame, 
    factor_col: str, 
    periods: List[int] = [1, 5, 10, 20]
) -> Dict:
    """多周期因子分析"""
    analyzer = FactorAnalyzer()
    results = {}
    
    for period in periods:
        return_col = f"forward_return_{period}d"
        if return_col not in data.columns:
            continue
            
        # 过滤有效数据
        valid_data = data.filter(pl.col(return_col).is_not_null())
        if len(valid_data) == 0:
            continue
            
        try:
            metrics = analyzer.analyze_factor(valid_data, factor_col, return_col)
            results[f"{period}d"] = {
                'ic_mean': metrics.ic_mean,
                'ic_ir': metrics.ic_ir,
                'long_short_return': metrics.long_short_return,
                'p_value': metrics.p_value
            }
        except Exception as e:
            logger.warning(f"分析失败 {factor_col} {period}d: {e}")
    
    return results


def analyze_by_market_cap(data: pl.DataFrame, factor_col: str) -> Dict:
    """按市值分层分析"""
    analyzer = FactorAnalyzer()
    
    # 计算市值（用收盘价*成交量近似）
    data = data.with_columns([
        (pl.col("close") * pl.col("volume")).alias("market_cap_proxy")
    ])
    
    # 按日期分组，计算市值分位数
    results = {}
    for date, group in data.group_by("trade_date"):
        cap_values = group["market_cap_proxy"].to_numpy()
        if len(cap_values) < 30:
            continue
            
        median_cap = np.median(cap_values)
        
        large_cap = group.filter(pl.col("market_cap_proxy") >= median_cap)
        small_cap = group.filter(pl.col("market_cap_proxy") < median_cap)
        
        if len(large_cap) > 10:
            try:
                metrics = analyzer.analyze_factor(large_cap, factor_col, "forward_return_1d")
                results.setdefault('large_cap', []).append(metrics.ic_ir)
            except:
                pass
                
        if len(small_cap) > 10:
            try:
                metrics = analyzer.analyze_factor(small_cap, factor_col, "forward_return_1d")
                results.setdefault('small_cap', []).append(metrics.ic_ir)
            except:
                pass
    
    # 计算平均IC_IR
    return {
        'large_cap_ir': np.mean(results.get('large_cap', [0])),
        'small_cap_ir': np.mean(results.get('small_cap', [0]))
    }


def rolling_factor_analysis(
    data: pl.DataFrame, 
    factor_col: str, 
    window: int = 60
) -> Dict:
    """滚动窗口稳定性分析"""
    analyzer = FactorAnalyzer()
    
    dates = sorted(data["trade_date"].unique().to_list())
    ic_series = []
    
    for i in range(window, len(dates)):
        window_dates = dates[i-window:i]
        window_data = data.filter(pl.col("trade_date").is_in(window_dates))
        
        if len(window_data) < 100:
            continue
            
        try:
            ic_df = analyzer.calculate_ic(window_data, factor_col, "forward_return_1d")
            ic_values = ic_df['ic'].drop_nulls().to_numpy()
            if len(ic_values) > 0:
                ic_series.append(np.mean(ic_values))
        except:
            pass
    
    if not ic_series:
        return {'stability': 0, 'consistency': 0}
    
    ic_series = np.array(ic_series)
    
    return {
        'stability': np.std(ic_series),  # 标准差越小越稳定
        'consistency': np.sum(ic_series > 0) / len(ic_series),  # IC为正的比例
        'avg_ic_ir': np.mean(ic_series) / np.std(ic_series) if np.std(ic_series) > 0 else 0
    }


def print_enhanced_report(results: Dict):
    """打印增强版报告"""
    print("\n" + "=" * 120)
    print("增强版因子有效性分析报告")
    print("=" * 120)
    
    # 1. 多周期表现
    print("\n【多周期表现】")
    print(f"{'因子名称':<25} {'1天IC/IR':>12} {'5天IC/IR':>12} {'10天IC/IR':>12} {'20天IC/IR':>12}")
    print("-" * 120)
    
    for factor_name, period_results in sorted(results.items()):
        if factor_name.startswith('_'):
            continue
            
        line = f"{factor_name:<25}"
        for period in ['1d', '5d', '10d', '20d']:
            if period in period_results:
                r = period_results[period]
                line += f" {r['ic_mean']:>5.3f}/{r['ic_ir']:>4.2f}"
            else:
                line += "     -/-   "
        print(line)
    
    # 2. 分层表现
    print("\n【分层表现（按市值）】")
    print(f"{'因子名称':<25} {'大盘股IC_IR':>12} {'小盘股IC_IR':>12} {'差异':>10}")
    print("-" * 120)
    
    for factor_name, period_results in sorted(results.items()):
        if factor_name.startswith('_') or 'large_cap_ir' not in period_results:
            continue
            
        large = period_results.get('large_cap_ir', 0)
        small = period_results.get('small_cap_ir', 0)
        diff = small - large
        
        print(f"{factor_name:<25} {large:>12.4f} {small:>12.4f} {diff:>10.4f}")
    
    # 3. 稳定性分析
    print("\n【稳定性分析】")
    print(f"{'因子名称':<25} {'稳定性(σ)':>12} {'一致性(%)':>12} {'滚动IC_IR':>12}")
    print("-" * 120)
    
    for factor_name, period_results in sorted(results.items()):
        if factor_name.startswith('_') or 'stability' not in period_results:
            continue
            
        stab = period_results.get('stability', 0)
        cons = period_results.get('consistency', 0) * 100
        ir = period_results.get('avg_ic_ir', 0)
        
        print(f"{factor_name:<25} {stab:>12.4f} {cons:>11.1f}% {ir:>12.4f}")
    
    print("=" * 120)


def main():
    parser = argparse.ArgumentParser(description='增强版因子分析工具')
    parser.add_argument('--start-date', type=str, default='2024-01-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=200)
    parser.add_argument('--factors', type=str, nargs='+', 
                       help='指定分析的因子列表，默认分析所有')
    
    args = parser.parse_args()
    
    print("=" * 120)
    print("增强版因子分析流程")
    print("=" * 120)
    print(f"数据区间: {args.start_date} ~ {args.end_date}")
    print(f"最大股票数: {args.max_stocks}")
    print()
    
    # 1. 加载数据
    data = load_raw_data(args.start_date, args.end_date, max_stocks=args.max_stocks)
    if len(data) == 0:
        logger.error("未加载到数据")
        return
    
    logger.info(f"总数据量: {len(data)} 条")
    
    # 2. 计算因子
    data = calculate_all_factors(data)
    
    # 3. 计算多周期未来收益
    data = calculate_forward_returns(data, periods=[1, 5, 10, 20])
    
    # 4. 选择要分析的因子
    if args.factors:
        factor_cols = [f"factor_{f}" for f in args.factors]
    else:
        factor_cols = [col for col in data.columns if col.startswith("factor_")]
    
    logger.info(f"分析 {len(factor_cols)} 个因子")
    
    # 5. 综合分析
    results = {}
    
    for factor_col in factor_cols[:10]:  # 限制前10个因子以加快测试
        logger.info(f"分析因子: {factor_col}")
        
        # A. 多周期分析
        multi_period = analyze_factor_multi_period(data, factor_col)
        results[factor_col] = multi_period
        
        # B. 分层分析
        tier_result = analyze_by_market_cap(data, factor_col)
        results[factor_col].update(tier_result)
        
        # C. 稳定性分析
        stability = rolling_factor_analysis(data, factor_col)
        results[factor_col].update(stability)
    
    # 6. 打印报告
    print_enhanced_report(results)


if __name__ == "__main__":
    main()
