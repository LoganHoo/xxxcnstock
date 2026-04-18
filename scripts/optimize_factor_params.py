#!/usr/bin/env python3
"""
因子参数优化脚本

功能:
1. 对指定因子进行参数网格搜索
2. 测试不同参数组合的有效性
3. 输出最优参数配置

使用方法:
    # 优化单个因子
    python scripts/optimize_factor_params.py --factor ma5_bias --start-date 2024-06-01 --end-date 2025-04-13
    
    # 优化所有可优化因子
    python scripts/optimize_factor_params.py --all --start-date 2024-06-01 --end-date 2025-04-13
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime

from core.factor_analyzer import FactorAnalyzer
from core.factor_filter_config import get_factor_filter_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ParamOptimizationResult:
    """参数优化结果"""
    factor_name: str
    params: Dict[str, Any]
    ic_mean: float
    ic_ir: float
    ic_positive_ratio: float
    long_short_return: float
    long_short_sharpe: float
    max_drawdown: float
    
    def to_dict(self) -> Dict:
        return {
            'factor_name': self.factor_name,
            'params': str(self.params),
            'ic_mean': self.ic_mean,
            'ic_ir': self.ic_ir,
            'ic_positive_ratio': self.ic_positive_ratio,
            'long_short_return': self.long_short_return,
            'long_short_sharpe': self.long_short_sharpe,
            'max_drawdown': self.max_drawdown,
        }


def load_raw_data(start_date: str, end_date: str, data_dir: str = "data", max_stocks: int = 300) -> pl.DataFrame:
    """加载原始K线数据"""
    data_path = Path(data_dir) / "kline"
    
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
    
    logger.info(f"加载了 {len(all_data)} 只股票的数据")
    return pl.concat(all_data)


def calculate_forward_returns(data: pl.DataFrame) -> pl.DataFrame:
    """计算未来收益率"""
    data = data.with_columns([
        pl.col("close").shift(-1).over("code").alias("future_close_1d")
    ])
    
    data = data.with_columns([
        pl.when(pl.col("future_close_1d").is_null() | pl.col("future_close_1d").is_nan())
        .then(None)
        .otherwise((pl.col("future_close_1d") - pl.col("close")) / pl.col("close"))
        .alias("forward_return_1d")
    ])
    
    data = data.filter(pl.col("forward_return_1d").is_not_null())
    
    return data


def calculate_ma5_bias(data: pl.DataFrame, window: int = 5) -> pl.DataFrame:
    """计算MA乖离率"""
    ma_col = f"ma{window}"
    data = data.with_columns([
        pl.col("close").rolling_mean(window_size=window).over("code").alias(ma_col)
    ])
    data = data.with_columns([
        ((pl.col("close") - pl.col(ma_col)) / pl.col(ma_col)).alias(f"factor_ma{window}_bias")
    ])
    return data


def calculate_ma5_slope(data: pl.DataFrame, window: int = 5) -> pl.DataFrame:
    """计算MA斜率"""
    data = data.with_columns([
        (pl.col("close") - pl.col("close").shift(window-1).over("code")).alias(f"factor_ma{window}_slope")
    ])
    return data


def calculate_v_ratio(data: pl.DataFrame, period: int = 5) -> pl.DataFrame:
    """计算量比"""
    data = data.with_columns([
        pl.col("volume").rolling_mean(window_size=period).over("code").alias(f"volume_ma{period}")
    ])
    data = data.with_columns([
        (pl.col("volume") / pl.col(f"volume_ma{period}")).alias(f"factor_v_ratio{period}")
    ])
    return data


def calculate_cost_peak(data: pl.DataFrame, window: int = 20, bins: int = 50) -> pl.DataFrame:
    """计算筹码峰位"""
    data = data.with_columns([
        pl.col("close").rolling_min(window_size=window).over("code").alias(f"low_{window}d"),
        pl.col("close").rolling_max(window_size=window).over("code").alias(f"high_{window}d")
    ])
    
    data = data.with_columns([
        ((pl.col("close") - pl.col(f"low_{window}d")) / 
         (pl.col(f"high_{window}d") - pl.col(f"low_{window}d")))
        .fill_nan(0.5)
        .alias(f"factor_cost_peak_w{window}")
    ])
    return data


def optimize_factor_params(
    factor_name: str,
    data: pl.DataFrame,
    param_combinations: List[Dict[str, Any]]
) -> List[ParamOptimizationResult]:
    """
    优化因子参数
    
    Args:
        factor_name: 因子名称
        data: 市场数据
        param_combinations: 参数组合列表
    
    Returns:
        优化结果列表
    """
    results = []
    analyzer = FactorAnalyzer()
    
    total = len(param_combinations)
    logger.info(f"开始优化 {factor_name}: 共 {total} 种参数组合")
    
    for i, params in enumerate(param_combinations):
        if i % 10 == 0:
            logger.info(f"进度: {i}/{total}")
        
        try:
            # 根据因子类型计算因子值
            if factor_name == 'ma5_bias':
                window = params.get('window', 5)
                data_with_factor = calculate_ma5_bias(data, window)
                factor_col = f"factor_ma{window}_bias"
            elif factor_name == 'ma5_slope':
                window = params.get('window', 5)
                data_with_factor = calculate_ma5_slope(data, window)
                factor_col = f"factor_ma{window}_slope"
            elif factor_name == 'v_ratio10':
                period = params.get('period', 5)
                data_with_factor = calculate_v_ratio(data, period)
                factor_col = f"factor_v_ratio{period}"
            elif factor_name == 'cost_peak':
                window = params.get('window', 20)
                bins = params.get('bins', 50)
                data_with_factor = calculate_cost_peak(data, window, bins)
                factor_col = f"factor_cost_peak_w{window}"
            else:
                logger.warning(f"未知因子: {factor_name}")
                continue
            
            # 过滤NaN
            data_with_factor = data_with_factor.filter(
                pl.col(factor_col).is_not_nan() & 
                pl.col(factor_col).is_not_null()
            )
            
            if len(data_with_factor) < 100:
                continue
            
            # 分析因子有效性
            metrics = analyzer.analyze_factor(data_with_factor, factor_col, "forward_return_1d")
            
            result = ParamOptimizationResult(
                factor_name=factor_name,
                params=params,
                ic_mean=metrics.ic_mean,
                ic_ir=metrics.ic_ir,
                ic_positive_ratio=metrics.ic_positive_ratio,
                long_short_return=metrics.long_short_return,
                long_short_sharpe=metrics.long_short_sharpe,
                max_drawdown=metrics.max_drawdown
            )
            results.append(result)
            
        except Exception as e:
            logger.debug(f"参数组合失败 {params}: {e}")
            continue
    
    return results


def print_optimization_results(results: List[ParamOptimizationResult], top_n: int = 5):
    """打印优化结果"""
    if not results:
        print("无优化结果")
        return
    
    print("\n" + "=" * 120)
    print(f"参数优化结果 (Top {top_n})")
    print("=" * 120)
    
    # 按IC_IR排序
    sorted_results = sorted(results, key=lambda x: abs(x.ic_ir), reverse=True)
    
    print(f"\n{'排名':<4} {'参数':<30} {'IC均值':>10} {'IC_IR':>10} {'IC正比':>8} {'多空收益':>10} {'夏普':>8} {'最大回撤':>10}")
    print("-" * 120)
    
    for i, result in enumerate(sorted_results[:top_n], 1):
        param_str = ", ".join([f"{k}={v}" for k, v in result.params.items()])
        print(f"{i:<4} {param_str:<30} {result.ic_mean:>10.4f} {result.ic_ir:>10.4f} "
              f"{result.ic_positive_ratio:>8.2%} {result.long_short_return:>10.2%} "
              f"{result.long_short_sharpe:>8.2f} {result.max_drawdown:>10.2%}")
    
    print("=" * 120)
    
    # 打印最优参数
    best = sorted_results[0]
    print(f"\n【最优参数】")
    print(f"  因子: {best.factor_name}")
    print(f"  参数: {best.params}")
    print(f"  IC均值: {best.ic_mean:.4f}")
    print(f"  IC_IR: {best.ic_ir:.4f}")
    print(f"  多空收益: {best.long_short_return:.2%}")
    print(f"  夏普比率: {best.long_short_sharpe:.2f}")


def main():
    parser = argparse.ArgumentParser(description='因子参数优化脚本')
    parser.add_argument('--factor', type=str, help='要优化的因子名称')
    parser.add_argument('--all', action='store_true', help='优化所有可优化因子')
    parser.add_argument('--start-date', type=str, default='2024-06-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=200)
    parser.add_argument('--output', type=str, default='factor_optimization_results.csv')
    
    args = parser.parse_args()
    
    if not args.factor and not args.all:
        parser.print_help()
        return
    
    print("=" * 120)
    print("因子参数优化")
    print("=" * 120)
    print(f"数据区间: {args.start_date} ~ {args.end_date}")
    print()
    
    # 加载配置
    config = get_factor_filter_config()
    
    # 加载数据
    data = load_raw_data(args.start_date, args.end_date, max_stocks=args.max_stocks)
    if len(data) == 0:
        logger.error("未加载到数据")
        return
    
    # 计算未来收益率
    data = calculate_forward_returns(data)
    
    # 确定要优化的因子
    if args.all:
        # 获取所有可优化的因子
        all_factors = config.get_all_factors()
        factors_to_optimize = [
            name for name, f in all_factors.items() 
            if f.optimization_range
        ]
    else:
        factors_to_optimize = [args.factor]
    
    logger.info(f"将优化 {len(factors_to_optimize)} 个因子: {factors_to_optimize}")
    
    all_results = []
    
    for factor_name in factors_to_optimize:
        # 获取参数优化范围
        opt_range = config.get_factor_optimization_params(factor_name)
        
        if not opt_range:
            logger.warning(f"因子 {factor_name} 无优化参数范围")
            continue
        
        # 生成参数组合
        from itertools import product
        param_names = list(opt_range.keys())
        param_values = [opt_range[name] for name in param_names]
        param_combinations = [dict(zip(param_names, values)) for values in product(*param_values)]
        
        logger.info(f"{factor_name}: {len(param_combinations)} 种参数组合")
        
        # 优化
        results = optimize_factor_params(factor_name, data, param_combinations)
        all_results.extend(results)
        
        # 打印结果
        print_optimization_results(results)
    
    # 保存结果
    if all_results:
        results_df = pl.DataFrame([r.to_dict() for r in all_results])
        output_path = Path(args.output)
        results_df.write_csv(output_path)
        logger.info(f"结果已保存: {output_path}")
    
    print(f"\n共优化 {len(factors_to_optimize)} 个因子，生成 {len(all_results)} 个结果")


if __name__ == "__main__":
    main()
