#!/usr/bin/env python3
"""
完整因子分析流程
1. 加载原始数据
2. 计算所有因子（使用factors目录下的所有因子类）
3. 测试因子有效性
4. 输出分析报告
"""
import sys
from pathlib import Path

# 获取项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import polars as pl
import numpy as np
from typing import List, Dict
import logging

# 导入因子库
from core.factor_library import FactorRegistry

# 导入所有因子模块以触发注册
import factors.technical
import factors.volume_price
import factors.market

from core.factor_analyzer import FactorAnalyzer, FactorMetrics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_raw_data(start_date: str, end_date: str, data_dir: str = None, max_stocks: int = 500) -> pl.DataFrame:
    """加载原始K线数据"""
    # 使用项目根目录作为基准
    if data_dir is None:
        data_path = PROJECT_ROOT / "data" / "kline"
    else:
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
            
            # 统一列类型
            df = df.with_columns([
                pl.col("code").cast(pl.Utf8),
                pl.col("trade_date").cast(pl.Utf8),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ])
            
            # 过滤日期范围
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


def get_all_factors() -> List:
    """获取所有已注册的因子实例"""
    registry = FactorRegistry()
    factor_classes = registry.list_all()
    
    factors_list = []
    for name, factor_class in factor_classes.items():
        try:
            factor = factor_class()
            factors_list.append(factor)
            logger.debug(f"实例化因子: {name}")
        except Exception as e:
            logger.warning(f"实例化因子失败 {name}: {e}")
    
    return factors_list


def calculate_factors(data: pl.DataFrame, factor_instances: List = None) -> pl.DataFrame:
    """计算所有因子"""
    logger.info("计算因子...")
    
    # 如果没有传入因子实例，获取所有已注册的因子
    if factor_instances is None:
        factor_instances = get_all_factors()
    
    logger.info(f"将计算 {len(factor_instances)} 个因子")
    
    # 逐个计算因子
    success_count = 0
    failed_factors = []
    
    for factor in factor_instances:
        try:
            data = factor.calculate(data)
            success_count += 1
        except Exception as e:
            logger.warning(f"计算因子失败 {factor.name}: {e}")
            failed_factors.append(factor.name)
    
    # 添加内置因子（不在因子类中的）
    data = calculate_builtin_factors(data)
    
    # 填充缺失值
    factor_cols = [col for col in data.columns if col.startswith("factor_")]
    for col in factor_cols:
        data = data.with_columns([
            pl.when(pl.col(col).is_nan() | pl.col(col).is_null())
            .then(0)
            .otherwise(pl.col(col))
            .alias(col)
        ])
    
    logger.info(f"因子计算完成: {len(factor_cols)} 个因子 (成功: {success_count}, 失败: {len(failed_factors)})")
    if failed_factors:
        logger.warning(f"失败的因子: {failed_factors}")
    
    return data


def calculate_builtin_factors(data: pl.DataFrame) -> pl.DataFrame:
    """计算内置因子（不在因子类中的简单因子）"""
    logger.info("计算内置因子...")
    
    # 1. MA5乖离率（如果因子类中没有计算）
    if "factor_ma5_bias" not in data.columns:
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=5).over("code").alias("ma5")
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col("ma5")) / pl.col("ma5")).alias("factor_ma5_bias")
        ])
    
    # 2. 10点量比（如果因子类中没有计算）
    if "factor_v_ratio10" not in data.columns:
        data = data.with_columns([
            pl.col("volume").rolling_mean(window_size=5).over("code").alias("volume_ma5")
        ])
        data = data.with_columns([
            (pl.col("volume") / pl.col("volume_ma5")).alias("factor_v_ratio10")
        ])
    
    # 3. 涨跌停得分（市场情绪因子）
    if "factor_limit_up_score" not in data.columns:
        def get_limit_rate(code):
            code_str = str(code)
            if code_str.startswith('300') or code_str.startswith('688'):
                return 0.20
            elif code_str.startswith('8') or code_str.startswith('4'):
                return 0.30
            else:
                return 0.10
        
        data = data.with_columns([
            pl.col("code").map_elements(get_limit_rate, return_dtype=pl.Float64).alias("limit_rate")
        ])
        
        data = data.with_columns([
            pl.col("close").shift(1).over("code").alias("prev_close")
        ])
        
        data = data.with_columns([
            ((pl.col("close") - pl.col("prev_close")) / pl.col("prev_close")).alias("change_pct")
        ])
        
        data = data.with_columns([
            (pl.col("change_pct") >= pl.col("limit_rate")).cast(pl.Int64).alias("is_limit_up"),
            (pl.col("change_pct") <= -pl.col("limit_rate")).cast(pl.Int64).alias("is_limit_down")
        ])
        
        # 按日期计算涨停得分
        daily_stats = data.group_by("trade_date").agg([
            pl.sum("is_limit_up").alias("total_limit_up"),
            pl.sum("is_limit_down").alias("total_limit_down")
        ])
        daily_stats = daily_stats.with_columns([
            (pl.col("total_limit_up") - pl.col("total_limit_down")).alias("factor_limit_up_score")
        ])
        
        data = data.join(
            daily_stats[["trade_date", "factor_limit_up_score"]],
            on="trade_date",
            how="left"
        )
    
    # 4. 筹码峰位（如果因子类中没有计算）
    if "factor_cost_peak" not in data.columns:
        data = data.with_columns([
            pl.col("close").rolling_min(window_size=20).over("code").alias("low_20d"),
            pl.col("close").rolling_max(window_size=20).over("code").alias("high_20d")
        ])
        
        data = data.with_columns([
            ((pl.col("close") - pl.col("low_20d")) / (pl.col("high_20d") - pl.col("low_20d")))
            .fill_nan(0.5)
            .alias("factor_cost_peak")
        ])
    
    return data


def calculate_forward_returns(data: pl.DataFrame, periods: List[int] = [1, 5, 20]) -> pl.DataFrame:
    """计算未来收益率"""
    logger.info("计算未来收益率...")
    
    for period in periods:
        data = data.with_columns([
            pl.col("close").shift(-period).over("code").alias(f"future_close_{period}d")
        ])
        
        data = data.with_columns([
            pl.when(pl.col(f"future_close_{period}d").is_null() | pl.col(f"future_close_{period}d").is_nan())
            .then(None)
            .otherwise((pl.col(f"future_close_{period}d") - pl.col("close")) / pl.col("close"))
            .alias(f"forward_return_{period}d")
        ])
    
    # 过滤掉未来收益率为空的行
    data = data.filter(pl.col("forward_return_1d").is_not_null())
    
    return data


def analyze_factors(data: pl.DataFrame, factor_cols: List[str]) -> pl.DataFrame:
    """分析因子有效性"""
    analyzer = FactorAnalyzer()
    results = []
    
    for factor_col in factor_cols:
        logger.info(f"分析因子: {factor_col}")
        
        try:
            metrics = analyzer.analyze_factor(data, factor_col, "forward_return_1d")
            results.append(metrics.to_dict())
        except Exception as e:
            logger.error(f"分析因子失败 {factor_col}: {e}")
    
    return pl.DataFrame(results)


def print_report(results_df: pl.DataFrame):
    """打印分析报告"""
    print("\n" + "=" * 100)
    print("因子有效性分析报告")
    print("=" * 100)
    
    if len(results_df) == 0:
        print("无测试结果")
        return
    
    # 按IC_IR排序
    results_df = results_df.sort("ic_ir", descending=True)
    
    print(f"\n共测试 {len(results_df)} 个因子\n")
    
    print(f"{'因子名称':<25} {'IC均值':>10} {'IC_IR':>10} {'多空收益':>10} {'夏普':>8} {'p值':>10} {'有效':>6}")
    print("-" * 100)
    
    effective_count = 0
    for row in results_df.to_dicts():
        is_effective = (
            abs(row['ic_ir']) >= 0.3 and
            abs(row['ic_mean']) >= 0.02 and
            row['p_value'] <= 0.05
        )
        if is_effective:
            effective_count += 1
        
        print(f"{row['factor_name']:<25} "
              f"{row['ic_mean']:>10.4f} "
              f"{row['ic_ir']:>10.4f} "
              f"{row['long_short_return']:>10.2%} "
              f"{row['long_short_sharpe']:>8.2f} "
              f"{row['p_value']:>10.4f} "
              f"{'✓' if is_effective else '':>6}")
    
    print("=" * 100)
    print(f"\n有效因子: {effective_count} / {len(results_df)}")
    
    if effective_count > 0:
        print("\n有效因子列表:")
        for row in results_df.to_dicts():
            is_effective = (
                abs(row['ic_ir']) >= 0.3 and
                abs(row['ic_mean']) >= 0.02 and
                row['p_value'] <= 0.05
            )
            if is_effective:
                print(f"  ✓ {row['factor_name']:<25} IC_IR={row['ic_ir']:.4f}")


def main():
    parser = argparse.ArgumentParser(description='因子分析工具')
    parser.add_argument('--start-date', type=str, default='2024-01-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=500,
                       help='最大分析股票数量')
    parser.add_argument('--output', type=str, default='factor_analysis_results.csv')
    parser.add_argument('--builtin-only', action='store_true',
                       help='仅使用内置因子（不使用factors目录下的因子类）')
    
    args = parser.parse_args()
    
    print("=" * 100)
    print("完整因子分析流程")
    print("=" * 100)
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
    if args.builtin_only:
        logger.info("仅使用内置因子")
        data = calculate_builtin_factors(data)
    else:
        # 获取所有已注册的因子
        factor_instances = get_all_factors()
        logger.info(f"从因子库加载了 {len(factor_instances)} 个因子类")
        data = calculate_factors(data, factor_instances)
    
    # 3. 计算未来收益率
    data = calculate_forward_returns(data)
    
    # 4. 识别因子列
    factor_cols = [col for col in data.columns if col.startswith("factor_")]
    logger.info(f"发现 {len(factor_cols)} 个因子: {factor_cols}")
    
    # 5. 分析因子
    results_df = analyze_factors(data, factor_cols)
    
    # 6. 打印报告
    print_report(results_df)
    
    # 7. 保存结果
    if len(results_df) > 0:
        # 使用绝对路径或相对于项目根目录的路径
        output_path = PROJECT_ROOT / args.output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        results_df.write_csv(output_path)
        logger.info(f"结果已保存: {output_path}")


if __name__ == "__main__":
    main()
