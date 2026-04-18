#!/usr/bin/env python3
"""
冠军组合搜索器

目标: 找到最优的过滤器+因子组合，最大化选股效果

优化目标:
1. 最大化IC_IR
2. 最大化多空收益
3. 最大化夏普比率
4. 最小化最大回撤

使用方法:
    python scripts/find_champion_combo.py --start-date 2024-06-01 --end-date 2025-04-13
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from itertools import combinations, product
import logging
from datetime import datetime

from core.factor_analyzer import FactorAnalyzer
from core.factor_filter_config import get_factor_filter_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ChampionResult:
    """冠军组合结果"""
    combo_id: int
    filter_combo: List[str]
    factor_combo: Dict[str, float]
    ic_mean: float
    ic_ir: float
    ic_positive_ratio: float
    long_short_return: float
    long_short_sharpe: float
    max_drawdown: float
    win_rate: float
    total_score: float  # 综合评分
    
    def to_dict(self) -> Dict:
        return {
            'combo_id': self.combo_id,
            'filter_combo': ','.join(self.filter_combo),
            'factor_combo': str(self.factor_combo),
            'ic_mean': self.ic_mean,
            'ic_ir': self.ic_ir,
            'ic_positive_ratio': self.ic_positive_ratio,
            'long_short_return': self.long_short_return,
            'long_short_sharpe': self.long_short_sharpe,
            'max_drawdown': self.max_drawdown,
            'win_rate': self.win_rate,
            'total_score': self.total_score,
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


def apply_filters(data: pl.DataFrame, filter_combo: List[str], config) -> pl.DataFrame:
    """应用过滤器组合"""
    for filter_id in filter_combo:
        filter_config = config.get_filter(filter_id)
        if not filter_config:
            continue
        
        params = filter_config.params
        
        try:
            if filter_id == 'min_volume':
                min_vol = params.get('min_volume', 1000000)
                data = data.filter(pl.col("volume") >= min_vol)
            elif filter_id == 'price_range':
                min_p = params.get('min_price', 3.0)
                max_p = params.get('max_price', 200.0)
                data = data.filter((pl.col("close") >= min_p) & (pl.col("close") <= max_p))
            elif filter_id == 'exclude_kcb':
                data = data.filter(~pl.col("code").str.starts_with("688"))
            elif filter_id == 'exclude_bse':
                data = data.filter(~pl.col("code").str.starts_with("8"))
                data = data.filter(~pl.col("code").str.starts_with("4"))
            elif filter_id == 'exclude_cyb':
                data = data.filter(~pl.col("code").str.starts_with("300"))
            elif filter_id == 'turnover_rate':
                min_t = params.get('min_turnover', 0.005)
                max_t = params.get('max_turnover', 0.30)
                # 简化处理
            elif filter_id == 'float_cap':
                min_cap = params.get('min_float_cap', 1000000000)
                max_cap = params.get('max_float_cap', 500000000000)
                # 需要市值数据
        except Exception as e:
            logger.debug(f"应用过滤器 {filter_id} 失败: {e}")
    
    return data


def calculate_factors(data: pl.DataFrame, factor_weights: Dict[str, float]) -> pl.DataFrame:
    """计算因子值"""
    # 1. MA5乖离率
    if 'ma5_bias' in factor_weights:
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=5).over("code").alias("ma5")
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col("ma5")) / pl.col("ma5")).alias("factor_ma5_bias")
        ])
    
    # 2. MA5斜率
    if 'ma5_slope' in factor_weights:
        data = data.with_columns([
            (pl.col("close") - pl.col("close").shift(4).over("code")).alias("factor_ma5_slope")
        ])
    
    # 3. 量比
    if 'v_ratio10' in factor_weights:
        data = data.with_columns([
            pl.col("volume").rolling_mean(window_size=5).over("code").alias("volume_ma5")
        ])
        data = data.with_columns([
            (pl.col("volume") / pl.col("volume_ma5")).alias("factor_v_ratio10")
        ])
    
    # 4. 筹码峰位
    if 'cost_peak' in factor_weights:
        data = data.with_columns([
            pl.col("close").rolling_min(window_size=20).over("code").alias("low_20d"),
            pl.col("close").rolling_max(window_size=20).over("code").alias("high_20d")
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col("low_20d")) / (pl.col("high_20d") - pl.col("low_20d")))
            .fill_nan(0.5)
            .alias("factor_cost_peak")
        ])
    
    # 5. RSI
    if 'rsi' in factor_weights:
        data = data.with_columns([
            pl.col("close").diff().over("code").alias("price_diff")
        ])
        data = data.with_columns([
            pl.when(pl.col("price_diff") > 0).then(pl.col("price_diff")).otherwise(0).alias("gain"),
            pl.when(pl.col("price_diff") < 0).then(-pl.col("price_diff")).otherwise(0).alias("loss")
        ])
        data = data.with_columns([
            pl.col("gain").rolling_mean(window_size=14).over("code").alias("avg_gain"),
            pl.col("loss").rolling_mean(window_size=14).over("code").alias("avg_loss")
        ])
        data = data.with_columns([
            (100 - (100 / (1 + pl.col("avg_gain") / pl.col("avg_loss")))).alias("factor_rsi")
        ])
    
    # 6. MACD
    if 'macd' in factor_weights:
        data = data.with_columns([
            pl.col("close").ewm_mean(span=12).over("code").alias("ema12"),
            pl.col("close").ewm_mean(span=26).over("code").alias("ema26")
        ])
        data = data.with_columns([
            (pl.col("ema12") - pl.col("ema26")).alias("factor_macd")
        ])
    
    # 7. 布林带
    if 'bollinger' in factor_weights:
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=20).over("code").alias("bb_ma20"),
            pl.col("close").rolling_std(window_size=20).over("code").alias("bb_std20")
        ])
        data = data.with_columns([
            pl.when(pl.col("bb_std20") > 0)
            .then((pl.col("close") - pl.col("bb_ma20")) / (pl.col("bb_std20") * 2))
            .otherwise(0)
            .alias("factor_bollinger")
        ])
    
    # 8. ATR
    if 'atr' in factor_weights:
        data = data.with_columns([
            (pl.col("high") - pl.col("low")).alias("tr1"),
            (pl.col("high") - pl.col("close").shift(1).over("code")).abs().alias("tr2"),
            (pl.col("low") - pl.col("close").shift(1).over("code")).abs().alias("tr3")
        ])
        data = data.with_columns([
            pl.max_horizontal(["tr1", "tr2", "tr3"]).alias("tr")
        ])
        data = data.with_columns([
            pl.col("tr").rolling_mean(window_size=14).over("code").alias("factor_atr")
        ])
    
    # 填充NaN
    factor_cols = [col for col in data.columns if col.startswith("factor_")]
    for col in factor_cols:
        data = data.with_columns([
            pl.when(pl.col(col).is_nan() | pl.col(col).is_null())
            .then(0)
            .otherwise(pl.col(col))
            .alias(col)
        ])
    
    return data


def calculate_composite_score(data: pl.DataFrame, factor_weights: Dict[str, float]) -> pl.DataFrame:
    """计算综合评分"""
    # 标准化因子值
    for factor_name in factor_weights.keys():
        factor_col = f"factor_{factor_name}"
        zscore_col = f"{factor_col}_zscore"
        
        if factor_col in data.columns:
            mean_val = data[factor_col].mean()
            std_val = data[factor_col].std()
            
            if std_val and std_val > 0:
                data = data.with_columns([
                    ((pl.col(factor_col) - mean_val) / std_val).alias(zscore_col)
                ])
            else:
                data = data.with_columns([
                    pl.lit(0).alias(zscore_col)
                ])
    
    # 计算加权综合评分
    score_expr = pl.lit(0)
    total_weight = sum(abs(w) for w in factor_weights.values())
    
    for factor_name, weight in factor_weights.items():
        zscore_col = f"factor_{factor_name}_zscore"
        if zscore_col in data.columns:
            score_expr = score_expr + pl.col(zscore_col) * (weight / total_weight)
    
    data = data.with_columns([
        score_expr.alias("composite_score")
    ])
    
    return data


def test_combination(
    data: pl.DataFrame,
    filter_combo: List[str],
    factor_weights: Dict[str, float],
    config
) -> ChampionResult:
    """测试一个组合"""
    combo_id = hash(str(filter_combo) + str(factor_weights)) % 100000
    
    # 1. 应用过滤器
    filtered_data = apply_filters(data, filter_combo, config)
    
    if len(filtered_data) < 100:
        return None
    
    # 2. 计算因子
    data_with_factors = calculate_factors(filtered_data, factor_weights)
    
    # 3. 计算综合评分
    data_scored = calculate_composite_score(data_with_factors, factor_weights)
    
    # 4. 分析因子有效性
    analyzer = FactorAnalyzer()
    
    try:
        metrics = analyzer.analyze_factor(data_scored, "composite_score", "forward_return_1d")
        
        # 计算综合评分 (加权平均)
        # IC_IR权重40%, 多空收益30%, 夏普20%, 回撤10%
        ic_ir_score = min(abs(metrics.ic_ir) / 0.5, 1.0)  # 归一化到0-1
        return_score = min(max(metrics.long_short_return, 0) / 0.5, 1.0)
        sharpe_score = min(max(metrics.long_short_sharpe, 0) / 2.0, 1.0)
        dd_score = 1.0 - min(abs(min(metrics.max_drawdown, 0)) / 0.3, 1.0)
        
        total_score = ic_ir_score * 0.4 + return_score * 0.3 + sharpe_score * 0.2 + dd_score * 0.1
        
        return ChampionResult(
            combo_id=combo_id,
            filter_combo=filter_combo,
            factor_combo=factor_weights,
            ic_mean=metrics.ic_mean,
            ic_ir=metrics.ic_ir,
            ic_positive_ratio=metrics.ic_positive_ratio,
            long_short_return=metrics.long_short_return,
            long_short_sharpe=metrics.long_short_sharpe,
            max_drawdown=metrics.max_drawdown,
            win_rate=0.5,  # 简化
            total_score=total_score
        )
    except Exception as e:
        logger.debug(f"分析失败: {e}")
        return None


def generate_filter_combinations(available_filters: List[str]) -> List[List[str]]:
    """生成过滤器组合"""
    combos = []
    
    # 基础组合
    base = ['min_volume', 'price_range']
    combos.append(base)
    
    # 加科创板过滤
    combos.append(base + ['exclude_kcb'])
    
    # 加北交所过滤
    combos.append(base + ['exclude_bse'])
    
    # 全部板块过滤
    combos.append(base + ['exclude_kcb', 'exclude_bse'])
    
    # 加创业板过滤
    combos.append(base + ['exclude_kcb', 'exclude_bse', 'exclude_cyb'])
    
    # 加换手率
    combos.append(base + ['exclude_kcb', 'exclude_bse', 'turnover_rate'])
    
    return combos


def generate_factor_combinations(available_factors: List[str]) -> List[Dict[str, float]]:
    """生成因子权重组合"""
    combos = []
    
    # 预定义一些有效的组合
    combos.append({'ma5_bias': 0.4, 'v_ratio10': 0.3, 'cost_peak': 0.3})
    combos.append({'ma5_bias': 0.3, 'ma5_slope': 0.3, 'v_ratio10': 0.2, 'cost_peak': 0.2})
    combos.append({'rsi': 0.3, 'macd': 0.3, 'bollinger': 0.2, 'atr': 0.2})
    combos.append({'ma5_bias': 0.25, 'rsi': 0.25, 'v_ratio10': 0.25, 'cost_peak': 0.25})
    combos.append({'ma5_slope': 0.4, 'macd': 0.3, 'v_ratio10': 0.3})
    combos.append({'cost_peak': 0.5, 'v_ratio10': 0.3, 'ma5_bias': 0.2})
    
    # 反向组合
    combos.append({'ma5_bias': -0.4, 'v_ratio10': 0.3, 'cost_peak': 0.3})
    combos.append({'rsi': -0.3, 'macd': -0.3, 'bollinger': 0.4})
    
    return combos


def print_champion_results(results: List[ChampionResult], top_n: int = 10):
    """打印冠军组合结果"""
    print("\n" + "=" * 140)
    print(f"🏆 冠军组合排行榜 (Top {top_n})")
    print("=" * 140)
    
    # 按综合评分排序
    sorted_results = sorted(results, key=lambda x: x.total_score, reverse=True)
    
    print(f"\n{'排名':<4} {'综合分':>8} {'IC_IR':>8} {'多空收益':>10} {'夏普':>8} {'最大回撤':>10} {'过滤器':<40} {'因子权重'}")
    print("-" * 140)
    
    for i, r in enumerate(sorted_results[:top_n], 1):
        filter_str = ",".join(r.filter_combo[:3]) + ("..." if len(r.filter_combo) > 3 else "")
        factor_str = ",".join([f"{k}={v:.1f}" for k, v in list(r.factor_combo.items())[:3]])
        print(f"{i:<4} {r.total_score:>8.4f} {r.ic_ir:>8.4f} {r.long_short_return:>10.2%} "
              f"{r.long_short_sharpe:>8.2f} {r.max_drawdown:>10.2%} {filter_str:<40} {factor_str}")
    
    print("=" * 140)
    
    # 打印冠军详情
    if sorted_results:
        champion = sorted_results[0]
        print("\n" + "🥇 " * 30)
        print("【冠军组合详情】")
        print(f"  综合评分: {champion.total_score:.4f}")
        print(f"  IC均值: {champion.ic_mean:.4f}")
        print(f"  IC_IR: {champion.ic_ir:.4f}")
        print(f"  多空收益: {champion.long_short_return:.2%}")
        print(f"  夏普比率: {champion.long_short_sharpe:.2f}")
        print(f"  最大回撤: {champion.max_drawdown:.2%}")
        print(f"\n  过滤器组合:")
        for f in champion.filter_combo:
            print(f"    - {f}")
        print(f"\n  因子权重:")
        for factor, weight in champion.factor_combo.items():
            print(f"    - {factor}: {weight:.2f}")
        print("🥇 " * 30)


def main():
    parser = argparse.ArgumentParser(description='冠军组合搜索器')
    parser.add_argument('--start-date', type=str, default='2024-06-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=200)
    parser.add_argument('--output', type=str, default='champion_combo_results.csv')
    
    args = parser.parse_args()
    
    print("=" * 140)
    print("🏆 冠军组合搜索器")
    print("=" * 140)
    print(f"数据区间: {args.start_date} ~ {args.end_date}")
    print(f"最大股票数: {args.max_stocks}")
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
    
    # 生成组合
    filter_combos = generate_filter_combinations([])
    factor_combos = generate_factor_combinations([])
    
    total_combos = len(filter_combos) * len(factor_combos)
    logger.info(f"将测试 {len(filter_combos)} 种过滤器组合 × {len(factor_combos)} 种因子组合 = {total_combos} 种总组合")
    
    # 测试所有组合
    results = []
    combo_count = 0
    
    for filter_combo in filter_combos:
        for factor_combo in factor_combos:
            combo_count += 1
            if combo_count % 10 == 0:
                logger.info(f"进度: {combo_count}/{total_combos}")
            
            result = test_combination(data, filter_combo, factor_combo, config)
            if result:
                results.append(result)
    
    # 打印结果
    print_champion_results(results, top_n=15)
    
    # 保存结果
    if results:
        results_df = pl.DataFrame([r.to_dict() for r in results])
        output_path = Path(args.output)
        results_df.write_csv(output_path)
        logger.info(f"结果已保存: {output_path}")
    
    print(f"\n共测试 {total_combos} 种组合，有效结果 {len(results)} 个")
    print(f"最优组合综合评分: {max(r.total_score for r in results):.4f}" if results else "无有效结果")


if __name__ == "__main__":
    main()
