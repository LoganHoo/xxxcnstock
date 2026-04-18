#!/usr/bin/env python3
"""
增强版选股服务

整合配置管理器，支持:
1. 从配置文件加载因子和过滤器
2. 动态调整因子权重
3. 灵活配置过滤器组合
4. 支持多种选股策略
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

from core.factor_filter_config import get_factor_filter_config, FactorConfig, FilterConfig

logger = logging.getLogger(__name__)


@dataclass
class StockPickResult:
    """选股结果"""
    code: str
    name: str = ""
    strategy: str = ""  # 选股策略
    score: float = 0.0
    grade: str = ""
    factors: Dict[str, float] = None
    filters_passed: List[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'name': self.name,
            'strategy': self.strategy,
            'score': self.score,
            'grade': self.grade,
            'factors': self.factors or {},
            'filters_passed': self.filters_passed or [],
        }


class EnhancedStockPicker:
    """增强版选股器"""
    
    def __init__(self, config_manager=None):
        self.config = config_manager or get_factor_filter_config()
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / "data"
        
    def load_kline_data(self, date: str, max_stocks: int = None) -> pl.DataFrame:
        """加载K线数据"""
        kline_dir = self.data_dir / "kline"
        
        if not kline_dir.exists():
            logger.error(f"K线数据目录不存在: {kline_dir}")
            return pl.DataFrame()
        
        all_data = []
        count = 0
        
        for parquet_file in kline_dir.glob("*.parquet"):
            if max_stocks and count >= max_stocks:
                break
            
            try:
                df = pl.read_parquet(parquet_file)
                
                # 统一列类型
                df = df.with_columns([
                    pl.col("code").cast(pl.Utf8),
                    pl.col("trade_date").cast(pl.Utf8),
                    pl.col("open").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("volume").cast(pl.Float64),
                ])
                
                # 获取指定日期的数据
                df = df.filter(pl.col("trade_date") == date)
                
                if len(df) > 0:
                    all_data.append(df)
                    count += 1
            except Exception as e:
                logger.debug(f"加载文件失败 {parquet_file}: {e}")
        
        if not all_data:
            return pl.DataFrame()
        
        return pl.concat(all_data)
    
    def calculate_factors(self, data: pl.DataFrame, factor_ids: List[str] = None) -> pl.DataFrame:
        """
        计算因子值
        
        Args:
            data: K线数据
            factor_ids: 要计算的因子列表，None表示计算所有启用的因子
        
        Returns:
            添加了因子列的数据
        """
        if factor_ids is None:
            factors = self.config.get_enabled_factors()
            factor_ids = list(factors.keys())
        
        for factor_id in factor_ids:
            factor_config = self.config.get_factor(factor_id)
            if not factor_config:
                continue
            
            params = factor_config.params
            
            try:
                if factor_id == 'ma5_bias':
                    window = params.get('window', 5)
                    data = self._calc_ma_bias(data, window)
                elif factor_id == 'ma5_slope':
                    window = params.get('window', 5)
                    data = self._calc_ma_slope(data, window)
                elif factor_id == 'ma_trend':
                    short_p = params.get('short_period', 5)
                    long_p = params.get('long_period', 20)
                    data = self._calc_ma_trend(data, short_p, long_p)
                elif factor_id == 'rsi':
                    period = params.get('period', 14)
                    data = self._calc_rsi(data, period)
                elif factor_id == 'v_ratio10':
                    data = self._calc_v_ratio(data, 5)
                elif factor_id == 'volume_ratio':
                    period = params.get('period', 5)
                    data = self._calc_v_ratio(data, period)
                elif factor_id == 'turnover':
                    data = self._calc_turnover(data)
                elif factor_id == 'cost_peak':
                    window = params.get('window', 20)
                    data = self._calc_cost_peak(data, window)
                elif factor_id == 'limit_up_score':
                    data = self._calc_limit_up_score(data)
                elif factor_id == 'bollinger':
                    period = params.get('period', 20)
                    std_dev = params.get('std_dev', 2)
                    data = self._calc_bollinger(data, period, std_dev)
                elif factor_id == 'atr':
                    period = params.get('period', 14)
                    data = self._calc_atr(data, period)
                elif factor_id == 'macd':
                    fast = params.get('fast', 12)
                    slow = params.get('slow', 26)
                    signal = params.get('signal', 9)
                    data = self._calc_macd(data, fast, slow, signal)
                elif factor_id == 'kdj':
                    n = params.get('n', 9)
                    m1 = params.get('m1', 3)
                    m2 = params.get('m2', 3)
                    data = self._calc_kdj(data, n, m1, m2)
                    
            except Exception as e:
                logger.warning(f"计算因子 {factor_id} 失败: {e}")
                continue
        
        return data
    
    def _calc_ma_bias(self, data: pl.DataFrame, window: int) -> pl.DataFrame:
        """计算MA乖离率"""
        ma_col = f"ma{window}"
        factor_col = f"factor_{ma_col}_bias"
        
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=window).over("code").alias(ma_col)
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col(ma_col)) / pl.col(ma_col)).alias(factor_col)
        ])
        return data
    
    def _calc_ma_slope(self, data: pl.DataFrame, window: int) -> pl.DataFrame:
        """计算MA斜率"""
        factor_col = f"factor_ma{window}_slope"
        data = data.with_columns([
            (pl.col("close") - pl.col("close").shift(window-1).over("code")).alias(factor_col)
        ])
        return data
    
    def _calc_ma_trend(self, data: pl.DataFrame, short_p: int, long_p: int) -> pl.DataFrame:
        """计算均线趋势"""
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=short_p).over("code").alias(f"ma{short_p}"),
            pl.col("close").rolling_mean(window_size=long_p).over("code").alias(f"ma{long_p}")
        ])
        data = data.with_columns([
            (pl.col(f"ma{short_p}") > pl.col(f"ma{long_p}")).cast(pl.Int64).alias("factor_ma_trend")
        ])
        return data
    
    def _calc_rsi(self, data: pl.DataFrame, period: int) -> pl.DataFrame:
        """计算RSI"""
        data = data.with_columns([
            pl.col("close").diff().over("code").alias("price_diff")
        ])
        data = data.with_columns([
            pl.when(pl.col("price_diff") > 0).then(pl.col("price_diff")).otherwise(0).alias("gain"),
            pl.when(pl.col("price_diff") < 0).then(-pl.col("price_diff")).otherwise(0).alias("loss")
        ])
        data = data.with_columns([
            pl.col("gain").rolling_mean(window_size=period).over("code").alias("avg_gain"),
            pl.col("loss").rolling_mean(window_size=period).over("code").alias("avg_loss")
        ])
        data = data.with_columns([
            (100 - (100 / (1 + pl.col("avg_gain") / pl.col("avg_loss")))).alias("factor_rsi")
        ])
        return data
    
    def _calc_v_ratio(self, data: pl.DataFrame, period: int) -> pl.DataFrame:
        """计算量比"""
        factor_col = f"factor_v_ratio{period}"
        data = data.with_columns([
            pl.col("volume").rolling_mean(window_size=period).over("code").alias(f"volume_ma{period}")
        ])
        data = data.with_columns([
            (pl.col("volume") / pl.col(f"volume_ma{period}")).alias(factor_col)
        ])
        return data
    
    def _calc_turnover(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算换手率（简化版）"""
        # 实际计算需要流通股本数据
        data = data.with_columns([
            pl.lit(0.05).alias("factor_turnover")  # 占位符
        ])
        return data
    
    def _calc_cost_peak(self, data: pl.DataFrame, window: int) -> pl.DataFrame:
        """计算筹码峰位"""
        data = data.with_columns([
            pl.col("close").rolling_min(window_size=window).over("code").alias(f"low_{window}d"),
            pl.col("close").rolling_max(window_size=window).over("code").alias(f"high_{window}d")
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col(f"low_{window}d")) / 
             (pl.col(f"high_{window}d") - pl.col(f"low_{window}d")))
            .fill_nan(0.5)
            .alias("factor_cost_peak")
        ])
        return data
    
    def _calc_limit_up_score(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算涨停情绪得分"""
        # 简化版：使用涨跌幅代替
        data = data.with_columns([
            pl.col("close").shift(1).over("code").alias("prev_close")
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col("prev_close")) / pl.col("prev_close")).alias("change_pct")
        ])
        data = data.with_columns([
            pl.when(pl.col("change_pct") > 0.095).then(1).otherwise(0).alias("is_limit_up")
        ])
        
        # 计算每日涨停家数
        daily_stats = data.group_by("trade_date").agg([
            pl.sum("is_limit_up").alias("limit_up_count")
        ])
        
        data = data.join(daily_stats, on="trade_date", how="left")
        data = data.with_columns([
            pl.col("limit_up_count").alias("factor_limit_up_score")
        ])
        return data
    
    def _calc_bollinger(self, data: pl.DataFrame, period: int, std_dev: float) -> pl.DataFrame:
        """计算布林带位置"""
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=period).over("code").alias(f"bb_ma{period}"),
            pl.col("close").rolling_std(window_size=period).over("code").alias(f"bb_std{period}")
        ])
        data = data.with_columns([
            (pl.col("close") - pl.col(f"bb_ma{period}")) / (pl.col(f"bb_std{period}") * std_dev)
        ])
        data = data.with_columns([
            pl.when(pl.col(f"bb_std{period}") > 0)
            .then((pl.col("close") - pl.col(f"bb_ma{period}")) / (pl.col(f"bb_std{period}") * std_dev))
            .otherwise(0)
            .alias("factor_bollinger")
        ])
        return data
    
    def _calc_atr(self, data: pl.DataFrame, period: int) -> pl.DataFrame:
        """计算ATR"""
        data = data.with_columns([
            (pl.col("high") - pl.col("low")).alias("tr1"),
            (pl.col("high") - pl.col("close").shift(1).over("code")).abs().alias("tr2"),
            (pl.col("low") - pl.col("close").shift(1).over("code")).abs().alias("tr3")
        ])
        data = data.with_columns([
            pl.max_horizontal(["tr1", "tr2", "tr3"]).alias("tr")
        ])
        data = data.with_columns([
            pl.col("tr").rolling_mean(window_size=period).over("code").alias("factor_atr")
        ])
        return data
    
    def _calc_macd(self, data: pl.DataFrame, fast: int, slow: int, signal: int) -> pl.DataFrame:
        """计算MACD"""
        data = data.with_columns([
            pl.col("close").ewm_mean(span=fast).over("code").alias(f"ema{fast}"),
            pl.col("close").ewm_mean(span=slow).over("code").alias(f"ema{slow}")
        ])
        data = data.with_columns([
            (pl.col(f"ema{fast}") - pl.col(f"ema{slow}")).alias("factor_macd")
        ])
        return data
    
    def _calc_kdj(self, data: pl.DataFrame, n: int, m1: int, m2: int) -> pl.DataFrame:
        """计算KDJ"""
        data = data.with_columns([
            pl.col("low").rolling_min(window_size=n).over("code").alias(f"lowest{n}"),
            pl.col("high").rolling_max(window_size=n).over("code").alias(f"highest{n}")
        ])
        data = data.with_columns([
            ((pl.col("close") - pl.col(f"lowest{n}")) / 
             (pl.col(f"highest{n}") - pl.col(f"lowest{n}")) * 100)
            .fill_nan(50)
            .alias("rsv")
        ])
        data = data.with_columns([
            pl.col("rsv").ewm_mean(span=m1).over("code").alias("factor_kdj_k")
        ])
        return data
    
    def apply_filters(self, data: pl.DataFrame, filter_combo: str = None) -> Tuple[pl.DataFrame, List[str]]:
        """
        应用过滤器
        
        Args:
            data: 股票数据
            filter_combo: 过滤器组合名称，None表示使用所有启用的过滤器
        
        Returns:
            (过滤后的数据, 通过的过滤器列表)
        """
        passed_filters = []
        original_count = len(data)
        
        if filter_combo:
            combo = self.config.get_filter_combination(filter_combo)
            if combo:
                filter_ids = combo.filters
            else:
                filter_ids = []
        else:
            enabled_filters = self.config.get_enabled_filters()
            filter_ids = list(enabled_filters.keys())
        
        for filter_id in filter_ids:
            filter_config = self.config.get_filter(filter_id)
            if not filter_config:
                continue
            
            params = filter_config.params
            before_count = len(data)
            
            try:
                if filter_id == 'data_freshness':
                    # 数据新鲜度已在加载时处理
                    passed_filters.append(filter_id)
                elif filter_id == 'delisting':
                    # 退市过滤
                    data = data.filter(~pl.col("code").str.contains("退"))
                    passed_filters.append(filter_id)
                elif filter_id == 'st_filter':
                    # ST过滤（需要股票名称）
                    passed_filters.append(filter_id)
                elif filter_id == 'suspension':
                    min_vol = params.get('min_volume', 0)
                    data = data.filter(pl.col("volume") > min_vol)
                    passed_filters.append(filter_id)
                elif filter_id == 'min_volume':
                    min_vol = params.get('min_volume', 1000000)
                    data = data.filter(pl.col("volume") >= min_vol)
                    passed_filters.append(filter_id)
                elif filter_id == 'price_range':
                    min_p = params.get('min_price', 3.0)
                    max_p = params.get('max_price', 200.0)
                    data = data.filter(
                        (pl.col("close") >= min_p) &
                        (pl.col("close") <= max_p)
                    )
                    passed_filters.append(filter_id)
                elif filter_id == 'exclude_kcb':
                    prefixes = params.get('excluded_prefixes', ['688'])
                    for prefix in prefixes:
                        data = data.filter(~pl.col("code").str.starts_with(prefix))
                    passed_filters.append(filter_id)
                elif filter_id == 'exclude_bse':
                    prefixes = params.get('excluded_prefixes', ['8', '4'])
                    for prefix in prefixes:
                        data = data.filter(~pl.col("code").str.starts_with(prefix))
                    passed_filters.append(filter_id)
                elif filter_id == 'exclude_cyb':
                    prefixes = params.get('excluded_prefixes', ['300'])
                    for prefix in prefixes:
                        data = data.filter(~pl.col("code").str.starts_with(prefix))
                    passed_filters.append(filter_id)
                elif filter_id == 'turnover_rate':
                    # 换手率过滤（需要额外数据）
                    passed_filters.append(filter_id)
                elif filter_id == 'sideways':
                    # 横盘过滤（需要计算波动率）
                    passed_filters.append(filter_id)
                    
            except Exception as e:
                logger.warning(f"应用过滤器 {filter_id} 失败: {e}")
                continue
        
        filtered_count = original_count - len(data)
        logger.info(f"过滤器: 原始{original_count}只 -> 过滤后{len(data)}只 (过滤{filtered_count}只)")
        
        return data, passed_filters
    
    def calculate_composite_score(self, data: pl.DataFrame, factor_combo: str) -> pl.DataFrame:
        """
        计算综合评分
        
        Args:
            data: 包含因子值的数据
            factor_combo: 因子组合名称
        
        Returns:
            添加了综合评分的数据
        """
        combo = self.config.get_factor_combination(factor_combo)
        if not combo:
            logger.warning(f"因子组合 {factor_combo} 不存在")
            return data
        
        # 标准化因子值
        for factor_item in combo.factors:
            factor_name = factor_item['name']
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
        total_weight = sum(f['weight'] for f in combo.factors)
        
        for factor_item in combo.factors:
            factor_name = factor_item['name']
            weight = factor_item['weight']
            zscore_col = f"factor_{factor_name}_zscore"
            
            if zscore_col in data.columns:
                score_expr = score_expr + pl.col(zscore_col) * (weight / total_weight)
        
        data = data.with_columns([
            score_expr.alias("composite_score")
        ])
        
        return data
    
    def select_stocks(self, date: str, strategy: str = "fund_behavior_trend", 
                     top_n: int = 50) -> List[StockPickResult]:
        """
        选股主函数
        
        Args:
            date: 选股日期
            strategy: 选股策略（因子组合名称）
            top_n: 返回股票数量
        
        Returns:
            选股结果列表
        """
        logger.info(f"开始选股: 日期={date}, 策略={strategy}")
        
        # 1. 加载数据
        data = self.load_kline_data(date)
        if len(data) == 0:
            logger.error("未加载到数据")
            return []
        
        logger.info(f"加载了 {len(data)} 只股票的数据")
        
        # 2. 应用过滤器
        data, passed_filters = self.apply_filters(data)
        
        if len(data) == 0:
            logger.warning("所有股票被过滤")
            return []
        
        # 3. 计算因子
        combo = self.config.get_factor_combination(strategy)
        if combo:
            factor_ids = [f['name'] for f in combo.factors]
            data = self.calculate_factors(data, factor_ids)
        else:
            data = self.calculate_factors(data)
        
        # 4. 计算综合评分
        data = self.calculate_composite_score(data, strategy)
        
        # 5. 排序并选择Top N
        data = data.sort("composite_score", descending=True)
        
        results = []
        for row in data.head(top_n).iter_rows(named=True):
            # 收集因子值
            factors = {}
            for col in data.columns:
                if col.startswith("factor_") and not col.endswith("_zscore"):
                    factor_name = col.replace("factor_", "")
                    factors[factor_name] = row.get(col, 0)
            
            # 确定等级
            score = row.get("composite_score", 0)
            if score > 1.5:
                grade = "S"
            elif score > 0.5:
                grade = "A"
            elif score > 0:
                grade = "B"
            else:
                grade = "C"
            
            result = StockPickResult(
                code=row.get("code", ""),
                strategy=strategy,
                score=score,
                grade=grade,
                factors=factors,
                filters_passed=passed_filters
            )
            results.append(result)
        
        logger.info(f"选股完成: 选中 {len(results)} 只股票")
        return results
    
    def save_results(self, results: List[StockPickResult], date: str, output_dir: str = None):
        """保存选股结果"""
        if output_dir is None:
            output_dir = self.data_dir / "picks"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 转换为DataFrame - 展平嵌套数据
        flat_data = []
        for r in results:
            flat_record = {
                'code': r.code,
                'name': r.name,
                'strategy': r.strategy,
                'score': r.score,
                'grade': r.grade,
                'filters_passed': ','.join(r.filters_passed) if r.filters_passed else '',
            }
            # 添加因子值
            if r.factors:
                for factor_name, factor_value in r.factors.items():
                    flat_record[f'factor_{factor_name}'] = factor_value
            flat_data.append(flat_record)
        
        df = pl.DataFrame(flat_data)
        
        # 保存Parquet
        output_file = output_dir / f"stock_picks_{date}.parquet"
        df.write_parquet(output_file)
        
        # 同时保存CSV
        csv_file = output_dir / f"stock_picks_{date}.csv"
        df.write_csv(csv_file)
        
        logger.info(f"选股结果已保存: {output_file}")
        
        return output_file


def main():
    """测试选股器"""
    import argparse
    
    parser = argparse.ArgumentParser(description='增强版选股器')
    parser.add_argument('--date', type=str, required=True, help='选股日期 YYYY-MM-DD')
    parser.add_argument('--strategy', type=str, default='fund_behavior_trend',
                       help='选股策略 (fund_behavior_trend/fund_behavior_short/technical_combo)')
    parser.add_argument('--top-n', type=int, default=50, help='返回股票数量')
    parser.add_argument('--output', type=str, help='输出目录')
    
    args = parser.parse_args()
    
    # 创建选股器
    picker = EnhancedStockPicker()
    
    # 执行选股
    results = picker.select_stocks(args.date, args.strategy, args.top_n)
    
    # 打印结果
    print("\n" + "=" * 100)
    print(f"选股结果: {args.date} | 策略: {args.strategy}")
    print("=" * 100)
    print(f"\n{'排名':<4} {'代码':<10} {'评分':>10} {'等级':<4} {'主要因子'}")
    print("-" * 100)
    
    for i, r in enumerate(results[:20], 1):
        # 过滤掉None值
        valid_factors = {k: v for k, v in r.factors.items() if v is not None}
        top_factors = sorted(valid_factors.items(), key=lambda x: abs(x[1]) if x[1] is not None else 0, reverse=True)[:3]
        factor_str = ", ".join([f"{k}={v:.3f}" for k, v in top_factors])
        print(f"{i:<4} {r.code:<10} {r.score:>10.4f} {r.grade:<4} {factor_str}")
    
    print("=" * 100)
    
    # 保存结果
    if results:
        picker.save_results(results, args.date, args.output)


if __name__ == "__main__":
    main()
