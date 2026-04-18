#!/usr/bin/env python3
"""
因子分析V2 - 全面优化版
优化点：
1. 全市场5000+股票数据
2. 基本面因子（PE、PB、ROE等）
3. XGBoost机器学习组合
4. 动态权重调整
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import polars as pl
import numpy as np
from typing import List, Dict, Tuple, Optional
import logging
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from core.factor_library import FactorRegistry
import factors.technical
import factors.volume_price
import factors.market

from core.factor_analyzer import FactorAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundamentalFactorCalculator:
    """基本面因子计算器"""
    
    def calculate_fundamental_factors(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        计算基本面因子（基于价格和成交量的代理指标）
        
        由于缺少财务报表数据，使用以下代理指标：
        - PE代理: 价格趋势 + 成交量稳定性
        - PB代理: 价格/均线比率
        - ROE代理: 收益率稳定性
        """
        logger.info("计算基本面因子...")
        
        # 1. PE代理因子 - 价格收益比（反向指标）
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=20).over("code").alias("ma20"),
            pl.col("close").rolling_std(window_size=20).over("code").alias("volatility_20d")
        ])
        
        # PE代理 = 价格稳定性 / 波动率（高稳定性+低波动 = 低PE特征）
        data = data.with_columns([
            (pl.col("ma20") / (pl.col("volatility_20d") + 0.001)).alias("factor_pe_proxy")
        ])
        
        # 2. PB代理因子 - 市净率代理（价格/均线）
        data = data.with_columns([
            (pl.col("close") / pl.col("ma20")).alias("factor_pb_proxy")
        ])
        
        # 3. ROE代理因子 - 收益稳定性
        data = data.with_columns([
            pl.col("close").pct_change().over("code").alias("daily_return")
        ])
        
        data = data.with_columns([
            pl.col("daily_return").rolling_mean(window_size=60).over("code").alias("return_mean_60d"),
            pl.col("daily_return").rolling_std(window_size=60).over("code").alias("return_std_60d")
        ])
        
        # ROE代理 = 收益均值 / 收益波动（夏普比率的变体）
        data = data.with_columns([
            (pl.col("return_mean_60d") / (pl.col("return_std_60d") + 0.001)).alias("factor_roe_proxy")
        ])
        
        # 4. 价值因子 - 价格位置（越低越有价值）
        data = data.with_columns([
            pl.col("close").rolling_min(window_size=252).over("code").alias("low_1y"),
            pl.col("close").rolling_max(window_size=252).over("code").alias("high_1y")
        ])
        
        data = data.with_columns([
            ((pl.col("close") - pl.col("low_1y")) / (pl.col("high_1y") - pl.col("low_1y") + 0.001))
            .alias("factor_value_score")
        ])
        
        # 5. 质量因子 - 趋势稳定性
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=5).over("code").alias("ma5"),
            pl.col("close").rolling_mean(window_size=10).over("code").alias("ma10"),
            pl.col("close").rolling_mean(window_size=30).over("code").alias("ma30")
        ])
        
        # 多头排列得分
        data = data.with_columns([
            ((pl.col("ma5") > pl.col("ma10")).cast(pl.Int32) +
             (pl.col("ma10") > pl.col("ma30")).cast(pl.Int32) +
             (pl.col("close") > pl.col("ma5")).cast(pl.Int32))
            .alias("factor_quality_score")
        ])
        
        # 清理临时列
        temp_cols = ["ma20", "volatility_20d", "daily_return", "return_mean_60d", 
                     "return_std_60d", "low_1y", "high_1y", "ma5", "ma10", "ma30"]
        data = data.drop([c for c in temp_cols if c in data.columns])
        
        logger.info("基本面因子计算完成")
        return data


class XGBoostFactorCombiner:
    """XGBoost因子组合器"""
    
    def __init__(self):
        self.model = None
        self.feature_importance = {}
        
    def train(self, data: pl.DataFrame, factor_cols: List[str], target_col: str = "forward_return_5d") -> Dict:
        """
        训练XGBoost模型组合因子
        
        Returns:
            特征重要性字典
        """
        try:
            import xgboost as xgb
        except ImportError:
            logger.warning("XGBoost未安装，使用等权组合")
            return self._equal_weight_combination(data, factor_cols)
        
        logger.info("训练XGBoost因子组合模型...")
        
        # 准备数据
        valid_data = data.filter(pl.col(target_col).is_not_null())
        
        if len(valid_data) < 1000:
            logger.warning("数据量不足，使用等权组合")
            return self._equal_weight_combination(data, factor_cols)
        
        # 特征矩阵
        X = valid_data[factor_cols].to_numpy()
        y = valid_data[target_col].to_numpy()
        
        # 去除NaN和Inf
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isinf(X).any(axis=1) | np.isnan(y) | np.isinf(y))
        X = X[valid_mask]
        y = y[valid_mask]
        
        if len(X) < 1000:
            logger.warning("有效数据量不足，使用等权组合")
            return self._equal_weight_combination(data, factor_cols)
        
        # 分位数回归（预测收益排名）
        y_rank = np.argsort(np.argsort(y)) / len(y)  # 转换为排名百分比
        
        # 训练模型
        self.model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )
        
        self.model.fit(X, y_rank)
        
        # 获取特征重要性
        importance = self.model.feature_importances_
        self.feature_importance = {
            factor_cols[i]: importance[i] 
            for i in range(len(factor_cols))
        }
        
        # 预测组合因子
        predictions = self.model.predict(X)
        
        logger.info(f"XGBoost训练完成，特征重要性Top5:")
        for factor, imp in sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  {factor}: {imp:.4f}")
        
        return self.feature_importance
    
    def predict(self, data: pl.DataFrame, factor_cols: List[str]) -> pl.DataFrame:
        """使用训练好的模型预测组合因子"""
        if self.model is None:
            return self._equal_weight_predict(data, factor_cols)
        
        X = data[factor_cols].to_numpy()
        
        # 处理NaN
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        predictions = self.model.predict(X)
        
        return data.with_columns([
            pl.Series(predictions).alias("factor_xgboost_combined")
        ])
    
    def _equal_weight_combination(self, data: pl.DataFrame, factor_cols: List[str]) -> Dict:
        """等权组合（备用方案）"""
        n = len(factor_cols)
        self.feature_importance = {col: 1.0/n for col in factor_cols}
        return self.feature_importance
    
    def _equal_weight_predict(self, data: pl.DataFrame, factor_cols: List[str]) -> pl.DataFrame:
        """等权组合预测"""
        combined = pl.lit(0.0)
        for col in factor_cols:
            combined = combined + pl.col(col)
        
        return data.with_columns([
            (combined / len(factor_cols)).alias("factor_xgboost_combined")
        ])


class DynamicWeightAdjuster:
    """动态权重调整器"""
    
    def __init__(self, lookback_window: int = 60):
        self.lookback_window = lookback_window
        self.factor_performance = {}
        
    def calculate_dynamic_weights(
        self, 
        data: pl.DataFrame, 
        factor_cols: List[str],
        return_col: str = "forward_return_5d"
    ) -> Dict[str, float]:
        """
        计算动态权重
        
        方法：基于近期IC表现调整权重
        - IC高的因子给予更高权重
        - 考虑IC的稳定性
        """
        logger.info("计算动态权重...")
        
        from scipy import stats
        
        weights = {}
        ic_scores = []
        
        # 获取最近的lookback_window天
        dates = sorted(data["trade_date"].unique().to_list())
        recent_dates = dates[-self.lookback_window:] if len(dates) > self.lookback_window else dates
        recent_data = data.filter(pl.col("trade_date").is_in(recent_dates))
        
        for factor_col in factor_cols:
            try:
                # 计算近期IC
                factor_values = recent_data[factor_col].to_numpy()
                returns = recent_data[return_col].to_numpy()
                
                mask = ~(np.isnan(factor_values) | np.isnan(returns) | 
                        np.isinf(factor_values) | np.isinf(returns))
                factor_values = factor_values[mask]
                returns = returns[mask]
                
                if len(factor_values) < 100:
                    continue
                
                ic, _ = stats.spearmanr(factor_values, returns)
                ic_scores.append((factor_col, abs(ic)))
                
            except Exception as e:
                logger.debug(f"计算IC失败 {factor_col}: {e}")
        
        if not ic_scores:
            # 等权
            n = len(factor_cols)
            return {col: 1.0/n for col in factor_cols}
        
        # 基于IC绝对值计算权重
        total_ic = sum(score for _, score in ic_scores)
        if total_ic == 0:
            n = len(ic_scores)
            return {col: 1.0/n for col, _ in ic_scores}
        
        weights = {col: score/total_ic for col, score in ic_scores}
        
        logger.info("动态权重Top5:")
        for factor, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  {factor}: {weight:.4f}")
        
        return weights
    
    def apply_dynamic_weights(self, data: pl.DataFrame, weights: Dict[str, float]) -> pl.DataFrame:
        """应用动态权重计算组合因子"""
        weighted_sum = pl.lit(0.0)
        
        for factor_col, weight in weights.items():
            if factor_col in data.columns:
                weighted_sum = weighted_sum + pl.col(factor_col) * weight
        
        return data.with_columns([
            weighted_sum.alias("factor_dynamic_weighted")
        ])


class FactorAnalysisV2:
    """因子分析V2主类"""
    
    def __init__(self):
        self.analyzer = FactorAnalyzer()
        self.fundamental_calc = FundamentalFactorCalculator()
        self.xgboost_combiner = XGBoostFactorCombiner()
        self.dynamic_adjuster = DynamicWeightAdjuster()
    
    def load_data(self, start_date: str, end_date: str, max_stocks: Optional[int] = None) -> pl.DataFrame:
        """加载数据"""
        data_path = PROJECT_ROOT / "data" / "kline"
        
        if not data_path.exists():
            logger.error(f"数据目录不存在: {data_path}")
            return pl.DataFrame()
        
        all_data = []
        count = 0
        
        parquet_files = list(data_path.glob("*.parquet"))
        logger.info(f"发现 {len(parquet_files)} 个数据文件")
        
        for parquet_file in parquet_files:
            if max_stocks and count >= max_stocks:
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
                    
                    if count % 1000 == 0:
                        logger.info(f"已加载 {count} 只股票...")
                        
            except Exception as e:
                pass
        
        if not all_data:
            return pl.DataFrame()
        
        data = pl.concat(all_data)
        logger.info(f"总数据量: {len(data)} 条, {data['code'].n_unique()} 只股票")
        
        return data
    
    def filter_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """数据过滤"""
        initial_count = data["code"].n_unique()
        
        # 剔除停牌
        data = data.filter(pl.col("volume") > 0)
        
        # 剔除新股
        stock_counts = data.group_by("code").agg([pl.count().alias("days")])
        valid_stocks = stock_counts.filter(pl.col("days") >= 252)["code"].to_list()
        data = data.filter(pl.col("code").is_in(valid_stocks))
        
        final_count = data["code"].n_unique()
        logger.info(f"过滤后: {final_count} 只股票 (剔除 {initial_count - final_count})")
        
        return data
    
    def calculate_all_factors(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算所有因子"""
        # 1. 技术面因子
        registry = FactorRegistry()
        factor_classes = registry.list_all()
        
        logger.info(f"计算 {len(factor_classes)} 个技术面因子...")
        for name, factor_class in factor_classes.items():
            try:
                factor = factor_class()
                data = factor.calculate(data)
            except:
                pass
        
        # 2. 基本面因子
        data = self.fundamental_calc.calculate_fundamental_factors(data)
        
        # 填充缺失值
        factor_cols = [c for c in data.columns if c.startswith("factor_")]
        for col in factor_cols:
            data = data.with_columns([
                pl.when(pl.col(col).is_nan() | pl.col(col).is_null())
                .then(0)
                .otherwise(pl.col(col))
                .alias(col)
            ])
        
        return data
    
    def calculate_forward_returns(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算未来收益"""
        for period in [1, 5, 10, 20]:
            data = data.with_columns([
                pl.col("close").shift(-period).over("code").alias(f"future_{period}d")
            ])
            data = data.with_columns([
                pl.when(pl.col(f"future_{period}d").is_null())
                .then(None)
                .otherwise((pl.col(f"future_{period}d") - pl.col("close")) / pl.col("close"))
                .alias(f"forward_return_{period}d")
            ])
        return data
    
    def run_full_analysis(self, data: pl.DataFrame) -> Dict:
        """运行完整分析"""
        results = {}
        
        # 获取所有因子
        factor_cols = [c for c in data.columns if c.startswith("factor_")]
        logger.info(f"分析 {len(factor_cols)} 个因子")
        
        # 1. 单因子分析
        for i, factor_col in enumerate(factor_cols):
            if i % 10 == 0:
                logger.info(f"[{i}/{len(factor_cols)}] 分析 {factor_col}")
            
            try:
                metrics = self.analyzer.analyze_factor(data, factor_col, "forward_return_5d")
                results[factor_col] = {
                    'ic_mean': metrics.ic_mean,
                    'ic_ir': metrics.ic_ir,
                    'long_short_return': metrics.long_short_return,
                    'p_value': metrics.p_value
                }
            except:
                pass
        
        # 2. XGBoost组合
        logger.info("训练XGBoost组合模型...")
        self.xgboost_combiner.train(data, factor_cols)
        data = self.xgboost_combiner.predict(data, factor_cols)
        
        try:
            metrics = self.analyzer.analyze_factor(data, "factor_xgboost_combined", "forward_return_5d")
            results['xgboost_combined'] = {
                'ic_mean': metrics.ic_mean,
                'ic_ir': metrics.ic_ir,
                'long_short_return': metrics.long_short_return,
                'p_value': metrics.p_value
            }
        except:
            pass
        
        # 3. 动态权重组合
        logger.info("计算动态权重...")
        dynamic_weights = self.dynamic_adjuster.calculate_dynamic_weights(data, factor_cols)
        data = self.dynamic_adjuster.apply_dynamic_weights(data, dynamic_weights)
        
        try:
            metrics = self.analyzer.analyze_factor(data, "factor_dynamic_weighted", "forward_return_5d")
            results['dynamic_weighted'] = {
                'ic_mean': metrics.ic_mean,
                'ic_ir': metrics.ic_ir,
                'long_short_return': metrics.long_short_return,
                'p_value': metrics.p_value
            }
        except:
            pass
        
        return results


def print_v2_report(results: Dict):
    """打印V2报告"""
    print("\n" + "=" * 120)
    print("因子分析V2 - 全面优化报告")
    print("=" * 120)
    
    # 按IC_IR排序
    sorted_results = sorted(results.items(), key=lambda x: abs(x[1].get('ic_ir', 0)), reverse=True)
    
    print(f"\n{'因子名称':<35} {'IC均值':>10} {'IC_IR':>10} {'多空收益':>12} {'p值':>10} {'有效':>6}")
    print("-" * 120)
    
    effective_count = 0
    for factor_name, metrics in sorted_results[:20]:
        ic_mean = metrics.get('ic_mean', 0)
        ic_ir = metrics.get('ic_ir', 0)
        ls_ret = metrics.get('long_short_return', 0)
        p_val = metrics.get('p_value', 1)
        
        is_effective = abs(ic_ir) >= 0.3 and abs(ic_mean) >= 0.02 and p_val <= 0.05
        if is_effective:
            effective_count += 1
        
        marker = ""
        if factor_name == "xgboost_combined":
            marker = "[XGB]"
        elif factor_name == "dynamic_weighted":
            marker = "[DYN]"
        
        print(f"{factor_name:<30}{marker:<5} {ic_mean:>10.4f} {ic_ir:>10.4f} "
              f"{ls_ret:>11.2%} {p_val:>10.4f} {'✓' if is_effective else '':>6}")
    
    print("=" * 120)
    print(f"\n有效因子: {effective_count} / {len(results)}")
    
    # 特别标记组合因子
    if 'xgboost_combined' in results:
        xgb = results['xgboost_combined']
        print(f"\n【XGBoost组合因子】")
        print(f"  IC均值: {xgb.get('ic_mean', 0):.4f}")
        print(f"  IC_IR: {xgb.get('ic_ir', 0):.4f}")
        print(f"  多空收益: {xgb.get('long_short_return', 0):.2%}")
    
    if 'dynamic_weighted' in results:
        dyn = results['dynamic_weighted']
        print(f"\n【动态权重组合因子】")
        print(f"  IC均值: {dyn.get('ic_mean', 0):.4f}")
        print(f"  IC_IR: {dyn.get('ic_ir', 0):.4f}")
        print(f"  多空收益: {dyn.get('long_short_return', 0):.2%}")


def main():
    parser = argparse.ArgumentParser(description='因子分析V2')
    parser.add_argument('--start-date', type=str, default='2024-01-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=None)
    parser.add_argument('--output', type=str, default='factor_analysis_v2.csv')
    
    args = parser.parse_args()
    
    print("=" * 120)
    print("因子分析V2 - 全面优化版")
    print("=" * 120)
    print(f"数据区间: {args.start_date} ~ {args.end_date}")
    print(f"优化特性:")
    print(f"  1. 全市场数据 (5000+股票)")
    print(f"  2. 基本面因子 (PE/PB/ROE代理)")
    print(f"  3. XGBoost机器学习组合")
    print(f"  4. 动态权重调整")
    print()
    
    # 初始化
    analysis = FactorAnalysisV2()
    
    # 1. 加载数据
    data = analysis.load_data(args.start_date, args.end_date, args.max_stocks)
    if len(data) == 0:
        logger.error("未加载到数据")
        return
    
    # 2. 过滤数据
    data = analysis.filter_data(data)
    
    # 3. 计算因子
    data = analysis.calculate_all_factors(data)
    
    # 4. 计算未来收益
    data = analysis.calculate_forward_returns(data)
    
    # 5. 完整分析
    results = analysis.run_full_analysis(data)
    
    # 6. 打印报告
    print_v2_report(results)
    
    # 7. 保存结果
    if results:
        output_df = pl.DataFrame([
            {'factor_name': k, **v} for k, v in results.items()
        ])
        output_path = PROJECT_ROOT / args.output
        output_df.write_csv(output_path)
        logger.info(f"结果已保存: {output_path}")


if __name__ == "__main__":
    main()
