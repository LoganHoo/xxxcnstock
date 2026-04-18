#!/usr/bin/env python3
"""
因子分析V3 - 终极优化版
优化点：
1. 全市场5000+股票数据
2. 真实基本面数据（从akshare获取PE/PB/ROE）
3. 滚动窗口动态权重
4. 基于XGBoost的选股策略构建
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


class FundamentalDataFetcher:
    """基本面数据获取器"""
    
    def __init__(self):
        self.fundamental_cache = {}
        
    def fetch_fundamental_data(self, trade_date: str) -> Optional[pl.DataFrame]:
        """
        从akshare获取基本面数据
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)
        
        Returns:
            包含PE/PB/ROE的基本面数据
        """
        try:
            import akshare as ak
        except ImportError:
            logger.warning("akshare未安装，使用代理基本面因子")
            return None
        
        # 转换日期格式
        date_str = trade_date.replace('-', '')
        
        try:
            # 获取A股估值数据
            df = ak.stock_a_pe(symbol="全部A股")
            
            if df is None or len(df) == 0:
                return None
            
            # 转换列名
            df = df.rename(columns={
                '代码': 'code',
                '名称': 'name',
                '市盈率': 'pe',
                '市净率': 'pb',
                'ROE': 'roe'
            })
            
            # 添加日期
            df['trade_date'] = trade_date
            
            # 转换为polars
            return pl.from_pandas(df)
            
        except Exception as e:
            logger.debug(f"获取基本面数据失败 {trade_date}: {e}")
            return None
    
    def merge_fundamental_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """将基本面数据合并到K线数据"""
        logger.info("获取真实基本面数据...")
        
        # 获取唯一日期
        dates = data["trade_date"].unique().to_list()
        
        all_fundamental = []
        for date in dates:
            fd = self.fetch_fundamental_data(date)
            if fd is not None:
                all_fundamental.append(fd)
        
        if not all_fundamental:
            logger.warning("未获取到基本面数据，使用代理因子")
            return self._add_proxy_fundamental_factors(data)
        
        # 合并所有日期的基本面数据
        fundamental_df = pl.concat(all_fundamental)
        
        # 左连接
        data = data.join(
            fundamental_df[["code", "trade_date", "pe", "pb", "roe"]],
            on=["code", "trade_date"],
            how="left"
        )
        
        # 计算基本面因子
        data = data.with_columns([
            (1.0 / (pl.col("pe") + 0.001)).alias("factor_ep"),  # 盈利收益率
            (1.0 / (pl.col("pb") + 0.001)).alias("factor_bp"),  # 账面市值比
            pl.col("roe").alias("factor_roe_real")  # ROE
        ])
        
        # 填充缺失值
        for col in ["factor_ep", "factor_bp", "factor_roe_real"]:
            data = data.with_columns([
                pl.col(col).fill_null(0).alias(col)
            ])
        
        logger.info("真实基本面数据合并完成")
        return data
    
    def _add_proxy_fundamental_factors(self, data: pl.DataFrame) -> pl.DataFrame:
        """添加代理基本面因子（当无法获取真实数据时）"""
        logger.info("使用代理基本面因子...")
        
        # EP代理
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=20).over("code").alias("ma20"),
            pl.col("close").rolling_std(window_size=20).over("code").alias("vol20")
        ])
        
        data = data.with_columns([
            (pl.col("ma20") / (pl.col("vol20") + 0.001)).alias("factor_ep_proxy")
        ])
        
        # BP代理
        data = data.with_columns([
            (pl.col("close") / pl.col("ma20")).alias("factor_bp_proxy")
        ])
        
        # ROE代理
        data = data.with_columns([
            pl.col("close").pct_change().over("code").alias("ret")
        ])
        
        data = data.with_columns([
            pl.col("ret").rolling_mean(window_size=60).over("code").alias("ret_mean"),
            pl.col("ret").rolling_std(window_size=60).over("code").alias("ret_std")
        ])
        
        data = data.with_columns([
            (pl.col("ret_mean") / (pl.col("ret_std") + 0.001)).alias("factor_roe_proxy")
        ])
        
        # 清理临时列
        data = data.drop(["ma20", "vol20", "ret", "ret_mean", "ret_std"])
        
        return data


class RollingDynamicWeightAdjuster:
    """滚动窗口动态权重调整器"""
    
    def __init__(self, lookback_window: int = 60):
        self.lookback_window = lookback_window
        self.rolling_ic = {}
        
    def calculate_rolling_ic(
        self, 
        data: pl.DataFrame, 
        factor_col: str,
        return_col: str = "forward_return_5d"
    ) -> List[float]:
        """计算滚动IC序列"""
        from scipy import stats
        
        dates = sorted(data["trade_date"].unique().to_list())
        ic_series = []
        
        for i in range(self.lookback_window, len(dates)):
            window_dates = dates[i-self.lookback_window:i]
            window_data = data.filter(pl.col("trade_date").is_in(window_dates))
            
            factor_values = window_data[factor_col].to_numpy()
            returns = window_data[return_col].to_numpy()
            
            mask = ~(np.isnan(factor_values) | np.isnan(returns) | 
                    np.isinf(factor_values) | np.isinf(returns))
            factor_values = factor_values[mask]
            returns = returns[mask]
            
            if len(factor_values) < 100:
                ic_series.append(0)
                continue
            
            try:
                ic, _ = stats.spearmanr(factor_values, returns)
                ic_series.append(ic if not np.isnan(ic) else 0)
            except:
                ic_series.append(0)
        
        return ic_series
    
    def calculate_dynamic_weights(
        self, 
        data: pl.DataFrame, 
        factor_cols: List[str],
        return_col: str = "forward_return_5d"
    ) -> Dict[str, float]:
        """计算基于滚动IC的动态权重"""
        logger.info("计算滚动窗口动态权重...")
        
        ic_scores = []
        
        for factor_col in factor_cols:
            ic_series = self.calculate_rolling_ic(data, factor_col, return_col)
            
            if len(ic_series) == 0:
                continue
            
            # 使用IC均值作为权重基础
            ic_mean = np.mean(np.abs(ic_series))
            ic_std = np.std(ic_series)
            
            # IC_IR加权
            ic_ir = ic_mean / (ic_std + 0.001)
            ic_scores.append((factor_col, ic_ir))
        
        if not ic_scores:
            # 等权
            n = len(factor_cols)
            return {col: 1.0/n for col in factor_cols}
        
        # 基于IC_IR计算权重
        total_ic_ir = sum(score for _, score in ic_scores)
        if total_ic_ir == 0:
            n = len(ic_scores)
            return {col: 1.0/n for col, _ in ic_scores}
        
        weights = {col: max(0, score)/total_ic_ir for col, score in ic_scores}
        
        logger.info("动态权重Top5:")
        for factor, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  {factor}: {weight:.4f}")
        
        return weights


class XGBoostStockStrategy:
    """基于XGBoost的选股策略"""
    
    def __init__(self):
        self.model = None
        self.feature_cols = []
        
    def train_model(
        self, 
        data: pl.DataFrame, 
        factor_cols: List[str],
        target_col: str = "forward_return_5d"
    ):
        """训练XGBoost选股模型"""
        try:
            import xgboost as xgb
        except ImportError:
            logger.error("XGBoost未安装")
            return False
        
        logger.info("训练XGBoost选股模型...")
        
        # 准备数据
        valid_data = data.filter(pl.col(target_col).is_not_null())
        
        # 特征工程
        X = valid_data[factor_cols].to_numpy()
        y = valid_data[target_col].to_numpy()
        
        # 清理数据
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isinf(X).any(axis=1) | 
                      np.isnan(y) | np.isinf(y))
        X = X[valid_mask]
        y = y[valid_mask]
        
        if len(X) < 1000:
            logger.warning("训练数据不足")
            return False
        
        # 转换为排名（分位数回归）
        y_rank = np.argsort(np.argsort(y)) / len(y)
        
        # 训练模型
        self.model = xgb.XGBRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            n_jobs=-1
        )
        
        self.model.fit(X, y_rank)
        self.feature_cols = factor_cols
        
        # 特征重要性
        importance = self.model.feature_importances_
        feature_imp = sorted(
            [(factor_cols[i], importance[i]) for i in range(len(factor_cols))],
            key=lambda x: x[1],
            reverse=True
        )
        
        logger.info("特征重要性Top10:")
        for factor, imp in feature_imp[:10]:
            logger.info(f"  {factor}: {imp:.4f}")
        
        return True
    
    def predict_scores(self, data: pl.DataFrame) -> pl.DataFrame:
        """预测选股得分"""
        if self.model is None:
            return data
        
        X = data[self.feature_cols].to_numpy()
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        scores = self.model.predict(X)
        
        return data.with_columns([
            pl.Series(scores).alias("xgboost_score")
        ])
    
    def select_stocks(
        self, 
        data: pl.DataFrame, 
        date: str,
        top_n: int = 20
    ) -> List[Dict]:
        """选股"""
        date_data = data.filter(pl.col("trade_date") == date)
        
        if len(date_data) == 0:
            return []
        
        # 预测得分
        date_data = self.predict_scores(date_data)
        
        # 排序并选择Top N
        selected = date_data.sort("xgboost_score", descending=True).head(top_n)
        
        return selected[["code", "xgboost_score", "close"]].to_dicts()


class FactorAnalysisV3:
    """因子分析V3主类"""
    
    def __init__(self):
        self.analyzer = FactorAnalyzer()
        self.fundamental_fetcher = FundamentalDataFetcher()
        self.rolling_adjuster = RollingDynamicWeightAdjuster()
        self.xgb_strategy = XGBoostStockStrategy()
    
    def load_all_market_data(
        self, 
        start_date: str, 
        end_date: str
    ) -> pl.DataFrame:
        """加载全市场数据"""
        data_path = PROJECT_ROOT / "data" / "kline"
        
        if not data_path.exists():
            logger.error(f"数据目录不存在: {data_path}")
            return pl.DataFrame()
        
        all_data = []
        parquet_files = list(data_path.glob("*.parquet"))
        logger.info(f"发现 {len(parquet_files)} 只股票数据文件")
        
        for i, parquet_file in enumerate(parquet_files):
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
                
                if (i + 1) % 1000 == 0:
                    logger.info(f"已加载 {i+1} 只股票...")
                    
            except Exception as e:
                pass
        
        if not all_data:
            return pl.DataFrame()
        
        data = pl.concat(all_data)
        logger.info(f"总数据: {len(data)} 条, {data['code'].n_unique()} 只股票")
        
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
        
        # 2. 基本面因子（真实数据或代理）
        data = self.fundamental_fetcher.merge_fundamental_data(data)
        
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
        
        # 2. XGBoost模型训练
        logger.info("训练XGBoost选股模型...")
        self.xgb_strategy.train_model(data, factor_cols)
        
        # 3. 滚动动态权重
        logger.info("计算滚动动态权重...")
        dynamic_weights = self.rolling_adjuster.calculate_dynamic_weights(data, factor_cols)
        
        return results, dynamic_weights


def print_v3_report(results: Dict, weights: Dict):
    """打印V3报告"""
    print("\n" + "=" * 130)
    print("因子分析V3 - 终极优化报告")
    print("=" * 130)
    
    sorted_results = sorted(results.items(), key=lambda x: abs(x[1].get('ic_ir', 0)), reverse=True)
    
    print(f"\n{'因子名称':<35} {'IC均值':>10} {'IC_IR':>10} {'多空收益':>12} {'p值':>10} {'有效':>6}")
    print("-" * 130)
    
    effective_count = 0
    for factor_name, metrics in sorted_results[:20]:
        ic_mean = metrics.get('ic_mean', 0)
        ic_ir = metrics.get('ic_ir', 0)
        ls_ret = metrics.get('long_short_return', 0)
        p_val = metrics.get('p_value', 1)
        
        is_effective = abs(ic_ir) >= 0.3 and abs(ic_mean) >= 0.02 and p_val <= 0.05
        if is_effective:
            effective_count += 1
        
        print(f"{factor_name:<35} {ic_mean:>10.4f} {ic_ir:>10.4f} "
              f"{ls_ret:>11.2%} {p_val:>10.4f} {'✓' if is_effective else '':>6}")
    
    print("=" * 130)
    print(f"\n有效因子: {effective_count} / {len(results)}")
    
    # 动态权重Top5
    print(f"\n【动态权重Top5】")
    for factor, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {factor}: {weight:.4f}")


def main():
    parser = argparse.ArgumentParser(description='因子分析V3 - 终极优化版')
    parser.add_argument('--start-date', type=str, default='2024-01-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=None,
                       help='限制股票数量（默认全市场）')
    parser.add_argument('--output', type=str, default='factor_analysis_v3.csv')
    
    args = parser.parse_args()
    
    print("=" * 130)
    print("因子分析V3 - 终极优化版")
    print("=" * 130)
    print(f"数据区间: {args.start_date} ~ {args.end_date}")
    print(f"优化特性:")
    print(f"  1. 全市场5000+股票数据")
    print(f"  2. 真实基本面数据（PE/PB/ROE）")
    print(f"  3. 滚动窗口动态权重")
    print(f"  4. XGBoost选股策略")
    print()
    
    analysis = FactorAnalysisV3()
    
    # 1. 加载全市场数据
    if args.max_stocks:
        logger.info(f"限制加载 {args.max_stocks} 只股票")
        data = analysis.load_all_market_data(args.start_date, args.end_date)
        data = data.filter(pl.col("code").is_in(data["code"].unique().to_list()[:args.max_stocks]))
    else:
        data = analysis.load_all_market_data(args.start_date, args.end_date)
    
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
    results, weights = analysis.run_full_analysis(data)
    
    # 6. 打印报告
    print_v3_report(results, weights)
    
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
