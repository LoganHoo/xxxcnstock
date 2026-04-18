#!/usr/bin/env python3
"""
优化版因子分析流程
优化点：
1. 全市场数据（5000+股票）
2. 数据过滤（ST股、停牌、新股）
3. 因子中性化（市值、行业）
4. 多周期收益测试
5. 因子组合分析
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

from core.factor_library import FactorRegistry
import factors.technical
import factors.volume_price
import factors.market

from core.factor_analyzer import FactorAnalyzer, FactorMetrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataFilter:
    """数据过滤器"""
    
    def __init__(self):
        self.st_codes = set()  # ST股票代码
        self.new_stock_days = 252  # 新股定义：上市不满1年
        self.min_volume = 1_000_000  # 最小成交量（股）
        self.min_amount = 10_000_000  # 最小成交额（元）
    
    def filter_stocks(self, data: pl.DataFrame, stock_list_path: Optional[Path] = None) -> pl.DataFrame:
        """
        过滤低质量股票
        
        过滤条件：
        1. 剔除ST/*ST股票
        2. 剔除停牌股票（成交量为0）
        3. 剔除新股（上市不满1年）
        4. 剔除流动性差的股票（日均成交额<1000万）
        """
        initial_count = data["code"].n_unique()
        logger.info(f"过滤前股票数: {initial_count}")
        
        # 1. 剔除停牌（成交量为0或成交额过小）
        data = data.filter(
            (pl.col("volume") > 0) &
            (pl.col("volume") >= self.min_volume) &
            (pl.col("close") * pl.col("volume") >= self.min_amount)
        )
        
        after_volume = data["code"].n_unique()
        logger.info(f"剔除停牌/低流动性后: {after_volume}")
        
        # 2. 剔除新股（数据量不足252个交易日）
        stock_counts = data.group_by("code").agg([
            pl.count().alias("trade_days")
        ])
        valid_stocks = stock_counts.filter(pl.col("trade_days") >= self.new_stock_days)["code"].to_list()
        data = data.filter(pl.col("code").is_in(valid_stocks))
        
        after_new = data["code"].n_unique()
        logger.info(f"剔除新股后: {after_new}")
        
        # 3. 剔除ST股票（从名称判断）
        # 如果有stock_list文件，读取ST状态
        if stock_list_path and stock_list_path.exists():
            try:
                stock_list = pl.read_parquet(stock_list_path)
                if "name" in stock_list.columns:
                    st_mask = stock_list["name"].str.contains("ST|退市")
                    st_codes = stock_list.filter(st_mask)["code"].to_list()
                    data = data.filter(~pl.col("code").is_in(st_codes))
                    logger.info(f"剔除ST股票后: {data['code'].n_unique()}")
            except Exception as e:
                logger.warning(f"读取stock_list失败: {e}")
        
        final_count = data["code"].n_unique()
        logger.info(f"过滤后股票数: {final_count} (剔除 {initial_count - final_count} 只)")
        
        return data


class FactorNeutralizer:
    """因子中性化处理器"""
    
    def __init__(self):
        self.analyzer = FactorAnalyzer()
    
    def add_market_cap(self, data: pl.DataFrame) -> pl.DataFrame:
        """添加市值代理变量（收盘价*成交量）"""
        return data.with_columns([
            (pl.col("close") * pl.col("volume")).alias("market_cap_proxy")
        ])
    
    def neutralize_factor(
        self, 
        data: pl.DataFrame, 
        factor_col: str,
        neutralize_cols: List[str] = ["market_cap_proxy"]
    ) -> pl.DataFrame:
        """
        对因子进行中性化处理
        
        方法：回归残差法
        factor = β0 + β1*市值 + β2*行业 + ε
        使用残差 ε 作为中性化后的因子
        """
        from sklearn.linear_model import LinearRegression
        
        neutralized_factor = f"{factor_col}_neutral"
        residuals_list = []
        
        for date, group in data.group_by("trade_date"):
            factor_values = group[factor_col].to_numpy()
            
            # 构建特征矩阵
            X = np.column_stack([
                group[col].to_numpy() for col in neutralize_cols
            ])
            
            # 去除NaN
            valid_mask = ~(np.isnan(factor_values) | np.any(np.isnan(X), axis=1))
            factor_values = factor_values[valid_mask]
            X = X[valid_mask]
            
            if len(factor_values) < 20:
                continue
            
            # 回归
            model = LinearRegression()
            model.fit(X, factor_values)
            residuals = factor_values - model.predict(X)
            
            # 更新数据
            group_df = group.filter(pl.Series(valid_mask))
            group_df = group_df.with_columns([
                pl.Series(residuals).alias(neutralized_factor)
            ])
            residuals_list.append(group_df)
        
        if residuals_list:
            data = pl.concat(residuals_list)
        
        return data


class OptimizedFactorAnalyzer:
    """优化版因子分析器"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.analyzer = FactorAnalyzer()
        self.data_filter = DataFilter()
        self.neutralizer = FactorNeutralizer()
    
    def load_data(
        self, 
        start_date: str, 
        end_date: str, 
        max_stocks: Optional[int] = None,
        apply_filter: bool = True
    ) -> pl.DataFrame:
        """加载并预处理数据"""
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
                
                # 过滤日期
                df = df.filter(
                    (pl.col("trade_date") >= start_date) &
                    (pl.col("trade_date") <= end_date)
                )
                
                if len(df) > 0:
                    all_data.append(df)
                    count += 1
                    
                    if count % 500 == 0:
                        logger.info(f"已加载 {count} 只股票...")
                        
            except Exception as e:
                logger.debug(f"加载失败 {parquet_file}: {e}")
        
        if not all_data:
            return pl.DataFrame()
        
        data = pl.concat(all_data)
        logger.info(f"原始数据: {len(data)} 条, {data['code'].n_unique()} 只股票")
        
        # 应用过滤
        if apply_filter:
            stock_list_path = PROJECT_ROOT / "data" / "stock_list.parquet"
            data = self.data_filter.filter_stocks(data, stock_list_path)
        
        return data
    
    def calculate_all_factors(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算所有因子"""
        registry = FactorRegistry()
        factor_classes = registry.list_all()
        
        logger.info(f"计算 {len(factor_classes)} 个因子...")
        
        success_count = 0
        for name, factor_class in factor_classes.items():
            try:
                factor = factor_class()
                data = factor.calculate(data)
                success_count += 1
            except Exception as e:
                logger.debug(f"计算失败 {name}: {e}")
        
        logger.info(f"因子计算完成: {success_count}/{len(factor_classes)}")
        
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
    
    def calculate_forward_returns(
        self, 
        data: pl.DataFrame, 
        periods: List[int] = [1, 5, 10, 20]
    ) -> pl.DataFrame:
        """计算多周期未来收益率"""
        logger.info(f"计算未来收益率: {periods} 天")
        
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
        self,
        data: pl.DataFrame,
        factor_col: str,
        periods: List[int] = [1, 5, 10, 20]
    ) -> Dict:
        """多周期因子分析"""
        results = {}
        
        for period in periods:
            return_col = f"forward_return_{period}d"
            if return_col not in data.columns:
                continue
            
            valid_data = data.filter(pl.col(return_col).is_not_null())
            if len(valid_data) < 1000:
                continue
            
            try:
                metrics = self.analyzer.analyze_factor(valid_data, factor_col, return_col)
                results[f"{period}d"] = {
                    'ic_mean': metrics.ic_mean,
                    'ic_std': metrics.ic_std,
                    'ic_ir': metrics.ic_ir,
                    'ic_positive_ratio': metrics.ic_positive_ratio,
                    'long_short_return': metrics.long_short_return,
                    'long_short_sharpe': metrics.long_short_sharpe,
                    'p_value': metrics.p_value
                }
            except Exception as e:
                logger.debug(f"分析失败 {factor_col} {period}d: {e}")
        
        return results
    
    def analyze_factor_combinations(
        self,
        data: pl.DataFrame,
        factor_cols: List[str],
        top_n: int = 5
    ) -> Dict:
        """分析因子组合效果"""
        logger.info("分析因子组合...")
        
        # 选择IC最高的top_n个因子
        ic_scores = []
        for col in factor_cols:
            try:
                ic_df = self.analyzer.calculate_ic(data, col, "forward_return_5d")
                ic_values = ic_df['ic'].drop_nulls().to_numpy()
                if len(ic_values) > 0:
                    ic_scores.append((col, np.mean(np.abs(ic_values))))
            except:
                pass
        
        ic_scores.sort(key=lambda x: x[1], reverse=True)
        top_factors = [f[0] for f in ic_scores[:top_n]]
        
        logger.info(f"选择Top {len(top_factors)} 因子: {top_factors}")
        
        # 等权组合
        combined = pl.lit(0.0)
        for f in top_factors:
            combined = combined + pl.col(f)
        data = data.with_columns([
            (combined / len(top_factors)).alias("combined_factor")
        ])
        
        # 分析组合因子
        try:
            metrics = self.analyzer.analyze_factor(data, "combined_factor", "forward_return_5d")
            return {
                'factors': top_factors,
                'ic_mean': metrics.ic_mean,
                'ic_ir': metrics.ic_ir,
                'long_short_return': metrics.long_short_return,
                'p_value': metrics.p_value
            }
        except Exception as e:
            logger.error(f"组合分析失败: {e}")
            return {}


def print_optimized_report(all_results: Dict):
    """打印优化版报告"""
    print("\n" + "=" * 130)
    print("优化版因子有效性分析报告")
    print("=" * 130)
    
    # 1. 多周期表现
    print("\n【多周期因子表现】（按IC_IR排序）")
    print(f"{'因子名称':<30} {'1天IC/IR':>15} {'5天IC/IR':>15} {'10天IC/IR':>15} {'20天IC/IR':>15}")
    print("-" * 130)
    
    # 按5天IC_IR排序
    sorted_factors = sorted(
        all_results.items(),
        key=lambda x: x[1].get('5d', {}).get('ic_ir', 0),
        reverse=True
    )
    
    for factor_name, period_results in sorted_factors[:15]:  # 显示前15
        if factor_name.startswith('_'):
            continue
        
        line = f"{factor_name:<30}"
        for period in ['1d', '5d', '10d', '20d']:
            if period in period_results:
                r = period_results[period]
                ic = r['ic_mean']
                ir = r['ic_ir']
                # 标记有效因子
                mark = "✓" if abs(ir) >= 0.3 and abs(ic) >= 0.02 and r['p_value'] <= 0.05 else ""
                line += f" {ic:>6.3f}/{ir:>5.2f}{mark:<2}"
            else:
                line += "       -/-      "
        print(line)
    
    # 2. 有效因子统计
    effective_factors = []
    for factor_name, period_results in all_results.items():
        if factor_name.startswith('_'):
            continue
        for period, metrics in period_results.items():
            if period.endswith('d') and isinstance(metrics, dict):
                if (abs(metrics.get('ic_ir', 0)) >= 0.3 and 
                    abs(metrics.get('ic_mean', 0)) >= 0.02 and
                    metrics.get('p_value', 1) <= 0.05):
                    effective_factors.append({
                        'name': factor_name,
                        'period': period,
                        'ic_ir': metrics['ic_ir'],
                        'ic_mean': metrics['ic_mean']
                    })
    
    print(f"\n【有效因子统计】")
    print(f"有效因子数量: {len(effective_factors)}")
    if effective_factors:
        print("\n有效因子列表:")
        for ef in sorted(effective_factors, key=lambda x: abs(x['ic_ir']), reverse=True):
            print(f"  ✓ {ef['name']:<30} {ef['period']:>5} IC_IR={ef['ic_ir']:>6.3f} IC={ef['ic_mean']:>7.4f}")
    
    # 3. 组合因子
    if '_combined' in all_results:
        print(f"\n【因子组合表现】")
        combo = all_results['_combined']
        print(f"组合因子: {', '.join(combo.get('factors', []))}")
        print(f"IC均值: {combo.get('ic_mean', 0):.4f}")
        print(f"IC_IR: {combo.get('ic_ir', 0):.4f}")
        print(f"多空收益: {combo.get('long_short_return', 0):.2%}")
        print(f"p值: {combo.get('p_value', 1):.4f}")
    
    print("=" * 130)


def main():
    parser = argparse.ArgumentParser(description='优化版因子分析工具')
    parser.add_argument('--start-date', type=str, default='2024-01-01')
    parser.add_argument('--end-date', type=str, default='2025-04-13')
    parser.add_argument('--max-stocks', type=int, default=None,
                       help='最大股票数量（默认全市场）')
    parser.add_argument('--no-filter', action='store_true',
                       help='禁用数据过滤')
    parser.add_argument('--neutralize', action='store_true',
                       help='启用因子中性化')
    parser.add_argument('--output', type=str, default='factor_analysis_optimized.csv')
    
    args = parser.parse_args()
    
    print("=" * 130)
    print("优化版因子分析流程")
    print("=" * 130)
    print(f"数据区间: {args.start_date} ~ {args.end_date}")
    print(f"数据过滤: {'禁用' if args.no_filter else '启用'}")
    print(f"因子中性化: {'启用' if args.neutralize else '禁用'}")
    print()
    
    # 初始化分析器
    opt_analyzer = OptimizedFactorAnalyzer()
    
    # 1. 加载数据
    data = opt_analyzer.load_data(
        args.start_date, 
        args.end_date,
        max_stocks=args.max_stocks,
        apply_filter=not args.no_filter
    )
    
    if len(data) == 0:
        logger.error("未加载到数据")
        return
    
    logger.info(f"总数据量: {len(data)} 条")
    
    # 2. 计算因子
    data = opt_analyzer.calculate_all_factors(data)
    
    # 3. 因子中性化
    if args.neutralize:
        logger.info("进行因子中性化...")
        data = opt_analyzer.neutralizer.add_market_cap(data)
        factor_cols = [col for col in data.columns if col.startswith("factor_")]
        for col in factor_cols[:5]:  # 只中性化前5个因子以加快速度
            data = opt_analyzer.neutralizer.neutralize_factor(data, col)
    
    # 4. 计算未来收益
    data = opt_analyzer.calculate_forward_returns(data, periods=[1, 5, 10, 20])
    
    # 5. 获取因子列
    factor_cols = [col for col in data.columns if col.startswith("factor_")]
    logger.info(f"分析 {len(factor_cols)} 个因子")
    
    # 6. 多周期分析
    all_results = {}
    for i, factor_col in enumerate(factor_cols):
        logger.info(f"[{i+1}/{len(factor_cols)}] 分析因子: {factor_col}")
        
        # 使用中性化后的因子（如果存在）
        if args.neutralize and f"{factor_col}_neutral" in data.columns:
            factor_col = f"{factor_col}_neutral"
        
        multi_period = opt_analyzer.analyze_factor_multi_period(data, factor_col)
        all_results[factor_col] = multi_period
    
    # 7. 因子组合分析
    combo_result = opt_analyzer.analyze_factor_combinations(data, factor_cols)
    if combo_result:
        all_results['_combined'] = combo_result
    
    # 8. 打印报告
    print_optimized_report(all_results)
    
    # 9. 保存结果
    output_rows = []
    for factor_name, period_results in all_results.items():
        if factor_name.startswith('_'):
            continue
        for period, metrics in period_results.items():
            if period.endswith('d') and isinstance(metrics, dict):
                row = {
                    'factor_name': factor_name,
                    'period': period,
                    **metrics
                }
                output_rows.append(row)
    
    if output_rows:
        output_df = pl.DataFrame(output_rows)
        output_path = PROJECT_ROOT / args.output
        output_df.write_csv(output_path)
        logger.info(f"结果已保存: {output_path}")


if __name__ == "__main__":
    main()
