#!/usr/bin/env python3
"""
因子有效性分析器
用于测试因子的预测能力、Alpha收益、IC值等

核心指标:
1. IC (Information Coefficient) - 信息系数
2. IR (Information Ratio) - 信息比率
3. Alpha - 超额收益
4. 因子收益率 - 多空组合收益
5. 换手率 - 因子稳定性
"""
import polars as pl
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class FactorMetrics:
    """因子有效性指标"""
    factor_name: str
    
    # IC指标
    ic_mean: float          # IC均值
    ic_std: float           # IC标准差
    ic_ir: float            # IC信息比率
    ic_positive_ratio: float  # IC为正的比例
    
    # 收益率指标
    long_short_return: float    # 多空组合年化收益
    long_short_sharpe: float    # 多空组合夏普比率
    top_quantile_return: float  # 头部组合收益
    bottom_quantile_return: float  # 尾部组合收益
    
    # 稳定性指标
    turnover: float         # 换手率
    max_drawdown: float     # 最大回撤
    
    # 显著性
    t_statistic: float      # t统计量
    p_value: float          # p值
    
    def to_dict(self) -> Dict:
        return {
            'factor_name': self.factor_name,
            'ic_mean': self.ic_mean,
            'ic_std': self.ic_std,
            'ic_ir': self.ic_ir,
            'ic_positive_ratio': self.ic_positive_ratio,
            'long_short_return': self.long_short_return,
            'long_short_sharpe': self.long_short_sharpe,
            'top_quantile_return': self.top_quantile_return,
            'bottom_quantile_return': self.bottom_quantile_return,
            'turnover': self.turnover,
            'max_drawdown': self.max_drawdown,
            't_statistic': self.t_statistic,
            'p_value': self.p_value,
        }


class FactorAnalyzer:
    """因子分析器"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.logger = logging.getLogger(__name__)
    
    def calculate_ic(
        self,
        data: pl.DataFrame,
        factor_col: str,
        forward_return_col: str = "forward_return_1d",
        method: str = "spearman"
    ) -> pl.DataFrame:
        """
        计算因子的IC值（信息系数）
        
        Args:
            data: 包含因子值和收益率的数据
            factor_col: 因子列名
            forward_return_col: 未来收益率列名
            method: 相关系数方法 (spearman/pearson)
        
        Returns:
            每日IC值DataFrame
        """
        from scipy import stats
        
        def calc_ic(group):
            factor_values = group[factor_col].to_numpy()
            returns = group[forward_return_col].to_numpy()
            
            # 去除NaN和inf
            mask = ~(np.isnan(factor_values) | np.isnan(returns) | 
                     np.isinf(factor_values) | np.isinf(returns))
            factor_values = factor_values[mask]
            returns = returns[mask]
            
            if len(factor_values) < 10:
                return {'ic': np.nan, 'p_value': np.nan}
            
            # 检查常数输入
            if np.std(factor_values) == 0 or np.std(returns) == 0:
                return {'ic': np.nan, 'p_value': np.nan}
            
            try:
                if method == "spearman":
                    ic, p_value = stats.spearmanr(factor_values, returns)
                else:
                    ic, p_value = stats.pearsonr(factor_values, returns)
            except Exception:
                return {'ic': np.nan, 'p_value': np.nan}
            
            return {'ic': ic, 'p_value': p_value}
        
        # 按日期分组计算IC
        ic_results = []
        for date, group in data.group_by("trade_date"):
            result = calc_ic(group)
            ic_results.append({
                'trade_date': str(date),
                'ic': float(result['ic']) if not np.isnan(result['ic']) else None,
                'p_value': float(result['p_value']) if not np.isnan(result['p_value']) else None
            })
        
        if not ic_results:
            return pl.DataFrame({'trade_date': [], 'ic': [], 'p_value': []})
        
        return pl.DataFrame(ic_results)
    
    def analyze_factor_ic(
        self,
        data: pl.DataFrame,
        factor_col: str,
        forward_return_col: str = "forward_return_1d"
    ) -> Dict[str, float]:
        """
        分析因子的IC指标
        
        Returns:
            IC统计指标字典
        """
        ic_df = self.calculate_ic(data, factor_col, forward_return_col)
        ic_values = ic_df['ic'].drop_nulls().to_numpy()
        
        if len(ic_values) == 0:
            return {
                'ic_mean': 0,
                'ic_std': 0,
                'ic_ir': 0,
                'ic_positive_ratio': 0,
            }
        
        ic_mean = np.mean(ic_values)
        ic_std = np.std(ic_values)
        ic_ir = ic_mean / ic_std if ic_std > 0 else 0
        ic_positive_ratio = np.sum(ic_values > 0) / len(ic_values)
        
        return {
            'ic_mean': ic_mean,
            'ic_std': ic_std,
            'ic_ir': ic_ir,
            'ic_positive_ratio': ic_positive_ratio,
        }
    
    def calculate_quantile_returns(
        self,
        data: pl.DataFrame,
        factor_col: str,
        forward_return_col: str = "forward_return_1d",
        n_quantiles: int = 5
    ) -> pl.DataFrame:
        """
        计算因子分位数组合收益
        
        Args:
            data: 包含因子值的数据
            factor_col: 因子列名
            forward_return_col: 未来收益率列名
            n_quantiles: 分位数数量
        
        Returns:
            各分位数收益DataFrame
        """
        results = []
        
        for date, group in data.group_by("trade_date"):
            # 计算分位数
            factor_values = group[factor_col].to_numpy()
            
            if len(factor_values) < n_quantiles * 10:
                continue
            
            # 添加分位数标签
            quantiles = np.percentile(factor_values, 
                                     np.linspace(0, 100, n_quantiles + 1))
            
            for i in range(n_quantiles):
                if i == 0:
                    mask = factor_values <= quantiles[i + 1]
                elif i == n_quantiles - 1:
                    mask = factor_values > quantiles[i]
                else:
                    mask = (factor_values > quantiles[i]) & (factor_values <= quantiles[i + 1])
                
                group_df = group.filter(pl.col(factor_col).is_between(
                    quantiles[i] if i > 0 else float('-inf'),
                    quantiles[i + 1] if i < n_quantiles - 1 else float('inf')
                ))
                
                if len(group_df) > 0:
                    avg_return = group_df[forward_return_col].mean()
                    results.append({
                        'trade_date': date,
                        'quantile': i + 1,
                        'avg_return': avg_return,
                        'count': len(group_df)
                    })
        
        return pl.DataFrame(results)
    
    def calculate_long_short_return(
        self,
        data: pl.DataFrame,
        factor_col: str,
        forward_return_col: str = "forward_return_1d",
        top_pct: float = 0.2,
        bottom_pct: float = 0.2
    ) -> Dict[str, float]:
        """
        计算多空组合收益
        
        Args:
            data: 包含因子值的数据
            factor_col: 因子列名
            forward_return_col: 未来收益率列名
            top_pct: 头部百分比
            bottom_pct: 尾部百分比
        
        Returns:
            多空组合收益指标
        """
        long_short_returns = []
        
        for date, group in data.group_by("trade_date"):
            factor_values = group[factor_col].to_numpy()
            
            if len(factor_values) < 20:
                continue
            
            # 排序
            sorted_indices = np.argsort(factor_values)
            n = len(sorted_indices)
            
            # 多头（因子值最高）
            top_n = int(n * top_pct)
            top_indices = sorted_indices[-top_n:]
            
            # 空头（因子值最低）
            bottom_n = int(n * bottom_pct)
            bottom_indices = sorted_indices[:bottom_n]
            
            # 计算收益
            returns = group[forward_return_col].to_numpy()
            top_return = np.mean(returns[top_indices])
            bottom_return = np.mean(returns[bottom_indices])
            
            # 多空收益
            long_short = top_return - bottom_return
            
            long_short_returns.append({
                'trade_date': date,
                'long_short': long_short,
                'top': top_return,
                'bottom': bottom_return
            })
        
        if not long_short_returns:
            return {
                'long_short_return': 0,
                'long_short_sharpe': 0,
                'top_quantile_return': 0,
                'bottom_quantile_return': 0,
            }
        
        ls_df = pl.DataFrame(long_short_returns)
        ls_returns = ls_df['long_short'].to_numpy()
        
        # 年化收益
        annual_return = np.mean(ls_returns) * 252
        annual_vol = np.std(ls_returns) * np.sqrt(252)
        sharpe = annual_return / annual_vol if annual_vol > 0 else 0
        
        return {
            'long_short_return': annual_return,
            'long_short_sharpe': sharpe,
            'top_quantile_return': np.mean(ls_df['top'].to_numpy()) * 252,
            'bottom_quantile_return': np.mean(ls_df['bottom'].to_numpy()) * 252,
        }
    
    def analyze_factor(
        self,
        data: pl.DataFrame,
        factor_col: str,
        forward_return_col: str = "forward_return_1d"
    ) -> FactorMetrics:
        """
        全面分析因子有效性
        
        Args:
            data: 包含因子值和未来收益率的数据
            factor_col: 因子列名
            forward_return_col: 未来收益率列名
        
        Returns:
            因子有效性指标
        """
        self.logger.info(f"分析因子: {factor_col}")
        
        # 1. 计算IC指标
        ic_metrics = self.analyze_factor_ic(data, factor_col, forward_return_col)
        
        # 2. 计算多空收益
        ls_metrics = self.calculate_long_short_return(data, factor_col, forward_return_col)
        
        # 3. 计算最大回撤
        ls_df = self.calculate_long_short_return_detailed(data, factor_col, forward_return_col)
        max_dd = self._calculate_max_drawdown(ls_df)
        
        # 4. 计算换手率
        turnover = self._calculate_turnover(data, factor_col)
        
        # 5. t检验
        ic_df = self.calculate_ic(data, factor_col, forward_return_col)
        ic_values = ic_df['ic'].drop_nulls().to_numpy()
        if len(ic_values) > 0:
            from scipy import stats
            t_stat, p_value = stats.ttest_1samp(ic_values, 0)
        else:
            t_stat, p_value = 0, 1
        
        return FactorMetrics(
            factor_name=factor_col,
            ic_mean=ic_metrics['ic_mean'],
            ic_std=ic_metrics['ic_std'],
            ic_ir=ic_metrics['ic_ir'],
            ic_positive_ratio=ic_metrics['ic_positive_ratio'],
            long_short_return=ls_metrics['long_short_return'],
            long_short_sharpe=ls_metrics['long_short_sharpe'],
            top_quantile_return=ls_metrics['top_quantile_return'],
            bottom_quantile_return=ls_metrics['bottom_quantile_return'],
            turnover=turnover,
            max_drawdown=max_dd,
            t_statistic=t_stat,
            p_value=p_value
        )
    
    def calculate_long_short_return_detailed(
        self,
        data: pl.DataFrame,
        factor_col: str,
        forward_return_col: str = "forward_return_1d",
        top_pct: float = 0.2,
        bottom_pct: float = 0.2
    ) -> pl.DataFrame:
        """计算详细的多空收益序列"""
        long_short_returns = []
        
        for date, group in data.group_by("trade_date"):
            factor_values = group[factor_col].to_numpy()
            
            if len(factor_values) < 20:
                continue
            
            sorted_indices = np.argsort(factor_values)
            n = len(sorted_indices)
            
            top_n = int(n * top_pct)
            bottom_n = int(n * bottom_pct)
            
            returns = group[forward_return_col].to_numpy()
            top_return = np.mean(returns[sorted_indices[-top_n:]])
            bottom_return = np.mean(returns[sorted_indices[:bottom_n]])
            
            long_short_returns.append({
                'trade_date': date,
                'long_short': top_return - bottom_return,
            })
        
        return pl.DataFrame(long_short_returns)
    
    def _calculate_max_drawdown(self, returns_df: pl.DataFrame) -> float:
        """计算最大回撤"""
        if len(returns_df) == 0:
            return 0
        
        returns = returns_df['long_short'].to_numpy()
        cumulative = np.cumprod(1 + returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        
        return np.min(drawdown)
    
    def _calculate_turnover(self, data: pl.DataFrame, factor_col: str) -> float:
        """计算因子换手率"""
        dates = data['trade_date'].unique().sort()
        if len(dates) < 2:
            return 0
        
        turnovers = []
        
        for i in range(1, len(dates)):
            prev_date = dates[i - 1]
            curr_date = dates[i]
            
            prev_data = data.filter(pl.col("trade_date") == prev_date)
            curr_data = data.filter(pl.col("trade_date") == curr_date)
            
            # 获取前20%股票
            prev_codes = set(prev_data.sort(factor_col, descending=True).head(int(len(prev_data) * 0.2))['code'].to_list())
            curr_codes = set(curr_data.sort(factor_col, descending=True).head(int(len(curr_data) * 0.2))['code'].to_list())
            
            if len(prev_codes) > 0:
                turnover = len(prev_codes - curr_codes) / len(prev_codes)
                turnovers.append(turnover)
        
        return np.mean(turnovers) if turnovers else 0
    
    def analyze_all_factors(
        self,
        data: pl.DataFrame,
        factor_cols: List[str],
        forward_return_col: str = "forward_return_1d"
    ) -> pl.DataFrame:
        """
        批量分析多个因子
        
        Args:
            data: 包含多个因子的数据
            factor_cols: 因子列名列表
            forward_return_col: 未来收益率列名
        
        Returns:
            因子分析结果DataFrame
        """
        results = []
        
        for factor_col in factor_cols:
            try:
                metrics = self.analyze_factor(data, factor_col, forward_return_col)
                results.append(metrics.to_dict())
            except Exception as e:
                self.logger.error(f"分析因子 {factor_col} 失败: {e}")
        
        return pl.DataFrame(results)
    
    def get_effective_factors(
        self,
        analysis_df: pl.DataFrame,
        min_ic_ir: float = 0.3,
        min_ic_mean: float = 0.02,
        max_p_value: float = 0.05
    ) -> pl.DataFrame:
        """
        筛选有效因子
        
        Args:
            analysis_df: 因子分析结果
            min_ic_ir: 最小IC_IR
            min_ic_mean: 最小IC均值
            max_p_value: 最大p值
        
        Returns:
            有效因子DataFrame
        """
        return analysis_df.filter(
            (pl.col("ic_ir").abs() >= min_ic_ir) &
            (pl.col("ic_mean").abs() >= min_ic_mean) &
            (pl.col("p_value") <= max_p_value)
        )


if __name__ == "__main__":
    print("=" * 80)
    print("因子有效性分析器")
    print("=" * 80)
    print()
    print("使用示例:")
    print()
    print("```python")
    print("from core.factor_analyzer import FactorAnalyzer")
    print()
    print("# 创建分析器")
    print("analyzer = FactorAnalyzer()")
    print()
    print("# 分析单个因子")
    print("metrics = analyzer.analyze_factor(data, 'factor_ma5_bias')")
    print("print(f'IC均值: {metrics.ic_mean:.4f}')")
    print("print(f'IC_IR: {metrics.ic_ir:.4f}')")
    print()
    print("# 批量分析多个因子")
    print("factor_cols = ['factor_ma5_bias', 'factor_limit_up_score']")
    print("results = analyzer.analyze_all_factors(data, factor_cols)")
    print()
    print("# 筛选有效因子")
    print("effective = analyzer.get_effective_factors(results)")
    print("```")
    print()
    print("=" * 80)
