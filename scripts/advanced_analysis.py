#!/usr/bin/env python3
"""
高级数据分析脚本

功能:
1. 行业/板块深度分析
2. 股票间相关性分析
3. 多因子构建与分析
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdvancedAnalyzer:
    """高级分析器"""
    
    def __init__(self, data_dir: str = None):
        self.data_dir = data_dir or '/Volumes/Xdata/workstation/xxxcnstock/data/kline'
        self.stock_list_path = '/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet'
        self.results = {}
        
    def get_available_stocks(self) -> List[str]:
        """获取有数据的所有股票"""
        try:
            files = os.listdir(self.data_dir)
            stocks = [f.replace('.parquet', '') for f in files if f.endswith('.parquet')]
            return sorted(stocks)
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
    def load_kline_data(self, code: str) -> pd.DataFrame:
        """加载单只股票K线数据"""
        try:
            file_path = os.path.join(self.data_dir, f"{code}.parquet")
            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                df['code'] = code
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                return df
            return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    
    def load_stock_list(self) -> pd.DataFrame:
        """加载股票列表"""
        try:
            if os.path.exists(self.stock_list_path):
                return pd.read_parquet(self.stock_list_path)
            return pd.DataFrame()
        except:
            return pd.DataFrame()
    
    def infer_industry(self, code: str) -> str:
        """根据股票代码推断行业"""
        # 根据代码前缀推断板块
        if code.startswith('60'):
            return '沪市主板'
        elif code.startswith('00'):
            return '深市主板'
        elif code.startswith('30'):
            return '创业板'
        elif code.startswith('68'):
            return '科创板'
        elif code.startswith('8') or code.startswith('4'):
            return '北交所'
        else:
            return '其他'
    
    def analyze_sector_performance(self) -> Dict:
        """分析板块表现"""
        logger.info("开始板块表现分析...")
        
        stocks = self.get_available_stocks()
        
        sector_data = {
            '沪市主板': [],
            '深市主板': [],
            '创业板': [],
            '科创板': [],
            '北交所': [],
            '其他': []
        }
        
        # 按板块分类
        for code in stocks[:1000]:  # 限制样本
            sector = self.infer_industry(code)
            sector_data[sector].append(code)
        
        sector_performance = {}
        
        for sector, codes in sector_data.items():
            if len(codes) == 0:
                continue
            
            returns = []
            volatilities = []
            sharpes = []
            
            for code in codes[:100]:  # 每板块最多100只
                df = self.load_kline_data(code)
                if df.empty or len(df) < 20:
                    continue
                
                df = df.sort_values('trade_date')
                start_price = df['close'].iloc[0]
                end_price = df['close'].iloc[-1]
                total_return = (end_price - start_price) / start_price
                
                df['daily_return'] = df['close'].pct_change()
                volatility = df['daily_return'].std() * np.sqrt(252)
                
                avg_return = df['daily_return'].mean() * 252
                sharpe = (avg_return - 0.03) / volatility if volatility > 0 else 0
                
                returns.append(total_return)
                volatilities.append(volatility)
                sharpes.append(sharpe)
            
            if returns:
                sector_performance[sector] = {
                    'stock_count': len(codes),
                    'avg_return': float(np.mean(returns)),
                    'median_return': float(np.median(returns)),
                    'avg_volatility': float(np.mean(volatilities)),
                    'avg_sharpe': float(np.mean(sharpes)),
                    'best_return': float(np.max(returns)),
                    'worst_return': float(np.min(returns)),
                    'positive_ratio': float(np.sum(np.array(returns) > 0) / len(returns))
                }
        
        self.results['sector_performance'] = sector_performance
        return sector_performance
    
    def analyze_correlation_matrix(self, sample_size: int = 100) -> Dict:
        """分析股票间相关性"""
        logger.info(f"开始相关性分析 (样本: {sample_size})...")
        
        stocks = self.get_available_stocks()[:sample_size]
        
        # 收集收益率数据
        returns_data = {}
        
        for code in stocks:
            df = self.load_kline_data(code)
            if df.empty or len(df) < 60:
                continue
            
            df = df.sort_values('trade_date')
            df['daily_return'] = df['close'].pct_change()
            
            # 处理重复日期
            df = df.drop_duplicates(subset=['trade_date'], keep='first')
            
            returns_data[code] = df.set_index('trade_date')['daily_return']
        
        if len(returns_data) < 10:
            logger.warning("有效股票数量不足")
            return {}
        
        # 构建收益率矩阵
        returns_df = pd.DataFrame(returns_data)
        returns_df = returns_df.dropna(axis=1, thresh=len(returns_df)*0.8)  # 删除缺失值过多的列
        returns_df = returns_df.fillna(0)
        
        # 计算相关性矩阵
        corr_matrix = returns_df.corr()
        
        # 统计相关性分布
        corr_values = corr_matrix.values
        mask = ~np.eye(corr_values.shape[0], dtype=bool)  # 排除对角线
        corr_flat = corr_values[mask]
        
        correlation_stats = {
            'sample_size': len(returns_data),
            'avg_correlation': float(np.mean(corr_flat)),
            'median_correlation': float(np.median(corr_flat)),
            'std_correlation': float(np.std(corr_flat)),
            'high_correlation_pct': float(np.sum(corr_flat > 0.5) / len(corr_flat)),
            'low_correlation_pct': float(np.sum(corr_flat < 0.1) / len(corr_flat)),
            'negative_correlation_pct': float(np.sum(corr_flat < 0) / len(corr_flat))
        }
        
        # 找出相关性最高的股票对
        corr_pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_pairs.append({
                    'stock1': corr_matrix.columns[i],
                    'stock2': corr_matrix.columns[j],
                    'correlation': corr_matrix.iloc[i, j]
                })
        
        if len(corr_pairs) > 0:
            corr_pairs_df = pd.DataFrame(corr_pairs)
            top_correlated = corr_pairs_df.nlargest(min(10, len(corr_pairs_df)), 'correlation')
            least_correlated = corr_pairs_df.nsmallest(min(10, len(corr_pairs_df)), 'correlation')
        else:
            top_correlated = pd.DataFrame(columns=['stock1', 'stock2', 'correlation'])
            least_correlated = pd.DataFrame(columns=['stock1', 'stock2', 'correlation'])
        
        correlation_stats['top_correlated'] = top_correlated.to_dict('records')
        correlation_stats['least_correlated'] = least_correlated.to_dict('records')
        
        self.results['correlation_analysis'] = correlation_stats
        return correlation_stats
    
    def build_factors(self, sample_size: int = 500) -> Dict:
        """构建多因子"""
        logger.info(f"开始构建多因子 (样本: {sample_size})...")
        
        stocks = self.get_available_stocks()[:sample_size]
        
        factor_data = []
        
        for code in stocks:
            df = self.load_kline_data(code)
            if df.empty or len(df) < 60:
                continue
            
            df = df.sort_values('trade_date')
            
            # 价格特征
            latest = df.iloc[-1]
            
            # 收益率特征
            df['daily_return'] = df['close'].pct_change()
            
            # 价值因子
            latest_close = latest['close']
            
            # 动量因子
            returns_20d = (latest['close'] - df['close'].iloc[-20]) / df['close'].iloc[-20] if len(df) >= 20 else 0
            returns_60d = (latest['close'] - df['close'].iloc[-60]) / df['close'].iloc[-60] if len(df) >= 60 else 0
            
            # 波动率因子
            volatility_20d = df['daily_return'].iloc[-20:].std() * np.sqrt(252) if len(df) >= 20 else 0
            
            # 流动性因子
            avg_volume_20d = df['volume'].iloc[-20:].mean() if len(df) >= 20 else 0
            
            # 趋势因子
            ma20 = df['close'].iloc[-20:].mean() if len(df) >= 20 else latest['close']
            ma60 = df['close'].iloc[-60:].mean() if len(df) >= 60 else latest['close']
            
            trend_score = 1 if latest['close'] > ma20 > ma60 else (
                -1 if latest['close'] < ma20 < ma60 else 0
            )
            
            # 计算未来收益率（作为目标变量）
            future_return = 0
            if len(df) >= 20:
                future_return = (df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20]
            
            factor_data.append({
                'code': code,
                'sector': self.infer_industry(code),
                'price': latest_close,
                'momentum_20d': returns_20d,
                'momentum_60d': returns_60d,
                'volatility': volatility_20d,
                'liquidity': avg_volume_20d,
                'trend_score': trend_score,
                'future_return': future_return
            })
        
        if not factor_data:
            return {}
        
        factor_df = pd.DataFrame(factor_data)
        
        # 因子统计分析
        factor_stats = {
            'sample_size': len(factor_df),
            'factors': {}
        }
        
        for factor in ['momentum_20d', 'momentum_60d', 'volatility', 'trend_score']:
            factor_stats['factors'][factor] = {
                'mean': float(factor_df[factor].mean()),
                'std': float(factor_df[factor].std()),
                'min': float(factor_df[factor].min()),
                'max': float(factor_df[factor].max()),
                'median': float(factor_df[factor].median())
            }
        
        # 因子与收益的相关性
        correlations = {}
        for factor in ['momentum_20d', 'momentum_60d', 'volatility', 'trend_score']:
            corr = factor_df[factor].corr(factor_df['future_return'])
            correlations[factor] = float(corr)
        
        factor_stats['factor_return_correlation'] = correlations
        
        # 分板块因子表现
        sector_factors = factor_df.groupby('sector').agg({
            'momentum_20d': 'mean',
            'momentum_60d': 'mean',
            'volatility': 'mean',
            'trend_score': 'mean',
            'future_return': 'mean'
        }).to_dict()
        
        factor_stats['sector_factors'] = sector_factors
        
        # 因子分层收益
        factor_quintiles = {}
        for factor in ['momentum_20d', 'momentum_60d', 'volatility']:
            factor_df['quintile'] = pd.qcut(factor_df[factor], 5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'], duplicates='drop')
            quintile_returns = factor_df.groupby('quintile')['future_return'].mean().to_dict()
            factor_quintiles[factor] = {k: float(v) for k, v in quintile_returns.items()}
        
        factor_stats['factor_quintiles'] = factor_quintiles
        
        self.results['factor_analysis'] = factor_stats
        return factor_stats
    
    def generate_advanced_report(self) -> str:
        """生成高级分析报告"""
        report_lines = [
            "=" * 70,
            "高级数据分析报告",
            "=" * 70,
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        # 板块表现
        if 'sector_performance' in self.results:
            report_lines.extend([
                "【板块表现分析】",
                ""
            ])
            
            # 按收益率排序
            sorted_sectors = sorted(
                self.results['sector_performance'].items(),
                key=lambda x: x[1]['avg_return'],
                reverse=True
            )
            
            for sector, data in sorted_sectors:
                report_lines.extend([
                    f"  {sector}:",
                    f"    股票数: {data['stock_count']}",
                    f"    平均收益率: {data['avg_return']:.2%}",
                    f"    中位数收益率: {data['median_return']:.2%}",
                    f"    平均波动率: {data['avg_volatility']:.2%}",
                    f"    平均夏普比率: {data['avg_sharpe']:.2f}",
                    f"    正收益比例: {data['positive_ratio']:.2%}",
                    ""
                ])
        
        # 相关性分析
        if 'correlation_analysis' in self.results:
            corr = self.results['correlation_analysis']
            report_lines.extend([
                "【相关性分析】",
                f"  样本股票数: {corr['sample_size']}",
                f"  平均相关性: {corr['avg_correlation']:.4f}",
                f"  中位数相关性: {corr['median_correlation']:.4f}",
                f"  高相关性(>0.5)占比: {corr['high_correlation_pct']:.2%}",
                f"  低相关性(<0.1)占比: {corr['low_correlation_pct']:.2%}",
                f"  负相关性占比: {corr['negative_correlation_pct']:.2%}",
                "",
                "  相关性最高股票对:"
            ])
            
            for pair in corr.get('top_correlated', [])[:5]:
                report_lines.append(
                    f"    {pair['stock1']} - {pair['stock2']}: {pair['correlation']:.4f}"
                )
            
            report_lines.append("")
        
        # 因子分析
        if 'factor_analysis' in self.results:
            factor = self.results['factor_analysis']
            report_lines.extend([
                "【多因子分析】",
                f"  样本股票数: {factor['sample_size']}",
                "",
                "  因子统计:"
            ])
            
            for fname, fstat in factor.get('factors', {}).items():
                report_lines.append(
                    f"    {fname}: 均值={fstat['mean']:.4f}, 标准差={fstat['std']:.4f}"
                )
            
            report_lines.extend([
                "",
                "  因子与未来收益相关性:"
            ])
            
            for fname, corr in factor.get('factor_return_correlation', {}).items():
                report_lines.append(f"    {fname}: {corr:.4f}")
            
            # 因子分层收益
            if 'factor_quintiles' in factor:
                report_lines.extend([
                    "",
                    "  因子分层收益(五分位):"
                ])
                
                for fname, quintiles in factor['factor_quintiles'].items():
                    report_lines.append(f"    {fname}:")
                    for q, ret in quintiles.items():
                        report_lines.append(f"      {q}: {ret:.2%}")
            
            report_lines.append("")
        
        report_lines.append("=" * 70)
        
        return "\n".join(report_lines)
    
    def run_all_analysis(self):
        """运行所有分析"""
        logger.info("=" * 70)
        logger.info("开始高级数据分析")
        logger.info("=" * 70)
        
        # 1. 板块表现分析
        self.analyze_sector_performance()
        
        # 2. 相关性分析
        self.analyze_correlation_matrix(sample_size=150)
        
        # 3. 因子分析
        self.build_factors(sample_size=800)
        
        # 4. 生成报告
        report = self.generate_advanced_report()
        
        # 保存报告
        report_path = '/Volumes/Xdata/workstation/xxxcnstock/reports/advanced_analysis.txt'
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"\n高级分析报告已保存: {report_path}")
        
        return report


def main():
    """主函数"""
    analyzer = AdvancedAnalyzer()
    report = analyzer.run_all_analysis()
    print("\n" + report)


if __name__ == '__main__':
    main()
