#!/usr/bin/env python3
"""
历史数据分析脚本

功能:
1. 接入真实历史数据
2. 数据质量检查
3. 统计分析
4. 可视化
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalDataAnalyzer:
    """历史数据分析器"""
    
    def __init__(self, data_dir: str = None):
        self.data_dir = data_dir or '/Volumes/Xdata/workstation/xxxcnstock/data/kline'
        self.stock_list_path = '/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet'
        self.results = {}
        
    def load_stock_list(self) -> pd.DataFrame:
        """加载股票列表"""
        try:
            if os.path.exists(self.stock_list_path):
                df = pd.read_parquet(self.stock_list_path)
                logger.info(f"加载股票列表: {len(df)} 只股票")
                return df
            else:
                logger.warning(f"股票列表文件不存在: {self.stock_list_path}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"加载股票列表失败: {e}")
            return pd.DataFrame()
    
    def load_kline_data(self, code: str) -> pd.DataFrame:
        """加载单只股票K线数据"""
        try:
            file_path = os.path.join(self.data_dir, f"{code}.parquet")
            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"加载 {code} K线数据失败: {e}")
            return pd.DataFrame()
    
    def get_available_stocks(self) -> List[str]:
        """获取有数据的所有股票"""
        try:
            files = os.listdir(self.data_dir)
            stocks = [f.replace('.parquet', '') for f in files if f.endswith('.parquet')]
            logger.info(f"发现 {len(stocks)} 只股票的历史数据")
            return sorted(stocks)
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
    def analyze_data_quality(self, sample_size: int = 100) -> Dict:
        """分析数据质量"""
        logger.info("开始数据质量分析...")
        
        stocks = self.get_available_stocks()[:sample_size]
        
        quality_stats = {
            'total_stocks': len(stocks),
            'valid_stocks': 0,
            'invalid_stocks': [],
            'data_gaps': [],
            'price_anomalies': [],
            'volume_anomalies': []
        }
        
        for code in stocks:
            df = self.load_kline_data(code)
            
            if df.empty:
                quality_stats['invalid_stocks'].append(code)
                continue
            
            # 检查必要列
            required_cols = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                quality_stats['invalid_stocks'].append(f"{code}(缺少列: {missing_cols})")
                continue
            
            # 检查数据连续性
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date')
            
            date_diff = df['trade_date'].diff().dt.days
            gaps = date_diff[date_diff > 5]  # 超过5天的间隔视为缺口
            
            if len(gaps) > 0:
                quality_stats['data_gaps'].append({
                    'code': code,
                    'gap_count': len(gaps),
                    'max_gap': int(gaps.max())
                })
            
            # 检查价格异常
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if (df[col] <= 0).any():
                    quality_stats['price_anomalies'].append(f"{code}: {col} 有非正值")
                    break
            
            # 检查最高价/最低价逻辑
            if ((df['high'] < df['low']).any() or 
                (df['high'] < df['open']).any() or 
                (df['high'] < df['close']).any()):
                quality_stats['price_anomalies'].append(f"{code}: 最高价逻辑异常")
            
            # 检查成交量异常
            if (df['volume'] < 0).any():
                quality_stats['volume_anomalies'].append(f"{code}: 成交量为负")
            
            quality_stats['valid_stocks'] += 1
        
        self.results['data_quality'] = quality_stats
        return quality_stats
    
    def analyze_market_overview(self) -> Dict:
        """分析市场概况"""
        logger.info("开始市场概况分析...")
        
        stocks = self.get_available_stocks()
        
        # 加载所有数据
        all_data = []
        for code in stocks[:500]:  # 限制样本数量
            df = self.load_kline_data(code)
            if not df.empty:
                df['code'] = code
                all_data.append(df)
        
        if not all_data:
            logger.warning("没有可用的数据")
            return {}
        
        combined = pd.concat(all_data, ignore_index=True)
        combined['trade_date'] = pd.to_datetime(combined['trade_date'])
        
        # 按日期统计
        daily_stats = combined.groupby('trade_date').agg({
            'code': 'count',
            'volume': 'sum',
            'amount': 'sum' if 'amount' in combined.columns else 'volume'
        }).rename(columns={'code': 'stock_count'})
        
        # 计算市场整体指标
        latest_date = combined['trade_date'].max()
        earliest_date = combined['trade_date'].min()
        
        overview = {
            'total_stocks': len(stocks),
            'date_range': {
                'start': earliest_date.strftime('%Y-%m-%d'),
                'end': latest_date.strftime('%Y-%m-%d'),
                'days': (latest_date - earliest_date).days
            },
            'latest_trading_day': {
                'date': latest_date.strftime('%Y-%m-%d'),
                'active_stocks': int(daily_stats.loc[latest_date, 'stock_count']),
                'total_volume': float(daily_stats.loc[latest_date, 'volume']),
            },
            'avg_daily_volume': float(daily_stats['volume'].mean()),
            'max_daily_volume': {
                'date': daily_stats['volume'].idxmax().strftime('%Y-%m-%d'),
                'volume': float(daily_stats['volume'].max())
            }
        }
        
        self.results['market_overview'] = overview
        return overview
    
    def analyze_stock_performance(self, top_n: int = 20) -> Dict:
        """分析股票表现"""
        logger.info("开始股票表现分析...")
        
        stocks = self.get_available_stocks()
        
        performance_list = []
        
        for code in stocks:
            df = self.load_kline_data(code)
            if df.empty or len(df) < 20:
                continue
            
            df = df.sort_values('trade_date')
            
            # 计算收益率
            start_price = df['close'].iloc[0]
            end_price = df['close'].iloc[-1]
            total_return = (end_price - start_price) / start_price
            
            # 计算波动率
            df['daily_return'] = df['close'].pct_change()
            volatility = df['daily_return'].std() * np.sqrt(252)
            
            # 计算最大回撤
            df['cummax'] = df['close'].cummax()
            df['drawdown'] = (df['close'] - df['cummax']) / df['cummax']
            max_drawdown = df['drawdown'].min()
            
            # 计算夏普比率（假设无风险利率3%）
            avg_return = df['daily_return'].mean() * 252
            sharpe = (avg_return - 0.03) / volatility if volatility > 0 else 0
            
            performance_list.append({
                'code': code,
                'total_return': total_return,
                'annualized_return': avg_return,
                'volatility': volatility,
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe,
                'trading_days': len(df),
                'start_price': start_price,
                'end_price': end_price
            })
        
        if not performance_list:
            return {}
        
        perf_df = pd.DataFrame(performance_list)
        
        # 按收益率排序
        top_gainers = perf_df.nlargest(top_n, 'total_return')
        top_losers = perf_df.nsmallest(top_n, 'total_return')
        
        # 按夏普比率排序
        top_sharpe = perf_df.nlargest(top_n, 'sharpe_ratio')
        
        performance = {
            'total_analyzed': len(performance_list),
            'avg_return': float(perf_df['total_return'].mean()),
            'median_return': float(perf_df['total_return'].median()),
            'avg_volatility': float(perf_df['volatility'].mean()),
            'avg_sharpe': float(perf_df['sharpe_ratio'].mean()),
            'top_gainers': top_gainers[['code', 'total_return', 'sharpe_ratio']].to_dict('records'),
            'top_losers': top_losers[['code', 'total_return', 'sharpe_ratio']].to_dict('records'),
            'top_sharpe': top_sharpe[['code', 'total_return', 'sharpe_ratio']].to_dict('records')
        }
        
        self.results['stock_performance'] = performance
        return performance
    
    def analyze_sector_distribution(self) -> Dict:
        """分析行业分布"""
        logger.info("开始行业分布分析...")
        
        stock_list = self.load_stock_list()
        
        if stock_list.empty or 'industry' not in stock_list.columns:
            logger.warning("股票列表缺少行业信息")
            return {}
        
        # 统计行业分布
        industry_dist = stock_list['industry'].value_counts()
        
        sector_analysis = {
            'total_sectors': len(industry_dist),
            'sector_distribution': industry_dist.head(20).to_dict(),
            'top_sectors': industry_dist.head(10).index.tolist()
        }
        
        self.results['sector_distribution'] = sector_analysis
        return sector_analysis
    
    def analyze_trading_patterns(self) -> Dict:
        """分析交易模式"""
        logger.info("开始交易模式分析...")
        
        stocks = self.get_available_stocks()[:200]
        
        all_returns = []
        volume_patterns = []
        
        for code in stocks:
            df = self.load_kline_data(code)
            if df.empty or len(df) < 60:
                continue
            
            df = df.sort_values('trade_date')
            df['daily_return'] = df['close'].pct_change()
            df['volume_ma20'] = df['volume'].rolling(20).mean()
            df['volume_ratio'] = df['volume'] / df['volume_ma20']
            
            all_returns.extend(df['daily_return'].dropna().tolist())
            
            # 放量上涨/下跌统计
            high_volume = df[df['volume_ratio'] > 2]
            
            volume_patterns.append({
                'code': code,
                'high_volume_days': len(high_volume),
                'avg_volume_ratio': df['volume_ratio'].mean()
            })
        
        if not all_returns:
            return {}
        
        returns_array = np.array(all_returns)
        
        patterns = {
            'return_distribution': {
                'mean': float(np.mean(returns_array)),
                'std': float(np.std(returns_array)),
                'skewness': float(pd.Series(returns_array).skew()),
                'kurtosis': float(pd.Series(returns_array).kurtosis()),
                'positive_days': float(np.sum(returns_array > 0) / len(returns_array)),
                'negative_days': float(np.sum(returns_array < 0) / len(returns_array))
            },
            'extreme_moves': {
                'up_10pct': float(np.sum(returns_array > 0.099) / len(returns_array)),
                'down_10pct': float(np.sum(returns_array < -0.099) / len(returns_array)),
                'up_limit': float(np.sum(returns_array > 0.199) / len(returns_array)),
                'down_limit': float(np.sum(returns_array < -0.199) / len(returns_array))
            }
        }
        
        self.results['trading_patterns'] = patterns
        return patterns
    
    def generate_report(self) -> str:
        """生成分析报告"""
        report_lines = [
            "=" * 60,
            "历史数据分析报告",
            "=" * 60,
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        # 市场概况
        if 'market_overview' in self.results:
            overview = self.results['market_overview']
            report_lines.extend([
                "【市场概况】",
                f"  股票总数: {overview.get('total_stocks', 0)}",
                f"  数据时间范围: {overview.get('date_range', {}).get('start', 'N/A')} ~ {overview.get('date_range', {}).get('end', 'N/A')}",
                f"  最新交易日: {overview.get('latest_trading_day', {}).get('date', 'N/A')}",
                f"  活跃股票数: {overview.get('latest_trading_day', {}).get('active_stocks', 0)}",
                ""
            ])
        
        # 数据质量
        if 'data_quality' in self.results:
            quality = self.results['data_quality']
            report_lines.extend([
                "【数据质量】",
                f"  检查股票数: {quality.get('total_stocks', 0)}",
                f"  有效数据: {quality.get('valid_stocks', 0)}",
                f"  无效数据: {len(quality.get('invalid_stocks', []))}",
                f"  数据缺口: {len(quality.get('data_gaps', []))}",
                f"  价格异常: {len(quality.get('price_anomalies', []))}",
                ""
            ])
        
        # 股票表现
        if 'stock_performance' in self.results:
            perf = self.results['stock_performance']
            report_lines.extend([
                "【股票表现】",
                f"  分析股票数: {perf.get('total_analyzed', 0)}",
                f"  平均收益率: {perf.get('avg_return', 0):.2%}",
                f"  中位数收益率: {perf.get('median_return', 0):.2%}",
                f"  平均波动率: {perf.get('avg_volatility', 0):.2%}",
                f"  平均夏普比率: {perf.get('avg_sharpe', 0):.2f}",
                "",
                "  涨幅Top5:"
            ])
            
            for i, stock in enumerate(perf.get('top_gainers', [])[:5], 1):
                report_lines.append(
                    f"    {i}. {stock['code']}: {stock['total_return']:.2%} (夏普: {stock['sharpe_ratio']:.2f})"
                )
            
            report_lines.append("")
        
        # 交易模式
        if 'trading_patterns' in self.results:
            patterns = self.results['trading_patterns']
            dist = patterns.get('return_distribution', {})
            extreme = patterns.get('extreme_moves', {})
            
            report_lines.extend([
                "【交易模式】",
                f"  日均收益率: {dist.get('mean', 0):.4%}",
                f"  日收益率标准差: {dist.get('std', 0):.4%}",
                f"  上涨天数占比: {dist.get('positive_days', 0):.2%}",
                f"  下跌天数占比: {dist.get('negative_days', 0):.2%}",
                f"  涨停(>10%)概率: {extreme.get('up_10pct', 0):.4%}",
                f"  跌停(<-10%)概率: {extreme.get('down_10pct', 0):.4%}",
                ""
            ])
        
        report_lines.append("=" * 60)
        
        return "\n".join(report_lines)
    
    def run_full_analysis(self):
        """运行完整分析"""
        logger.info("=" * 60)
        logger.info("开始完整历史数据分析")
        logger.info("=" * 60)
        
        # 1. 数据质量检查
        self.analyze_data_quality(sample_size=200)
        
        # 2. 市场概况
        self.analyze_market_overview()
        
        # 3. 股票表现
        self.analyze_stock_performance(top_n=20)
        
        # 4. 行业分布
        self.analyze_sector_distribution()
        
        # 5. 交易模式
        self.analyze_trading_patterns()
        
        # 6. 生成报告
        report = self.generate_report()
        
        # 保存报告
        report_path = '/Volumes/Xdata/workstation/xxxcnstock/reports/historical_data_analysis.txt'
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"\n分析报告已保存: {report_path}")
        
        return report


def main():
    """主函数"""
    analyzer = HistoricalDataAnalyzer()
    report = analyzer.run_full_analysis()
    print("\n" + report)


if __name__ == '__main__':
    main()
