#!/usr/bin/env python3
"""
多因子选股策略

结合价值、成长、质量、动量等多个因子进行选股
"""
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MultiFactorConfig:
    """多因子配置"""
    # 价值因子权重
    pe_weight: float = 0.15
    pb_weight: float = 0.15
    ps_weight: float = 0.10
    
    # 成长因子权重
    revenue_growth_weight: float = 0.15
    profit_growth_weight: float = 0.15
    
    # 质量因子权重
    roe_weight: float = 0.15
    debt_ratio_weight: float = 0.10
    
    # 动量因子权重
    momentum_weight: float = 0.05
    
    # 筛选条件
    min_market_cap: float = 50.0  # 最小市值(亿)
    max_market_cap: float = 500.0  # 最大市值(亿)
    min_roe: float = 0.10  # 最小ROE
    max_pe: float = 50.0  # 最大PE
    max_pb: float = 5.0  # 最大PB
    max_debt_ratio: float = 0.70  # 最大负债率
    
    # 选股数量
    top_n: int = 20


class MultiFactorStrategy:
    """
    多因子选股策略
    
    因子构成:
    1. 价值因子: PE、PB、PS (低估值)
    2. 成长因子: 营收增长率、利润增长率
    3. 质量因子: ROE、负债率
    4. 动量因子: 价格动量
    """
    
    def __init__(self, config: Optional[MultiFactorConfig] = None):
        self.config = config or MultiFactorConfig()
        
    def calculate_factor_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算因子得分"""
        result = df.copy()
        
        # 价值因子得分 (越低越好，取倒数)
        if 'pe' in result.columns:
            result['pe_score'] = 1 / (result['pe'] + 1)
        if 'pb' in result.columns:
            result['pb_score'] = 1 / (result['pb'] + 1)
        if 'ps' in result.columns:
            result['ps_score'] = 1 / (result['ps'] + 1)
        
        # 成长因子得分 (越高越好)
        if 'revenue_growth' in result.columns:
            result['growth_score'] = result['revenue_growth']
        if 'profit_growth' in result.columns:
            result['profit_score'] = result['profit_growth']
        
        # 质量因子得分
        if 'roe' in result.columns:
            result['roe_score'] = result['roe']
        if 'debt_ratio' in result.columns:
            result['debt_score'] = 1 - result['debt_ratio']  # 负债率越低越好
        
        # 动量因子得分
        if 'momentum_20d' in result.columns:
            result['momentum_score'] = result['momentum_20d']
        
        return result
    
    def calculate_composite_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算综合得分"""
        result = df.copy()
        
        # 初始化综合得分
        result['composite_score'] = 0
        
        # 价值因子
        if 'pe_score' in result.columns:
            result['composite_score'] += result['pe_score'] * self.config.pe_weight
        if 'pb_score' in result.columns:
            result['composite_score'] += result['pb_score'] * self.config.pb_weight
        if 'ps_score' in result.columns:
            result['composite_score'] += result['ps_score'] * self.config.ps_weight
        
        # 成长因子
        if 'growth_score' in result.columns:
            result['composite_score'] += result['growth_score'] * self.config.revenue_growth_weight
        if 'profit_score' in result.columns:
            result['composite_score'] += result['profit_score'] * self.config.profit_growth_weight
        
        # 质量因子
        if 'roe_score' in result.columns:
            result['composite_score'] += result['roe_score'] * self.config.roe_weight
        if 'debt_score' in result.columns:
            result['composite_score'] += result['debt_score'] * self.config.debt_ratio_weight
        
        # 动量因子
        if 'momentum_score' in result.columns:
            result['composite_score'] += result['momentum_score'] * self.config.momentum_weight
        
        return result
    
    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用筛选条件"""
        result = df.copy()
        
        # 市值筛选
        if 'market_cap' in result.columns:
            result = result[
                (result['market_cap'] >= self.config.min_market_cap) &
                (result['market_cap'] <= self.config.max_market_cap)
            ]
        
        # ROE筛选
        if 'roe' in result.columns:
            result = result[result['roe'] >= self.config.min_roe]
        
        # PE筛选
        if 'pe' in result.columns:
            result = result[result['pe'] <= self.config.max_pe]
        
        # PB筛选
        if 'pb' in result.columns:
            result = result[result['pb'] <= self.config.max_pb]
        
        # 负债率筛选
        if 'debt_ratio' in result.columns:
            result = result[result['debt_ratio'] <= self.config.max_debt_ratio]
        
        return result
    
    def select_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """选股主函数"""
        logger.info(f"多因子选股开始，初始股票数: {len(df)}")
        
        # 应用筛选条件
        filtered = self.apply_filters(df)
        logger.info(f"筛选后股票数: {len(filtered)}")
        
        if filtered.empty:
            return filtered
        
        # 计算因子得分
        scored = self.calculate_factor_scores(filtered)
        
        # 计算综合得分
        final = self.calculate_composite_score(scored)
        
        # 按综合得分排序
        final = final.sort_values('composite_score', ascending=False)
        
        # 选取前N名
        selected = final.head(self.config.top_n)
        
        logger.info(f"最终选中股票数: {len(selected)}")
        
        return selected
    
    def get_factor_exposure(self, df: pd.DataFrame) -> Dict[str, float]:
        """获取因子暴露度"""
        exposure = {}
        
        # 价值暴露
        if 'pe' in df.columns:
            exposure['value'] = df['pe'].mean()
        
        # 成长暴露
        if 'revenue_growth' in df.columns:
            exposure['growth'] = df['revenue_growth'].mean()
        
        # 质量暴露
        if 'roe' in df.columns:
            exposure['quality'] = df['roe'].mean()
        
        # 动量暴露
        if 'momentum_20d' in df.columns:
            exposure['momentum'] = df['momentum_20d'].mean()
        
        return exposure
    
    def analyze_factor_correlation(self, df: pd.DataFrame) -> pd.DataFrame:
        """分析因子相关性"""
        factor_cols = ['pe', 'pb', 'ps', 'roe', 'revenue_growth', 
                      'profit_growth', 'debt_ratio', 'momentum_20d']
        
        available_cols = [c for c in factor_cols if c in df.columns]
        
        if len(available_cols) < 2:
            return pd.DataFrame()
        
        return df[available_cols].corr()


class FactorRotationStrategy:
    """
    因子轮动策略
    
    根据市场环境动态调整因子权重
    """
    
    def __init__(self, base_config: MultiFactorConfig = None):
        self.base_config = base_config or MultiFactorConfig()
        self.market_regime = 'neutral'  # bull, bear, neutral
        
    def detect_market_regime(self, market_data: Dict) -> str:
        """检测市场环境"""
        # 根据大盘趋势判断
        index_trend = market_data.get('index_trend', 0)
        volatility = market_data.get('volatility', 0)
        
        if index_trend > 0.05 and volatility < 0.2:
            return 'bull'
        elif index_trend < -0.05:
            return 'bear'
        else:
            return 'neutral'
    
    def adjust_weights(self, market_regime: str) -> MultiFactorConfig:
        """根据市场环境调整权重"""
        config = MultiFactorConfig()
        
        if market_regime == 'bull':
            # 牛市: 增加成长、动量权重
            config.revenue_growth_weight = 0.25
            config.profit_growth_weight = 0.20
            config.momentum_weight = 0.15
            config.pe_weight = 0.10
            config.pb_weight = 0.10
        elif market_regime == 'bear':
            # 熊市: 增加价值、质量权重
            config.pe_weight = 0.25
            config.pb_weight = 0.20
            config.roe_weight = 0.20
            config.debt_ratio_weight = 0.15
            config.revenue_growth_weight = 0.10
            config.momentum_weight = 0.00
        else:
            # 震荡市: 均衡配置
            config = self.base_config
        
        return config
    
    def execute(self, stock_data: pd.DataFrame, market_data: Dict) -> pd.DataFrame:
        """执行因子轮动策略"""
        # 检测市场环境
        regime = self.detect_market_regime(market_data)
        logger.info(f"当前市场环境: {regime}")
        
        # 调整权重
        adjusted_config = self.adjust_weights(regime)
        
        # 执行多因子选股
        strategy = MultiFactorStrategy(adjusted_config)
        return strategy.select_stocks(stock_data)


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 创建模拟数据
    np.random.seed(42)
    n_stocks = 100
    
    mock_data = pd.DataFrame({
        'code': [f'{i:06d}' for i in range(1, n_stocks + 1)],
        'name': [f'Stock_{i}' for i in range(1, n_stocks + 1)],
        'pe': np.random.uniform(5, 100, n_stocks),
        'pb': np.random.uniform(0.5, 10, n_stocks),
        'ps': np.random.uniform(0.5, 20, n_stocks),
        'roe': np.random.uniform(0.05, 0.30, n_stocks),
        'revenue_growth': np.random.uniform(-0.2, 0.5, n_stocks),
        'profit_growth': np.random.uniform(-0.3, 0.6, n_stocks),
        'debt_ratio': np.random.uniform(0.2, 0.8, n_stocks),
        'momentum_20d': np.random.uniform(-0.1, 0.2, n_stocks),
        'market_cap': np.random.uniform(30, 800, n_stocks)
    })
    
    # 运行策略
    strategy = MultiFactorStrategy()
    selected = strategy.select_stocks(mock_data)
    
    print("\n选中的股票:")
    print(selected[['code', 'name', 'pe', 'pb', 'roe', 'composite_score']].head(10))
    
    # 因子暴露分析
    exposure = strategy.get_factor_exposure(selected)
    print("\n因子暴露:")
    for factor, value in exposure.items():
        print(f"  {factor}: {value:.4f}")
