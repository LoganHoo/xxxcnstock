#!/usr/bin/env python3
"""
行业轮动策略

策略逻辑:
1. 分析不同行业在不同市场环境下的表现
2. 基于宏观经济指标、市场情绪进行行业配置
3. 动态调整行业权重，追逐强势行业

使用方法:
    from services.strategy_service.sector_rotation.strategy import SectorRotationStrategy
    
    strategy = SectorRotationStrategy(config)
    sector_signals = strategy.analyze_sectors(market_data)
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

logger = logging.getLogger(__name__)


class MarketRegime(Enum):
    """市场环境"""
    BULL = "bull"           # 牛市
    BEAR = "bear"           # 熊市
    VOLATILE = "volatile"   # 震荡市
    RECOVERY = "recovery"   # 复苏期


@dataclass
class SectorConfig:
    """行业轮动策略配置"""
    # 动量周期
    lookback_days: int = 60
    
    # 行业数量
    top_sectors: int = 3
    
    # 动量阈值
    min_sector_momentum: float = 0.05
    
    # 风险控制
    max_sector_weight: float = 0.40
    min_sector_weight: float = 0.10


class SectorRotationStrategy:
    """
    行业轮动策略
    
    核心逻辑:
    - 计算各行业指数动量
    - 识别强势行业
    - 基于市场环境调整配置
    """
    
    # 行业分类（简化版）
    SECTOR_MAP = {
        '金融': ['银行', '证券', '保险', '多元金融'],
        '科技': ['半导体', '电子', '计算机', '通信'],
        '消费': ['食品饮料', '家用电器', '汽车', '医药生物'],
        '周期': ['有色金属', '钢铁', '煤炭', '化工'],
        '制造': ['机械设备', '电气设备', '军工', '汽车'],
        '基建': ['房地产', '建筑', '建材', '公用事业'],
        '能源': ['石油石化', '煤炭', '电力', '新能源'],
        '医药': ['医药生物', '医疗器械', '医疗服务', '中药']
    }
    
    def __init__(self, config: Optional[SectorConfig] = None):
        self.config = config or SectorConfig()
        self.name = "sector_rotation"
        
    def detect_market_regime(self, market_index: pd.DataFrame) -> MarketRegime:
        """检测市场环境"""
        if len(market_index) < 60:
            return MarketRegime.VOLATILE
        
        # 计算市场动量
        returns_20d = (market_index['close'].iloc[-1] - market_index['close'].iloc[-20]) / market_index['close'].iloc[-20]
        returns_60d = (market_index['close'].iloc[-1] - market_index['close'].iloc[-60]) / market_index['close'].iloc[-60]
        
        # 计算波动率
        market_index['daily_return'] = market_index['close'].pct_change()
        volatility = market_index['daily_return'].iloc[-20:].std() * np.sqrt(252)
        
        # 判断市场环境
        if returns_60d > 0.15 and returns_20d > 0:
            return MarketRegime.BULL
        elif returns_60d < -0.15 and returns_20d < 0:
            return MarketRegime.BEAR
        elif returns_60d > 0 and returns_20d < 0:
            return MarketRegime.RECOVERY
        else:
            return MarketRegime.VOLATILE
    
    def calculate_sector_momentum(self, sector_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """计算各行业动量"""
        sector_metrics = []
        
        for sector_name, df in sector_data.items():
            if len(df) < self.config.lookback_days:
                continue
            
            try:
                # 计算收益率
                returns_20d = (df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20] if len(df) >= 20 else 0
                returns_60d = (df['close'].iloc[-1] - df['close'].iloc[-60]) / df['close'].iloc[-60] if len(df) >= 60 else 0
                
                # 计算波动率
                df['daily_return'] = df['close'].pct_change()
                volatility = df['daily_return'].iloc[-20:].std() * np.sqrt(252) if len(df) >= 20 else 0
                
                # 计算夏普比率
                avg_return = df['daily_return'].iloc[-20:].mean() * 252 if len(df) >= 20 else 0
                sharpe = (avg_return - 0.03) / volatility if volatility > 0 else 0
                
                # 计算相对强弱
                sector_metrics.append({
                    'sector': sector_name,
                    'returns_20d': returns_20d,
                    'returns_60d': returns_60d,
                    'volatility': volatility,
                    'sharpe': sharpe,
                    'composite_score': returns_20d * 0.4 + returns_60d * 0.4 + sharpe * 0.2
                })
                
            except Exception as e:
                logger.warning(f"计算 {sector_name} 动量失败: {e}")
                continue
        
        return pd.DataFrame(sector_metrics)
    
    def get_sector_preferences(self, regime: MarketRegime) -> Dict[str, float]:
        """根据市场环境获取行业偏好"""
        preferences = {
            MarketRegime.BULL: {
                '科技': 1.5,
                '消费': 1.3,
                '制造': 1.2,
                '周期': 1.0,
                '金融': 0.8,
                '基建': 0.7,
                '能源': 0.8,
                '医药': 1.0
            },
            MarketRegime.BEAR: {
                '医药': 1.5,
                '消费': 1.3,
                '金融': 1.2,
                '基建': 1.0,
                '能源': 0.9,
                '科技': 0.6,
                '周期': 0.5,
                '制造': 0.7
            },
            MarketRegime.RECOVERY: {
                '周期': 1.5,
                '金融': 1.3,
                '制造': 1.2,
                '基建': 1.1,
                '科技': 1.0,
                '消费': 0.9,
                '能源': 1.0,
                '医药': 0.8
            },
            MarketRegime.VOLATILE: {
                '医药': 1.3,
                '消费': 1.2,
                '金融': 1.1,
                '科技': 0.9,
                '周期': 0.8,
                '制造': 0.9,
                '基建': 1.0,
                '能源': 0.9
            }
        }
        
        return preferences.get(regime, preferences[MarketRegime.VOLATILE])
    
    def select_sectors(self, sector_data: Dict[str, pd.DataFrame], 
                      market_index: pd.DataFrame) -> List[Dict]:
        """
        行业选择主函数
        
        Args:
            sector_data: 行业名称 -> 行业指数数据的字典
            market_index: 市场指数数据
        
        Returns:
            行业配置信号列表
        """
        logger.info(f"行业轮动分析开始，行业数: {len(sector_data)}")
        
        # 检测市场环境
        regime = self.detect_market_regime(market_index)
        logger.info(f"当前市场环境: {regime.value}")
        
        # 计算行业动量
        sector_momentum = self.calculate_sector_momentum(sector_data)
        
        if sector_momentum.empty:
            logger.warning("行业动量计算失败")
            return []
        
        # 获取行业偏好权重
        preferences = self.get_sector_preferences(regime)
        
        # 应用偏好权重
        sector_momentum['preference_weight'] = sector_momentum['sector'].map(
            lambda x: preferences.get(x, 1.0)
        )
        
        sector_momentum['final_score'] = (
            sector_momentum['composite_score'] * sector_momentum['preference_weight']
        )
        
        # 筛选强势行业
        strong_sectors = sector_momentum[
            sector_momentum['returns_60d'] > self.config.min_sector_momentum
        ]
        
        if strong_sectors.empty:
            logger.warning("没有强势行业")
            return []
        
        # 排序并选取Top N
        selected = strong_sectors.nlargest(self.config.top_sectors, 'final_score')
        
        # 生成配置信号
        signals = []
        total_score = selected['final_score'].sum()
        
        for _, row in selected.iterrows():
            # 计算权重
            weight = row['final_score'] / total_score if total_score > 0 else 0
            weight = max(self.config.min_sector_weight, 
                        min(self.config.max_sector_weight, weight))
            
            signals.append({
                'sector': row['sector'],
                'strategy': self.name,
                'action': 'overweight',
                'weight': weight,
                'returns_20d': row['returns_20d'],
                'returns_60d': row['returns_60d'],
                'volatility': row['volatility'],
                'sharpe': row['sharpe'],
                'score': row['final_score'],
                'regime': regime.value,
                'confidence': min(abs(row['final_score']) * 2, 1.0)
            })
        
        logger.info(f"选中 {len(signals)} 个行业")
        return signals
    
    def analyze_sector_rotation(self, sector_data: Dict[str, pd.DataFrame],
                               lookback_months: int = 12) -> Dict:
        """分析行业轮动规律"""
        logger.info(f"分析行业轮动规律，回溯 {lookback_months} 个月")
        
        rotation_analysis = {
            'sector_performance': {},
            'rotation_patterns': [],
            'best_sectors_by_month': {}
        }
        
        # 计算各行业历史表现
        for sector_name, df in sector_data.items():
            if len(df) < 60:
                continue
            
            try:
                # 计算月度收益
                df['month'] = pd.to_datetime(df['trade_date']).dt.to_period('M')
                monthly_returns = df.groupby('month').apply(
                    lambda x: (x['close'].iloc[-1] - x['close'].iloc[0]) / x['close'].iloc[0]
                )
                
                rotation_analysis['sector_performance'][sector_name] = {
                    'avg_monthly_return': float(monthly_returns.mean()),
                    'volatility': float(monthly_returns.std()),
                    'sharpe': float(monthly_returns.mean() / monthly_returns.std()) if monthly_returns.std() > 0 else 0,
                    'win_rate': float((monthly_returns > 0).sum() / len(monthly_returns)),
                    'best_month': str(monthly_returns.idxmax()),
                    'worst_month': str(monthly_returns.idxmin())
                }
                
            except Exception as e:
                logger.warning(f"分析 {sector_name} 失败: {e}")
                continue
        
        # 找出每月表现最好的行业
        if rotation_analysis['sector_performance']:
            # 简化的月度分析
            for month in range(1, 13):
                month_performance = {}
                for sector, perf in rotation_analysis['sector_performance'].items():
                    # 这里简化处理，实际应该按月计算
                    month_performance[sector] = perf['avg_monthly_return']
                
                best_sector = max(month_performance.items(), key=lambda x: x[1])
                rotation_analysis['best_sectors_by_month'][month] = {
                    'sector': best_sector[0],
                    'return': best_sector[1]
                }
        
        return rotation_analysis
    
    def get_strategy_info(self) -> Dict:
        """获取策略信息"""
        return {
            'name': self.name,
            'description': '基于行业动量的轮动策略',
            'config': {
                'lookback_days': self.config.lookback_days,
                'top_sectors': self.config.top_sectors,
                'min_sector_momentum': self.config.min_sector_momentum
            },
            'supported_sectors': list(self.SECTOR_MAP.keys())
        }


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 创建模拟数据
    np.random.seed(42)
    
    # 模拟行业指数
    sector_data = {}
    sectors = ['金融', '科技', '消费', '周期', '制造', '基建', '能源', '医药']
    
    for sector in sectors:
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        
        # 不同行业有不同的收益特征
        if sector == '科技':
            returns = np.random.normal(0.002, 0.025, 100)  # 高收益高波动
        elif sector == '医药':
            returns = np.random.normal(0.001, 0.015, 100)  # 稳定收益
        elif sector == '周期':
            returns = np.random.normal(0.0005, 0.03, 100)  # 高波动
        else:
            returns = np.random.normal(0.001, 0.02, 100)
        
        prices = 1000 * np.exp(np.cumsum(returns))
        
        df = pd.DataFrame({
            'trade_date': dates,
            'close': prices
        })
        
        sector_data[sector] = df
    
    # 模拟市场指数
    market_dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
    market_returns = np.random.normal(0.001, 0.018, 100)
    market_prices = 3000 * np.exp(np.cumsum(market_returns))
    
    market_index = pd.DataFrame({
        'trade_date': market_dates,
        'close': market_prices
    })
    
    # 测试行业轮动策略
    print("\n=== 行业轮动策略测试 ===")
    strategy = SectorRotationStrategy()
    
    signals = strategy.select_sectors(sector_data, market_index)
    print(f"\n选中 {len(signals)} 个行业:")
    for s in signals:
        print(f"  {s['sector']}: 权重={s['weight']:.2%}, 60日收益={s['returns_60d']:.2%}")
    
    # 分析行业轮动规律
    print("\n=== 行业轮动规律分析 ===")
    rotation = strategy.analyze_sector_rotation(sector_data)
    print("各行业表现:")
    for sector, perf in rotation['sector_performance'].items():
        print(f"  {sector}: 月均收益={perf['avg_monthly_return']:.2%}, 胜率={perf['win_rate']:.2%}")
