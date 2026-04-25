#!/usr/bin/env python3
"""
动量选股策略

策略逻辑:
1. 基于20日/60日价格动量筛选股票
2. 结合成交量确认动量有效性
3. 动态调整持仓，追逐强势板块

使用方法:
    from services.strategy_service.momentum.strategy import MomentumStrategy
    
    strategy = MomentumStrategy(config)
    signals = strategy.select_stocks(market_data)
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

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

logger = logging.getLogger(__name__)


@dataclass
class MomentumConfig:
    """动量策略配置"""
    # 动量周期
    short_term_days: int = 20
    medium_term_days: int = 60
    
    # 筛选条件
    min_momentum_20d: float = 0.05  # 最小20日动量
    min_momentum_60d: float = 0.10  # 最小60日动量
    max_momentum_20d: float = 0.50  # 最大20日动量（避免过度追涨）
    
    # 成交量条件
    min_volume_ratio: float = 1.2  # 最小成交量比率
    
    # 波动率条件
    max_volatility: float = 0.60  # 最大波动率
    
    # 趋势条件
    require_uptrend: bool = True  # 要求处于上升趋势
    
    # 选股数量
    top_n: int = 20
    
    # 仓位管理
    position_pct: float = 0.10  # 单票仓位


class MomentumStrategy:
    """
    动量选股策略
    
    核心逻辑:
    - 短期动量(20日)捕捉近期强势
    - 中期动量(60日)确认趋势持续性
    - 成交量放大确认资金流入
    - 波动率控制降低风险
    """
    
    def __init__(self, config: Optional[MomentumConfig] = None):
        self.config = config or MomentumConfig()
        self.name = "momentum"
        
    def calculate_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算动量指标"""
        result = df.copy()
        
        # 计算收益率
        result['daily_return'] = result['close'].pct_change()
        
        # 短期动量 (20日)
        result['momentum_20d'] = (
            result['close'] - result['close'].shift(self.config.short_term_days)
        ) / result['close'].shift(self.config.short_term_days)
        
        # 中期动量 (60日)
        result['momentum_60d'] = (
            result['close'] - result['close'].shift(self.config.medium_term_days)
        ) / result['close'].shift(self.config.medium_term_days)
        
        # 动量加速度
        result['momentum_accel'] = result['momentum_20d'] - result['momentum_20d'].shift(5)
        
        # 成交量比率
        result['volume_ma20'] = result['volume'].rolling(20).mean()
        result['volume_ratio'] = result['volume'] / result['volume_ma20']
        
        # 波动率
        result['volatility'] = result['daily_return'].rolling(20).std() * np.sqrt(252)
        
        # 移动平均线
        result['ma20'] = result['close'].rolling(20).mean()
        result['ma60'] = result['close'].rolling(60).mean()
        
        # 趋势判断
        result['uptrend'] = (result['close'] > result['ma20']) & (result['ma20'] > result['ma60'])
        
        return result
    
    def filter_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """筛选符合条件的股票"""
        # 基本条件
        mask = (
            (df['momentum_20d'] >= self.config.min_momentum_20d) &
            (df['momentum_20d'] <= self.config.max_momentum_20d) &
            (df['momentum_60d'] >= self.config.min_momentum_60d) &
            (df['volume_ratio'] >= self.config.min_volume_ratio) &
            (df['volatility'] <= self.config.max_volatility)
        )
        
        # 趋势条件
        if self.config.require_uptrend:
            mask = mask & df['uptrend']
        
        return df[mask].copy()
    
    def score_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """对股票进行评分"""
        result = df.copy()
        
        # 动量得分 (短期动量权重更高)
        result['momentum_score'] = (
            result['momentum_20d'] * 0.6 +
            result['momentum_60d'] * 0.4
        )
        
        # 成交量得分
        result['volume_score'] = np.minimum(result['volume_ratio'] / 3, 1.0)
        
        # 动量加速度得分
        result['accel_score'] = np.where(
            result['momentum_accel'] > 0,
            np.minimum(result['momentum_accel'] * 10, 1.0),
            0
        )
        
        # 综合得分
        result['total_score'] = (
            result['momentum_score'] * 0.5 +
            result['volume_score'] * 0.3 +
            result['accel_score'] * 0.2
        )
        
        return result
    
    def select_stocks(self, market_data: Dict[str, pd.DataFrame]) -> List[Dict]:
        """
        选股主函数
        
        Args:
            market_data: 股票代码 -> K线数据的字典
        
        Returns:
            选股信号列表
        """
        logger.info(f"动量策略选股开始，股票池: {len(market_data)} 只")
        
        candidates = []
        
        for code, df in market_data.items():
            if len(df) < self.config.medium_term_days + 10:
                continue
            
            try:
                # 计算指标
                analyzed = self.calculate_momentum(df)
                
                # 取最新数据
                latest = analyzed.iloc[-1]
                
                # 检查是否有NaN
                if pd.isna(latest['momentum_20d']) or pd.isna(latest['momentum_60d']):
                    continue
                
                candidates.append({
                    'code': code,
                    'close': latest['close'],
                    'momentum_20d': latest['momentum_20d'],
                    'momentum_60d': latest['momentum_60d'],
                    'momentum_accel': latest['momentum_accel'],
                    'volume_ratio': latest['volume_ratio'],
                    'volatility': latest['volatility'],
                    'uptrend': latest['uptrend']
                })
                
            except Exception as e:
                logger.warning(f"分析 {code} 失败: {e}")
                continue
        
        if not candidates:
            logger.warning("没有符合条件的候选股票")
            return []
        
        # 转换为DataFrame
        candidates_df = pd.DataFrame(candidates)
        
        # 筛选
        filtered = self.filter_stocks(candidates_df)
        logger.info(f"筛选后候选股票: {len(filtered)} 只")
        
        if filtered.empty:
            return []
        
        # 评分
        scored = self.score_stocks(filtered)
        
        # 排序并选取Top N
        selected = scored.nlargest(self.config.top_n, 'total_score')
        
        # 生成信号
        signals = []
        for _, row in selected.iterrows():
            signals.append({
                'code': row['code'],
                'strategy': self.name,
                'action': 'buy',
                'price': row['close'],
                'score': row['total_score'],
                'momentum_20d': row['momentum_20d'],
                'momentum_60d': row['momentum_60d'],
                'volume_ratio': row['volume_ratio'],
                'confidence': min(row['total_score'] * 2, 1.0)
            })
        
        logger.info(f"最终选中股票: {len(signals)} 只")
        return signals
    
    def get_strategy_info(self) -> Dict:
        """获取策略信息"""
        return {
            'name': self.name,
            'description': '基于价格动量的选股策略',
            'config': {
                'short_term_days': self.config.short_term_days,
                'medium_term_days': self.config.medium_term_days,
                'min_momentum_20d': self.config.min_momentum_20d,
                'min_momentum_60d': self.config.min_momentum_60d,
                'top_n': self.config.top_n
            }
        }


class LowVolatilityStrategy:
    """
    低波动策略
    
    核心逻辑:
    - 选择历史波动率较低的股票
    - 追求稳定收益，降低回撤
    - 适合防御性配置
    """
    
    def __init__(self, config: Optional[MomentumConfig] = None):
        self.config = config or MomentumConfig()
        self.name = "low_volatility"
        
    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算指标"""
        result = df.copy()
        
        # 日收益率
        result['daily_return'] = result['close'].pct_change()
        
        # 波动率
        result['volatility_20d'] = result['daily_return'].rolling(20).std() * np.sqrt(252)
        result['volatility_60d'] = result['daily_return'].rolling(60).std() * np.sqrt(252)
        
        # 下行波动率
        downside_returns = result['daily_return'].where(result['daily_return'] < 0, 0)
        result['downside_vol'] = downside_returns.rolling(20).std() * np.sqrt(252)
        
        # 最大回撤
        result['cummax'] = result['close'].cummax()
        result['drawdown'] = (result['close'] - result['cummax']) / result['cummax']
        result['max_drawdown_20d'] = result['drawdown'].rolling(20).min()
        
        # 收益风险比
        result['return_vol_ratio'] = result['daily_return'].rolling(20).mean() / result['volatility_20d']
        
        return result
    
    def select_stocks(self, market_data: Dict[str, pd.DataFrame]) -> List[Dict]:
        """选股"""
        logger.info(f"低波动策略选股开始，股票池: {len(market_data)} 只")
        
        candidates = []
        
        for code, df in market_data.items():
            if len(df) < 60:
                continue
            
            try:
                analyzed = self.calculate_metrics(df)
                latest = analyzed.iloc[-1]
                
                if pd.isna(latest['volatility_20d']):
                    continue
                
                candidates.append({
                    'code': code,
                    'close': latest['close'],
                    'volatility_20d': latest['volatility_20d'],
                    'volatility_60d': latest['volatility_60d'],
                    'downside_vol': latest['downside_vol'],
                    'max_drawdown_20d': latest['max_drawdown_20d'],
                    'return_vol_ratio': latest['return_vol_ratio']
                })
                
            except Exception as e:
                continue
        
        if not candidates:
            return []
        
        candidates_df = pd.DataFrame(candidates)
        
        # 筛选低波动股票
        low_vol = candidates_df[
            (candidates_df['volatility_20d'] < 0.30) &
            (candidates_df['max_drawdown_20d'] > -0.10)
        ].copy()
        
        if low_vol.empty:
            return []
        
        # 按波动率排序（越低越好）
        low_vol['score'] = 1 / (low_vol['volatility_20d'] + 0.01)
        selected = low_vol.nsmallest(self.config.top_n, 'volatility_20d')
        
        signals = []
        for _, row in selected.iterrows():
            signals.append({
                'code': row['code'],
                'strategy': self.name,
                'action': 'buy',
                'price': row['close'],
                'volatility': row['volatility_20d'],
                'max_drawdown': row['max_drawdown_20d'],
                'confidence': 0.7
            })
        
        logger.info(f"低波动策略选中: {len(signals)} 只")
        return signals


class MultiFactorStrategy:
    """
    多因子复合策略
    
    结合动量、趋势、低波动三个因子
    """
    
    def __init__(self, config: Optional[MomentumConfig] = None):
        self.config = config or MomentumConfig()
        self.name = "multi_factor"
        self.momentum_strategy = MomentumStrategy(config)
        self.volatility_strategy = LowVolatilityStrategy(config)
        
    def select_stocks(self, market_data: Dict[str, pd.DataFrame]) -> List[Dict]:
        """多因子选股"""
        logger.info("多因子策略选股开始")
        
        # 收集所有股票的因子数据
        factor_data = []
        
        for code, df in market_data.items():
            if len(df) < 60:
                continue
            
            try:
                # 计算动量因子
                df['daily_return'] = df['close'].pct_change()
                momentum_20d = (df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20] if len(df) >= 20 else 0
                momentum_60d = (df['close'].iloc[-1] - df['close'].iloc[-60]) / df['close'].iloc[-60] if len(df) >= 60 else 0
                
                # 计算波动率因子
                volatility = df['daily_return'].iloc[-20:].std() * np.sqrt(252) if len(df) >= 20 else 0
                
                # 计算趋势因子
                ma20 = df['close'].iloc[-20:].mean()
                ma60 = df['close'].iloc[-60:].mean()
                trend_score = 1 if df['close'].iloc[-1] > ma20 > ma60 else 0
                
                # 成交量因子
                volume_ratio = df['volume'].iloc[-1] / df['volume'].iloc[-20:].mean() if len(df) >= 20 else 1
                
                factor_data.append({
                    'code': code,
                    'close': df['close'].iloc[-1],
                    'momentum_20d': momentum_20d,
                    'momentum_60d': momentum_60d,
                    'volatility': volatility,
                    'trend_score': trend_score,
                    'volume_ratio': volume_ratio
                })
                
            except Exception as e:
                continue
        
        if not factor_data:
            return []
        
        factor_df = pd.DataFrame(factor_data)
        
        # 因子标准化
        for col in ['momentum_20d', 'momentum_60d', 'volatility']:
            mean = factor_df[col].mean()
            std = factor_df[col].std()
            if std > 0:
                factor_df[f'{col}_z'] = (factor_df[col] - mean) / std
            else:
                factor_df[f'{col}_z'] = 0
        
        # 综合得分
        # 动量越高越好，波动率越低越好
        factor_df['total_score'] = (
            factor_df['momentum_20d_z'] * 0.3 +
            factor_df['momentum_60d_z'] * 0.2 -
            factor_df['volatility_z'] * 0.3 +
            factor_df['trend_score'] * 0.2
        )
        
        # 筛选条件
        filtered = factor_df[
            (factor_df['momentum_20d'] > 0) &
            (factor_df['volatility'] < 0.50) &
            (factor_df['volume_ratio'] > 1.0)
        ]
        
        if filtered.empty:
            return []
        
        # 选取Top N
        selected = filtered.nlargest(self.config.top_n, 'total_score')
        
        signals = []
        for _, row in selected.iterrows():
            signals.append({
                'code': row['code'],
                'strategy': self.name,
                'action': 'buy',
                'price': row['close'],
                'score': row['total_score'],
                'momentum_20d': row['momentum_20d'],
                'volatility': row['volatility'],
                'trend_score': row['trend_score'],
                'confidence': min(max(row['total_score'] + 1, 0), 1)
            })
        
        logger.info(f"多因子策略选中: {len(signals)} 只")
        return signals


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 创建模拟数据
    np.random.seed(42)
    
    market_data = {}
    for i in range(100):
        code = f"{600000 + i:06d}"
        
        # 生成随机价格序列
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        returns = np.random.normal(0.001, 0.02, 100)
        prices = 10 * np.exp(np.cumsum(returns))
        
        df = pd.DataFrame({
            'trade_date': dates,
            'open': prices * (1 + np.random.normal(0, 0.01, 100)),
            'high': prices * (1 + np.abs(np.random.normal(0, 0.02, 100))),
            'low': prices * (1 - np.abs(np.random.normal(0, 0.02, 100))),
            'close': prices,
            'volume': np.random.randint(1000000, 10000000, 100)
        })
        
        market_data[code] = df
    
    # 测试动量策略
    print("\n=== 动量策略测试 ===")
    momentum = MomentumStrategy()
    signals = momentum.select_stocks(market_data)
    print(f"选中 {len(signals)} 只股票")
    for s in signals[:5]:
        print(f"  {s['code']}: 得分={s['score']:.4f}, 20日动量={s['momentum_20d']:.2%}")
    
    # 测试低波动策略
    print("\n=== 低波动策略测试 ===")
    low_vol = LowVolatilityStrategy()
    signals = low_vol.select_stocks(market_data)
    print(f"选中 {len(signals)} 只股票")
    for s in signals[:5]:
        print(f"  {s['code']}: 波动率={s['volatility']:.2%}")
    
    # 测试多因子策略
    print("\n=== 多因子策略测试 ===")
    multi = MultiFactorStrategy()
    signals = multi.select_stocks(market_data)
    print(f"选中 {len(signals)} 只股票")
    for s in signals[:5]:
        print(f"  {s['code']}: 得分={s['score']:.4f}")
