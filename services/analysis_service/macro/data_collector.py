#!/usr/bin/env python3
"""
宏观数据收集器
采集Shibor利率、宏观指标等数据
"""
import pandas as pd
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MacroDataCollector:
    """宏观数据收集器"""
    
    def __init__(self):
        self.data_cache = {}
    
    def fetch_shibor(self, days: int = 30) -> pd.DataFrame:
        """
        获取Shibor数据
        
        Args:
            days: 获取天数
        
        Returns:
            Shibor数据DataFrame
        """
        try:
            # 尝试使用akshare获取Shibor数据
            import akshare as ak
            
            # 获取Shibor数据
            shibor_df = ak.rate_interbank(
                market="上海银行同业拆借市场",
                symbol="Shibor人民币",
                indicator="隔夜"
            )
            
            if shibor_df is not None and not shibor_df.empty:
                # 重命名列
                shibor_df = shibor_df.rename(columns={
                    '日期': 'date',
                    '利率': 'shibor_overnight'
                })
                shibor_df['date'] = pd.to_datetime(shibor_df['date'])
                shibor_df = shibor_df.set_index('date')
                
                # 获取其他期限数据
                for term, col_name in [
                    ('1周', 'shibor_1w'),
                    ('2周', 'shibor_2w'),
                    ('1月', 'shibor_1m'),
                    ('3月', 'shibor_3m'),
                    ('6月', 'shibor_6m'),
                    ('9月', 'shibor_9m'),
                    ('1年', 'shibor_1y')
                ]:
                    try:
                        term_df = ak.rate_interbank(
                            market="上海银行同业拆借市场",
                            symbol="Shibor人民币",
                            indicator=term
                        )
                        if term_df is not None and not term_df.empty:
                            shibor_df[col_name] = term_df['利率'].values
                    except Exception as e:
                        logger.warning(f"Failed to fetch Shibor {term}: {e}")
                
                return shibor_df.tail(days)
            
        except Exception as e:
            logger.warning(f"Failed to fetch Shibor from akshare: {e}")
        
        # 返回模拟数据用于测试
        logger.info("Returning mock Shibor data for testing")
        dates = pd.date_range(end=datetime.now(), periods=days, freq='B')
        mock_data = pd.DataFrame({
            'shibor_overnight': [1.5 + i * 0.01 for i in range(days)],
            'shibor_1w': [2.0 + i * 0.01 for i in range(days)],
            'shibor_1m': [2.3 + i * 0.01 for i in range(days)],
            'shibor_3m': [2.5 + i * 0.01 for i in range(days)],
            'shibor_6m': [2.7 + i * 0.01 for i in range(days)],
            'shibor_1y': [2.9 + i * 0.01 for i in range(days)],
        }, index=dates)
        
        return mock_data
    
    def fetch_macro_indicators(self) -> Dict[str, Any]:
        """
        获取宏观指标
        
        Returns:
            宏观指标字典
        """
        indicators = {}
        
        try:
            # 获取Shibor数据
            shibor_data = self.fetch_shibor(days=30)
            
            if not shibor_data.empty:
                # 计算Shibor趋势
                recent = shibor_data['shibor_1w'].tail(5).mean()
                previous = shibor_data['shibor_1w'].head(5).mean()
                
                if recent < previous * 0.98:
                    indicators['shibor_trend'] = 'down'
                elif recent > previous * 1.02:
                    indicators['shibor_trend'] = 'up'
                else:
                    indicators['shibor_trend'] = 'neutral'
                
                # 当前Shibor值
                indicators['current_shibor_1w'] = shibor_data['shibor_1w'].iloc[-1]
                indicators['current_shibor_1m'] = shibor_data['shibor_1m'].iloc[-1]
                
                # 计算流动性评分 (基于Shibor水平)
                current_shibor = indicators['current_shibor_1w']
                # Shibor越低，流动性越好，评分越高
                indicators['liquidity_score'] = max(0, min(100, 100 - current_shibor * 20))
            
        except Exception as e:
            logger.error(f"Failed to fetch macro indicators: {e}")
            # 返回默认值
            indicators = {
                'shibor_trend': 'neutral',
                'current_shibor_1w': 2.0,
                'current_shibor_1m': 2.3,
                'liquidity_score': 50
            }
        
        return indicators
    
    def fetch_m2_money_supply(self) -> Optional[pd.DataFrame]:
        """
        获取M2货币供应量数据
        
        Returns:
            M2数据DataFrame
        """
        try:
            import akshare as ak
            
            m2_df = ak.macro_china_m2_yearly()
            
            if m2_df is not None and not m2_df.empty:
                return m2_df
            
        except Exception as e:
            logger.warning(f"Failed to fetch M2 data: {e}")
        
        return None
    
    def fetch_ppi_cpi(self) -> Dict[str, pd.DataFrame]:
        """
        获取PPI和CPI数据
        
        Returns:
            {'ppi': DataFrame, 'cpi': DataFrame}
        """
        result = {'ppi': None, 'cpi': None}
        
        try:
            import akshare as ak
            
            # PPI
            result['ppi'] = ak.macro_china_ppi_yearly()
            
            # CPI
            result['cpi'] = ak.macro_china_cpi_yearly()
            
        except Exception as e:
            logger.warning(f"Failed to fetch PPI/CPI data: {e}")
        
        return result
