#!/usr/bin/env python3
"""
财务筛选器
基于财务指标筛选优质股票
"""
import pandas as pd
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ScreenCriteria:
    """筛选条件"""
    roe_min: float = 10.0              # 最小ROE
    gross_margin_min: float = 20.0     # 最小毛利率
    profit_growth_min: float = 20.0    # 最小利润增长率
    pe_max: float = 50.0               # 最大PE
    pb_max: float = 10.0               # 最大PB
    debt_ratio_max: float = 70.0       # 最大负债率
    revenue_growth_min: float = 20.0   # 最小营收增长率


class FinancialScreener:
    """
    财务筛选器
    
    筛选条件:
    1. ROE >= 10%
    2. 毛利率 >= 20%
    3. 净利润增长率 >= 20%
    4. PE <= 50
    5. PB <= 10
    """
    
    def __init__(self, criteria: Optional[ScreenCriteria] = None):
        self.criteria = criteria or ScreenCriteria()
    
    def screen(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """
        基本面筛选
        
        Args:
            stocks: 股票数据DataFrame
                - roe: 净资产收益率
                - gross_margin: 毛利率
                - profit_growth: 净利润增长率
        
        Returns:
            筛选后的DataFrame
        """
        filtered = stocks.copy()
        initial_count = len(filtered)
        
        # ROE筛选
        if 'roe' in filtered.columns:
            filtered = filtered[filtered['roe'] >= self.criteria.roe_min]
            logger.debug(f"After ROE filter: {len(filtered)}/{initial_count}")
        
        # 毛利率筛选
        if 'gross_margin' in filtered.columns:
            filtered = filtered[filtered['gross_margin'] >= self.criteria.gross_margin_min]
            logger.debug(f"After gross margin filter: {len(filtered)}")
        
        # 净利润增长率筛选
        if 'profit_growth' in filtered.columns:
            filtered = filtered[filtered['profit_growth'] >= self.criteria.profit_growth_min]
            logger.debug(f"After profit growth filter: {len(filtered)}")
        
        logger.info(f"Fundamental screening: {initial_count} -> {len(filtered)}")
        return filtered
    
    def screen_by_valuation(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """
        估值筛选
        
        Args:
            stocks: 股票数据DataFrame
                - pe: 市盈率
                - pb: 市净率
        
        Returns:
            筛选后的DataFrame
        """
        filtered = stocks.copy()
        initial_count = len(filtered)
        
        # PE筛选
        if 'pe' in filtered.columns:
            filtered = filtered[filtered['pe'] <= self.criteria.pe_max]
            logger.debug(f"After PE filter: {len(filtered)}/{initial_count}")
        
        # PB筛选
        if 'pb' in filtered.columns:
            filtered = filtered[filtered['pb'] <= self.criteria.pb_max]
            logger.debug(f"After PB filter: {len(filtered)}")
        
        logger.info(f"Valuation screening: {initial_count} -> {len(filtered)}")
        return filtered
    
    def screen_by_growth(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """
        成长性筛选
        
        Args:
            stocks: 股票数据DataFrame
                - revenue_growth: 营收增长率
                - profit_growth: 净利润增长率
        
        Returns:
            筛选后的DataFrame
        """
        filtered = stocks.copy()
        initial_count = len(filtered)
        
        # 营收增长率筛选
        if 'revenue_growth' in filtered.columns:
            filtered = filtered[filtered['revenue_growth'] >= self.criteria.revenue_growth_min]
            logger.debug(f"After revenue growth filter: {len(filtered)}/{initial_count}")
        
        # 净利润增长率筛选
        if 'profit_growth' in filtered.columns:
            filtered = filtered[filtered['profit_growth'] >= self.criteria.profit_growth_min]
            logger.debug(f"After profit growth filter: {len(filtered)}")
        
        logger.info(f"Growth screening: {initial_count} -> {len(filtered)}")
        return filtered
    
    def comprehensive_screen(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """
        综合筛选
        
        同时应用基本面、估值、成长性筛选
        """
        filtered = self.screen(stocks)
        filtered = self.screen_by_valuation(filtered)
        filtered = self.screen_by_growth(filtered)
        return filtered
    
    def rank_by_quality(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """
        按质量评分排序
        
        计算综合质量评分:
        - ROE得分 (30%)
        - 毛利率得分 (20%)
        - 增长率得分 (30%)
        - 估值得分 (20%)
        """
        df = stocks.copy()
        
        # 计算各项得分 (0-100)
        if 'roe' in df.columns:
            df['roe_score'] = df['roe'].clip(0, 30) * 100 / 30
        else:
            df['roe_score'] = 50
        
        if 'gross_margin' in df.columns:
            df['margin_score'] = df['gross_margin'].clip(0, 50) * 100 / 50
        else:
            df['margin_score'] = 50
        
        if 'profit_growth' in df.columns:
            df['growth_score'] = df['profit_growth'].clip(0, 50) * 100 / 50
        else:
            df['growth_score'] = 50
        
        if 'pe' in df.columns:
            # PE越低越好
            df['pe_score'] = (50 - df['pe'].clip(0, 50)) * 100 / 50
        else:
            df['pe_score'] = 50
        
        # 综合得分
        df['quality_score'] = (
            df['roe_score'] * 0.3 +
            df['margin_score'] * 0.2 +
            df['growth_score'] * 0.3 +
            df['pe_score'] * 0.2
        )
        
        # 按得分排序
        return df.sort_values('quality_score', ascending=False)
