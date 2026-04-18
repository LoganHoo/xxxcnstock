#!/usr/bin/env python3
"""
估值分析器
分析股票估值水平
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ValuationAnalyzer:
    """
    估值分析器
    
    分析方法:
    1. PE分析 - 相对行业和历史
    2. PB分析 - 结合ROE
    3. PEG分析 - 增长调整PE
    4. DCF估值 - 现金流折现
    """
    
    def __init__(self):
        pass
    
    def analyze_pe(self, pe: float, industry_avg_pe: Optional[float] = None, 
                   historical_avg_pe: Optional[float] = None) -> Dict[str, Any]:
        """
        分析PE估值
        
        Args:
            pe: 当前PE
            industry_avg_pe: 行业平均PE
            historical_avg_pe: 历史平均PE
        
        Returns:
            PE分析结果
        """
        result = {
            'current_pe': pe,
            'is_undervalued': False,
            'is_overvalued': False,
            'discount': 0.0,
            'premium': 0.0,
            'assessment': 'neutral'
        }
        
        # 与行业比较
        if industry_avg_pe is not None and industry_avg_pe > 0:
            discount = (industry_avg_pe - pe) / industry_avg_pe
            result['industry_discount'] = discount
            
            if discount >= 0.2:
                result['is_undervalued'] = True
                result['assessment'] = 'undervalued'
            elif discount <= -0.2:
                result['is_overvalued'] = True
                result['assessment'] = 'overvalued'
        
        # 与历史比较
        if historical_avg_pe is not None and historical_avg_pe > 0:
            historical_discount = (historical_avg_pe - pe) / historical_avg_pe
            result['historical_discount'] = historical_discount
        
        # 绝对估值判断
        if pe < 15:
            result['absolute_assessment'] = 'cheap'
        elif pe < 30:
            result['absolute_assessment'] = 'fair'
        else:
            result['absolute_assessment'] = 'expensive'
        
        return result
    
    def analyze_pb(self, pb: float, roe: Optional[float] = None,
                   industry_avg_pb: Optional[float] = None) -> Dict[str, Any]:
        """
        分析PB估值
        
        Args:
            pb: 当前PB
            roe: 净资产收益率
            industry_avg_pb: 行业平均PB
        
        Returns:
            PB分析结果
        """
        result = {
            'current_pb': pb,
            'pb_roe_ratio': None,
            'assessment': 'neutral'
        }
        
        # PB-ROE分析
        if roe is not None and roe > 0:
            pb_roe_ratio = pb / roe
            result['pb_roe_ratio'] = pb_roe_ratio
            
            # PB/ROE < 0.1 通常被认为是低估
            if pb_roe_ratio < 0.1:
                result['assessment'] = 'undervalued'
            elif pb_roe_ratio > 0.2:
                result['assessment'] = 'overvalued'
        
        # 与行业比较
        if industry_avg_pb is not None and industry_avg_pb > 0:
            discount = (industry_avg_pb - pb) / industry_avg_pb
            result['industry_discount'] = discount
        
        return result
    
    def calculate_peg(self, pe: float, growth_rate: float) -> Dict[str, Any]:
        """
        计算PEG比率
        
        PEG = PE / 增长率
        PEG < 1: 低估
        PEG > 2: 高估
        
        Args:
            pe: 市盈率
            growth_rate: 增长率 (%)
        
        Returns:
            PEG分析结果
        """
        if growth_rate <= 0:
            return {
                'peg': float('inf'),
                'assessment': 'negative_growth',
                'recommendation': 'Avoid'
            }
        
        peg = pe / growth_rate
        
        if peg < 1:
            assessment = 'undervalued'
            recommendation = 'Strong Buy'
        elif peg < 1.5:
            assessment = 'fair'
            recommendation = 'Buy'
        elif peg < 2:
            assessment = 'slightly_overvalued'
            recommendation = 'Hold'
        else:
            assessment = 'overvalued'
            recommendation = 'Sell'
        
        return {
            'peg': peg,
            'assessment': assessment,
            'recommendation': recommendation
        }
    
    def comprehensive_valuation(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """
        综合估值分析
        
        Args:
            metrics: 估值指标
                - pe: 市盈率
                - pb: 市净率
                - roe: 净资产收益率
                - growth_rate: 增长率
        
        Returns:
            综合估值结果
        """
        pe = metrics.get('pe', 0)
        pb = metrics.get('pb', 0)
        roe = metrics.get('roe', 0)
        growth_rate = metrics.get('growth_rate', 0)
        
        # PE分析
        pe_analysis = self.analyze_pe(pe)
        
        # PB分析
        pb_analysis = self.analyze_pb(pb, roe)
        
        # PEG分析
        peg_analysis = self.calculate_peg(pe, growth_rate)
        
        # 综合评分
        score = 50  # 基础分
        
        if pe_analysis.get('is_undervalued'):
            score += 20
        elif pe_analysis.get('is_overvalued'):
            score -= 20
        
        if pb_analysis.get('assessment') == 'undervalued':
            score += 15
        elif pb_analysis.get('assessment') == 'overvalued':
            score -= 15
        
        if peg_analysis.get('peg', 2) < 1:
            score += 15
        elif peg_analysis.get('peg', 2) > 2:
            score -= 15
        
        score = max(0, min(100, score))
        
        # 投资建议
        if score >= 70:
            recommendation = '买入'
        elif score >= 50:
            recommendation = '持有'
        else:
            recommendation = '观望'
        
        return {
            'score': score,
            'recommendation': recommendation,
            'pe_analysis': pe_analysis,
            'pb_analysis': pb_analysis,
            'peg_analysis': peg_analysis
        }
