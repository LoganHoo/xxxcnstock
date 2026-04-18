#!/usr/bin/env python3
"""
财务风险检测器
检测财务报表中的潜在风险
"""
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RiskThresholds:
    """风险阈值配置"""
    receivable_growth_max: float = 50.0      # 应收账款最大增长率
    inventory_turnover_min: float = -10.0    # 存货周转率最小变化
    debt_ratio_max: float = 70.0             # 最大负债率
    current_ratio_min: float = 1.0           # 最小流动比率
    cash_flow_ratio_min: float = 0.5         # 最小经营现金流/净利润比


class FinancialRiskDetector:
    """
    财务风险检测器
    
    检测以下风险:
    1. 应收账款异常增长
    2. 存货周转率下降
    3. 负债率过高
    4. 流动比率过低
    5. 现金流恶化
    """
    
    def __init__(self, thresholds: Optional[RiskThresholds] = None):
        self.thresholds = thresholds or RiskThresholds()
    
    def detect(self, financial_data: Dict[str, Any]) -> List[str]:
        """
        检测财务风险
        
        Args:
            financial_data: 财务数据字典
                - receivable_growth: 应收账款增长率
                - inventory_turnover: 存货周转率变化
                - debt_ratio: 负债率
                - current_ratio: 流动比率
                - cash_flow_ratio: 现金流比率
        
        Returns:
            风险列表
        """
        risks = []
        
        # 检测应收账款风险
        receivable_risks = self._detect_receivable_risk(financial_data)
        risks.extend(receivable_risks)
        
        # 检测存货风险
        inventory_risks = self._detect_inventory_risk(financial_data)
        risks.extend(inventory_risks)
        
        # 检测负债风险
        debt_risks = self._detect_debt_risk(financial_data)
        risks.extend(debt_risks)
        
        # 检测现金流风险
        cashflow_risks = self._detect_cashflow_risk(financial_data)
        risks.extend(cashflow_risks)
        
        if risks:
            logger.warning(f"Detected {len(risks)} financial risks: {risks}")
        else:
            logger.info("No financial risks detected")
        
        return risks
    
    def _detect_receivable_risk(self, data: Dict[str, Any]) -> List[str]:
        """检测应收账款风险"""
        risks = []
        
        receivable_growth = data.get('receivable_growth', 0)
        if receivable_growth > self.thresholds.receivable_growth_max:
            risks.append(f"应收账款异常增长({receivable_growth:.1f}%)，可能存在回款风险")
        
        return risks
    
    def _detect_inventory_risk(self, data: Dict[str, Any]) -> List[str]:
        """检测存货风险"""
        risks = []
        
        inventory_turnover = data.get('inventory_turnover', 0)
        if inventory_turnover < self.thresholds.inventory_turnover_min:
            risks.append(f"存货周转率下降({inventory_turnover:.1f}%)，可能存在滞销风险")
        
        return risks
    
    def _detect_debt_risk(self, data: Dict[str, Any]) -> List[str]:
        """检测负债风险"""
        risks = []
        
        debt_ratio = data.get('debt_ratio', 0)
        if debt_ratio > self.thresholds.debt_ratio_max:
            risks.append(f"负债率过高({debt_ratio:.1f}%)，偿债压力大")
        
        current_ratio = data.get('current_ratio', 2.0)
        if current_ratio < self.thresholds.current_ratio_min:
            risks.append(f"流动比率过低({current_ratio:.2f})，短期偿债能力不足")
        
        return risks
    
    def _detect_cashflow_risk(self, data: Dict[str, Any]) -> List[str]:
        """检测现金流风险"""
        risks = []
        
        cash_flow_ratio = data.get('cash_flow_ratio', 1.0)
        if cash_flow_ratio < self.thresholds.cash_flow_ratio_min:
            risks.append(f"经营现金流/净利润比率过低({cash_flow_ratio:.2f})，盈利质量存疑")
        
        return risks
    
    def assess_risk_level(self, financial_data: Dict[str, Any]) -> str:
        """
        评估风险等级
        
        Returns:
            风险等级: 'low', 'medium', 'high'
        """
        risks = self.detect(financial_data)
        
        if len(risks) >= 3:
            return 'high'
        elif len(risks) >= 1:
            return 'medium'
        else:
            return 'low'
    
    def generate_risk_report(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成风险报告
        
        Returns:
            风险报告字典
        """
        risks = self.detect(financial_data)
        risk_level = self.assess_risk_level(financial_data)
        
        return {
            'risk_level': risk_level,
            'risk_count': len(risks),
            'risks': risks,
            'recommendation': self._get_recommendation(risk_level)
        }
    
    def _get_recommendation(self, risk_level: str) -> str:
        """获取风险建议"""
        recommendations = {
            'low': '财务状况良好，可正常关注',
            'medium': '存在一定风险，需谨慎评估',
            'high': '风险较高，建议回避'
        }
        return recommendations.get(risk_level, '请进一步分析')
