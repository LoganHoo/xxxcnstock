from typing import Dict, List
import logging

from core.logger import setup_logger

logger = setup_logger("fundamental_filter")


class FundamentalFilter:
    """基本面筛选器
    
    基于第一性原理：股票内在价值 = 盈利能力 + 成长性 + 财务健康
    """
    
    # 筛选条件配置
    CONDITIONS = {
        "pe": {"min": 0, "max": 50, "weight": 0.15},      # 市盈率
        "pb": {"min": 0, "max": 10, "weight": 0.10},      # 市净率
        "roe": {"min": 10, "max": 100, "weight": 0.20},   # ROE
        "revenue_growth": {"min": 15, "max": 200, "weight": 0.15},  # 营收增长
        "profit_growth": {"min": 10, "max": 200, "weight": 0.20},   # 利润增长
        "debt_ratio": {"min": 0, "max": 60, "weight": 0.20}  # 负债率
    }
    
    def calculate_score(self, data: Dict) -> float:
        """
        计算基本面评分
        
        Args:
            data: 包含 pe, pb, roe, revenue_growth, profit_growth, debt_ratio
        Returns:
            评分 0-100
        """
        total_score = 0.0
        total_weight = 0.0
        
        for metric, config in self.CONDITIONS.items():
            value = data.get(metric)
            if value is None:
                continue
            
            weight = config["weight"]
            min_val = config["min"]
            max_val = config["max"]
            
            # 计算该项得分（0-100）
            if metric == "debt_ratio":
                # 负债率越低越好
                if value <= min_val:
                    item_score = 100
                elif value >= max_val:
                    item_score = 0
                else:
                    item_score = 100 - (value - min_val) / (max_val - min_val) * 100
            elif metric in ["pe", "pb"]:
                # PE/PB合理区间得分高
                mid = (min_val + max_val) / 2
                if min_val < value < max_val:
                    item_score = 100 - abs(value - mid) / mid * 50
                else:
                    item_score = 0
            else:
                # ROE、增长率等越高越好
                if value >= max_val:
                    item_score = 100
                elif value <= min_val:
                    item_score = 0
                else:
                    item_score = (value - min_val) / (max_val - min_val) * 100
            
            total_score += item_score * weight
            total_weight += weight
        
        if total_weight > 0:
            return round(total_score / total_weight, 2)
        return 0.0
    
    def filter(self, data: Dict) -> Dict:
        """
        执行筛选
        
        Returns:
            {
                "passed": bool,
                "score": float,
                "reasons": List[str]
            }
        """
        reasons = []
        passed = True
        
        # 检查各项条件
        pe = data.get("pe", 999)
        if pe <= 0 or pe > 50:
            reasons.append(f"PE={pe}不在合理区间(0,50)")
            passed = False
        
        roe = data.get("roe", 0)
        if roe < 10:
            reasons.append(f"ROE={roe}%低于10%")
            passed = False
        
        debt_ratio = data.get("debt_ratio", 999)
        if debt_ratio > 60:
            reasons.append(f"负债率={debt_ratio}%超过60%")
            passed = False
        
        revenue_growth = data.get("revenue_growth", 0)
        if revenue_growth < 15:
            reasons.append(f"营收增长={revenue_growth}%低于15%")
        
        score = self.calculate_score(data)
        
        if score >= 70:
            reasons.append("基本面优秀")
        elif score >= 50:
            reasons.append("基本面良好")
        
        return {
            "passed": passed,
            "score": score,
            "reasons": reasons
        }
