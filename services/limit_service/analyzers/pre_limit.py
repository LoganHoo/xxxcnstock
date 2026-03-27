from typing import Dict
import logging

from core.logger import setup_logger

logger = setup_logger("pre_limit_predictor")


class PreLimitPredictor:
    """涨停预判器
    
    基于第一性原理：涨停 = 供需失衡 + 情绪共振
    """
    
    # 因子权重
    FACTOR_WEIGHTS = {
        "price_momentum": 0.30,     # 价格动能
        "volume_energy": 0.25,      # 成交量能
        "seal_strength": 0.25,      # 封单强度（已涨停）
        "sector_effect": 0.20       # 板块效应
    }
    
    def predict(self, stock_data: Dict) -> Dict:
        """
        预测涨停概率
        
        Args:
            stock_data: {
                "change_pct": 涨幅,
                "volume_ratio": 量比,
                "turnover_rate": 换手率,
                "seal_amount": 封单金额（已涨停）,
                "seal_ratio": 封单/流通市值比,
                "sector_change": 板块涨幅,
                "sector_limit_count": 板块内涨停数
            }
        Returns:
            {
                "probability": 概率(0-100),
                "factors": 各因子得分,
                "prediction": 预判结果
            }
        """
        factors = {}
        
        # 1. 价格动能 (30%)
        change_pct = stock_data.get("change_pct", 0)
        
        if change_pct >= 9.9:
            factors["price_momentum"] = 100  # 已涨停
        elif change_pct >= 7:
            factors["price_momentum"] = 80 + (change_pct - 7) * 10
        elif change_pct >= 5:
            factors["price_momentum"] = 50 + (change_pct - 5) * 15
        else:
            factors["price_momentum"] = change_pct * 10
        
        # 2. 成交量能 (25%)
        volume_ratio = stock_data.get("volume_ratio", 1)
        turnover_rate = stock_data.get("turnover_rate", 0)
        
        volume_score = min(volume_ratio * 30, 80)
        turnover_score = 0
        if 3 <= turnover_rate <= 10:
            turnover_score = 100
        elif turnover_rate > 10:
            turnover_score = max(0, 100 - (turnover_rate - 10) * 5)
        
        factors["volume_energy"] = (volume_score + turnover_score) / 2
        
        # 3. 封单强度 (25%) - 仅对已涨停股票
        seal_amount = stock_data.get("seal_amount", 0)
        seal_ratio = stock_data.get("seal_ratio", 0)
        
        if seal_amount > 0:
            if seal_ratio >= 5:
                factors["seal_strength"] = 100
            elif seal_ratio >= 2:
                factors["seal_strength"] = 80
            elif seal_ratio >= 1:
                factors["seal_strength"] = 60
            else:
                factors["seal_strength"] = 40
        else:
            factors["seal_strength"] = 50
        
        # 4. 板块效应 (20%)
        sector_change = stock_data.get("sector_change", 0)
        sector_limit_count = stock_data.get("sector_limit_count", 0)
        
        sector_score = min(50 + sector_change * 10, 100)
        limit_bonus = min(sector_limit_count * 5, 30)
        factors["sector_effect"] = min(sector_score + limit_bonus, 100)
        
        # 计算综合概率
        probability = sum(
            factors[k] * self.FACTOR_WEIGHTS[k] 
            for k in self.FACTOR_WEIGHTS
        )
        
        # 预判结果
        if probability >= 80:
            prediction = "极高"
        elif probability >= 60:
            prediction = "较高"
        elif probability >= 40:
            prediction = "中等"
        else:
            prediction = "较低"
        
        return {
            "probability": round(probability, 2),
            "factors": factors,
            "prediction": prediction
        }
