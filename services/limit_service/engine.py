from typing import List, Dict
import logging

from core.models import LimitUpSignal, SignalLevel
from core.logger import setup_logger, get_signal_logger
from services.limit_service.analyzers.pre_limit import PreLimitPredictor

logger = setup_logger("limit_engine", log_file="signals/limit_engine.log")
signal_logger = get_signal_logger()


class LimitUpEngine:
    """打板引擎"""
    
    def __init__(self):
        self.pre_limit_predictor = PreLimitPredictor()
    
    async def analyze_limit_stock(self, stock_data: Dict) -> LimitUpSignal:
        """分析涨停股票"""
        code = stock_data["code"]
        name = stock_data["name"]
        
        # 预判
        prediction = self.pre_limit_predictor.predict(stock_data)
        
        # 评估封板强度
        seal_strength = self._evaluate_seal_strength(stock_data)
        
        # 分析涨停原因
        reasons = self._analyze_reasons(stock_data)
        
        # 次日预判
        next_day_predict = self._predict_next_day(stock_data, seal_strength)
        
        # 确定信号等级
        signal_level = self._determine_signal_level(stock_data, seal_strength)
        
        # 操作建议
        suggestion = self._get_suggestion(signal_level, seal_strength)
        
        signal = LimitUpSignal(
            code=code,
            name=name,
            change_pct=stock_data.get("change_pct", 10),
            limit_time=stock_data.get("limit_time", ""),
            seal_amount=stock_data.get("seal_amount", 0),
            seal_ratio=stock_data.get("seal_ratio", 0),
            continuous_limit=stock_data.get("continuous_limit", 1),
            open_count=stock_data.get("open_count", 0),
            reasons=reasons,
            signal_level=signal_level,
            next_day_predict=next_day_predict,
            suggestion=suggestion
        )
        
        # 记录信号日志
        logger.info(f"涨停信号: {code} {name} - 等级{signal_level.value}")
        
        return signal
    
    def _evaluate_seal_strength(self, data: Dict) -> str:
        """评估封板强度"""
        seal_ratio = data.get("seal_ratio", 0)
        limit_time = data.get("limit_time", "15:00:00")
        open_count = data.get("open_count", 0)
        
        # 强势封板：封单大、封板早、未开板
        if seal_ratio >= 5 and limit_time < "10:00:00" and open_count == 0:
            return "强势"
        # 中等封板
        elif seal_ratio >= 2 and open_count <= 2:
            return "中等"
        else:
            return "弱势"
    
    def _analyze_reasons(self, data: Dict) -> List[str]:
        """分析涨停原因"""
        reasons = []
        
        if data.get("continuous_limit", 1) == 1:
            reasons.append("首板")
        else:
            reasons.append(f"{data.get('continuous_limit')}连板")
        
        if data.get("sector"):
            reasons.append(f"板块: {data.get('sector')}")
        
        if data.get("seal_amount", 0) > 100000:  # 封单过亿
            reasons.append("大封单")
        
        return reasons
    
    def _predict_next_day(self, data: Dict, seal_strength: str) -> str:
        """次日预判"""
        continuous = data.get("continuous_limit", 1)
        
        if seal_strength == "强势" and continuous == 1:
            return "高开/一字板"
        elif seal_strength == "强势":
            return "高开加速"
        elif seal_strength == "中等":
            return "高开震荡"
        else:
            return "平开/低开"
    
    def _determine_signal_level(self, data: Dict, seal_strength: str) -> SignalLevel:
        """确定信号等级"""
        continuous = data.get("continuous_limit", 1)
        open_count = data.get("open_count", 0)
        
        # S级：龙头 + 首板 + 强封
        if continuous == 1 and seal_strength == "强势" and open_count == 0:
            return SignalLevel.S
        # A级：首板 + 中强封
        elif continuous == 1 and seal_strength in ["强势", "中等"]:
            return SignalLevel.A
        # B级：连板或中等封板
        elif seal_strength == "中等":
            return SignalLevel.B
        else:
            return SignalLevel.C
    
    def _get_suggestion(self, signal_level: SignalLevel, seal_strength: str) -> str:
        """获取操作建议"""
        if signal_level == SignalLevel.S:
            return "竞价可参与"
        elif signal_level == SignalLevel.A:
            return "竞价观望，封板确认后可参与"
        elif signal_level == SignalLevel.B:
            return "谨慎参与，观察封板强度"
        else:
            return "观望为主"
    
    async def analyze_limit_pool(self, stocks: List[Dict]) -> List[LimitUpSignal]:
        """分析涨停池"""
        signals = []
        
        for stock in stocks:
            try:
                signal = await self.analyze_limit_stock(stock)
                signals.append(signal)
            except Exception as e:
                logger.error(f"分析涨停股失败: {stock.get('code')}, {e}")
        
        # 按信号等级排序
        level_order = {SignalLevel.S: 0, SignalLevel.A: 1, SignalLevel.B: 2, SignalLevel.C: 3}
        signals.sort(key=lambda x: level_order[x.signal_level])
        
        logger.info(f"涨停池分析完成: {len(signals)} 只")
        return signals
