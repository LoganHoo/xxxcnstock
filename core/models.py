from enum import Enum
from datetime import datetime
from typing import List
from pydantic import BaseModel, Field


class SignalLevel(str, Enum):
    """信号等级"""
    S = "S"  # 最高优先级
    A = "A"  # 高优先级
    B = "B"  # 中等优先级
    C = "C"  # 低优先级


class StockQuote(BaseModel):
    """股票行情"""
    code: str = Field(..., description="股票代码")
    name: str = Field(..., description="股票名称")
    price: float = Field(..., description="当前价格")
    change_pct: float = Field(..., description="涨跌幅%")
    volume: int = Field(default=0, description="成交量")
    turnover_rate: float = Field(default=0, description="换手率%")
    amount: float = Field(default=0, description="成交额")
    high: float = Field(default=0, description="最高价")
    low: float = Field(default=0, description="最低价")
    open: float = Field(default=0, description="开盘价")
    pre_close: float = Field(default=0, description="昨收")
    timestamp: datetime = Field(default_factory=datetime.now, description="时间戳")


class StockScore(BaseModel):
    """股票评分"""
    code: str
    name: str
    total_score: float = Field(..., ge=0, le=100)
    fundamental_score: float = Field(default=0, ge=0, le=100)
    volume_price_score: float = Field(default=0, ge=0, le=100)
    fund_flow_score: float = Field(default=0, ge=0, le=100)
    sentiment_score: float = Field(default=0, ge=0, le=100)
    reasons: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class LimitUpSignal(BaseModel):
    """涨停信号"""
    code: str
    name: str
    change_pct: float
    limit_time: str = Field(..., description="涨停时间")
    seal_amount: float = Field(..., description="封单金额")
    seal_ratio: float = Field(default=0, description="封单/流通市值比")
    continuous_limit: int = Field(default=1, description="连板数")
    open_count: int = Field(default=0, description="开板次数")
    reasons: List[str] = Field(default_factory=list)
    signal_level: SignalLevel
    next_day_predict: str = Field(default="", description="次日预判")
    suggestion: str = Field(default="", description="操作建议")
    timestamp: datetime = Field(default_factory=datetime.now)


class StockSelectionSignal(BaseModel):
    """选股信号"""
    code: str
    name: str
    score: StockScore
    current_price: float
    change_pct: float
    signal_type: str = Field(default="选股", description="信号类型")
    signal_level: SignalLevel
    reasons: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class NotificationMessage(BaseModel):
    """通知消息"""
    title: str
    content: str
    level: SignalLevel
    channels: List[str] = Field(default_factory=lambda: ["wechat", "dingtalk"])
    timestamp: datetime = Field(default_factory=datetime.now)
