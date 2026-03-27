"""
股票推荐验证系统 - 数据库模型
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List
from sqlalchemy import (
    Column, BigInteger, Integer, String, Text, DECIMAL, Date, DateTime, 
    SmallInteger, Boolean, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.ext.declarative import declared_attr

Base = declarative_base()


class StockRecommendation(Base):
    """股票推荐主表"""
    __tablename__ = 'stock_recommendation'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    recommend_date = Column(Date, nullable=False, comment='推荐日期(T日)')
    code = Column(String(10), nullable=False, comment='股票代码')
    name = Column(String(50), comment='股票名称')
    grade = Column(String(5), nullable=False, comment='评级(S/A/B/C)')
    score = Column(Integer, comment='推荐评分')
    recommend_price = Column(DECIMAL(10, 3), nullable=False, comment='推荐日收盘价')
    recommend_change = Column(DECIMAL(8, 2), comment='推荐日涨跌幅%')
    
    is_st = Column(SmallInteger, default=0, comment='是否ST股票')
    industry = Column(String(50), comment='所属行业')
    market_cap = Column(DECIMAL(15, 2), comment='总市值(万元)')
    float_cap = Column(DECIMAL(15, 2), comment='流通市值(万元)')
    
    support_strong = Column(DECIMAL(10, 3), comment='强支撑位')
    resistance_strong = Column(DECIMAL(10, 3), comment='强压力位')
    ma20 = Column(DECIMAL(10, 3), comment='20日均线')
    ma60 = Column(DECIMAL(10, 3), comment='60日均线')
    cvd_signal = Column(String(20), comment='CVD信号')
    reasons = Column(Text, comment='推荐理由(JSON)')
    
    stop_loss_price = Column(DECIMAL(10, 3), comment='止损价')
    take_profit_price = Column(DECIMAL(10, 3), comment='止盈价')
    stop_loss_pct = Column(DECIMAL(5, 2), default=Decimal('-5.00'), comment='止损线%')
    take_profit_pct = Column(DECIMAL(5, 2), default=Decimal('10.00'), comment='止盈线%')
    
    max_profit = Column(DECIMAL(8, 2), default=Decimal('0'), comment='最大收益%')
    max_loss = Column(DECIMAL(8, 2), default=Decimal('0'), comment='最大亏损%')
    final_profit = Column(DECIMAL(8, 2), default=Decimal('0'), comment='最终收益%')
    best_day = Column(Integer, default=0, comment='最佳收益日期')
    worst_day = Column(Integer, default=0, comment='最差收益日期')
    
    status = Column(String(20), default='tracking', comment='状态')
    stop_reason = Column(String(50), comment='终止原因')
    stopped_at = Column(Date, comment='终止日期')
    
    user_buy_date = Column(Date, comment='用户实际买入日期')
    user_sell_date = Column(Date, comment='用户实际卖出日期')
    user_notes = Column(Text, comment='用户备注')
    
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    trackings = relationship("StockPickTracking", back_populates="recommendation", 
                             cascade="all, delete-orphan")
    
    __table_args__ = (
        UniqueConstraint('recommend_date', 'code', name='uk_recommend'),
        Index('idx_date', 'recommend_date'),
        Index('idx_code', 'code'),
        Index('idx_grade', 'grade'),
        Index('idx_status', 'status'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'recommend_date': str(self.recommend_date),
            'code': self.code,
            'name': self.name,
            'grade': self.grade,
            'score': self.score,
            'recommend_price': float(self.recommend_price) if self.recommend_price else None,
            'recommend_change': float(self.recommend_change) if self.recommend_change else None,
            'is_st': self.is_st,
            'industry': self.industry,
            'support_strong': float(self.support_strong) if self.support_strong else None,
            'resistance_strong': float(self.resistance_strong) if self.resistance_strong else None,
            'ma20': float(self.ma20) if self.ma20 else None,
            'ma60': float(self.ma60) if self.ma60 else None,
            'cvd_signal': self.cvd_signal,
            'reasons': self.reasons,
            'stop_loss_pct': float(self.stop_loss_pct) if self.stop_loss_pct else None,
            'take_profit_pct': float(self.take_profit_pct) if self.take_profit_pct else None,
            'max_profit': float(self.max_profit) if self.max_profit else None,
            'max_loss': float(self.max_loss) if self.max_loss else None,
            'final_profit': float(self.final_profit) if self.final_profit else None,
            'status': self.status,
            'stop_reason': self.stop_reason,
            'created_at': str(self.created_at),
        }


class StockPickTracking(Base):
    """股票推荐跟踪明细表"""
    __tablename__ = 'stock_pick_tracking'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    recommend_id = Column(BigInteger, ForeignKey('stock_recommendation.id', ondelete='CASCADE'), 
                          nullable=False, comment='推荐记录ID')
    recommend_date = Column(Date, nullable=False, comment='推荐日期')
    code = Column(String(10), nullable=False, comment='股票代码')
    
    track_day = Column(Integer, nullable=False, comment='跟踪天数(1-30)')
    track_date = Column(Date, nullable=False, comment='跟踪日期')
    
    open_price = Column(DECIMAL(10, 3), comment='开盘价')
    high_price = Column(DECIMAL(10, 3), comment='最高价')
    low_price = Column(DECIMAL(10, 3), comment='最低价')
    close_price = Column(DECIMAL(10, 3), nullable=False, comment='收盘价')
    prev_close_price = Column(DECIMAL(10, 3), comment='前一日收盘价')
    
    daily_change = Column(DECIMAL(8, 2), comment='当日涨跌幅%')
    cumulative_profit = Column(DECIMAL(8, 2), nullable=False, comment='累计收益%')
    
    volume = Column(BigInteger, comment='成交量(手)')
    amount = Column(DECIMAL(15, 2), comment='成交额(万元)')
    turnover_rate = Column(DECIMAL(8, 2), comment='换手率%')
    
    signal_type = Column(String(20), comment='信号类型')
    signal_reason = Column(String(100), comment='信号原因')
    
    created_at = Column(DateTime, default=datetime.now)
    
    recommendation = relationship("StockRecommendation", back_populates="trackings")
    
    __table_args__ = (
        UniqueConstraint('recommend_id', 'track_day', name='uk_track'),
        Index('idx_recommend_date', 'recommend_date'),
        Index('idx_code', 'code'),
        Index('idx_track_date', 'track_date'),
        Index('idx_cumulative_profit', 'cumulative_profit'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'recommend_id': self.recommend_id,
            'recommend_date': str(self.recommend_date),
            'code': self.code,
            'track_day': self.track_day,
            'track_date': str(self.track_date),
            'open_price': float(self.open_price) if self.open_price else None,
            'high_price': float(self.high_price) if self.high_price else None,
            'low_price': float(self.low_price) if self.low_price else None,
            'close_price': float(self.close_price) if self.close_price else None,
            'daily_change': float(self.daily_change) if self.daily_change else None,
            'cumulative_profit': float(self.cumulative_profit) if self.cumulative_profit else None,
            'volume': self.volume,
            'amount': float(self.amount) if self.amount else None,
            'turnover_rate': float(self.turnover_rate) if self.turnover_rate else None,
            'signal_type': self.signal_type,
            'signal_reason': self.signal_reason,
        }
