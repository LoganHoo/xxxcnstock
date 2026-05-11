#!/usr/bin/env python3
"""
市场数据 MySQL 表模型

定义大宗商品、宏观数据、石油美元、市场情绪的 ORM 模型。
"""
from sqlalchemy import Column, String, DateTime, Float, Integer, Date, Text, JSON
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class CommodityPrice(Base):
    """大宗商品价格表"""
    __tablename__ = 'commodity_prices'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, comment='交易日期')
    commodity_name = Column(String(50), nullable=False, comment='商品名称: gold/silver/copper/lithium')
    commodity_type = Column(String(20), nullable=False, comment='类型: metal/energy/agriculture')
    price = Column(Float, comment='当前价格')
    change_value = Column(Float, comment='涨跌额')
    change_pct = Column(Float, comment='涨跌幅%')
    source = Column(String(20), default='sina', comment='数据源')
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        {'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )


class MacroIndicator(Base):
    """宏观经济指标表"""
    __tablename__ = 'macro_indicators'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, comment='交易日期')
    indicator_name = Column(String(50), nullable=False, comment='指标名: dxy/us10y/cny/pmi_us/cpi_us/nfp/gdp_us/pmi_cn/cpi_cn/gdp_cn')
    value = Column(Float, comment='指标值')
    change_pct = Column(Float, comment='变化率%')
    source = Column(String(20), default='sina', comment='数据源: sina/eastmoney')
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        {'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )


class OilDollarData(Base):
    """石油与美元数据表"""
    __tablename__ = 'oil_dollar_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, comment='交易日期')
    data_type = Column(String(20), nullable=False, comment='类型: brent/wti')
    price = Column(Float, comment='价格')
    change_value = Column(Float, comment='涨跌额')
    change_pct = Column(Float, comment='涨跌幅%')
    source = Column(String(20), default='sina', comment='数据源')
    notes = Column(Text, comment='备注')
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        {'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )


class MarketSentiment(Base):
    """市场情绪指标表"""
    __tablename__ = 'market_sentiment'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, comment='交易日期')
    indicator_name = Column(String(50), nullable=False, comment='指标名: fear_greed/vix/bomb_rate/market_breadth')
    value = Column(Float, comment='指标值')
    level = Column(String(20), comment='等级: neutral/greed/fear 等')
    extra = Column(JSON, comment='额外数据 JSON')
    source = Column(String(20), default='sina', comment='数据源')
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        {'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )


class FinancialNews(Base):
    """财经新闻表"""
    __tablename__ = 'financial_news'

    id = Column(Integer, primary_key=True, autoincrement=True)
    news_date = Column(Date, nullable=False, comment='新闻日期')
    update_time = Column(String(30), comment='更新时间')
    domestic_news = Column(JSON, comment='国内新闻列表')
    overseas_news = Column(JSON, comment='海外新闻列表')
    all_news = Column(JSON, comment='全部新闻列表')
    source = Column(String(50), comment='数据源')
    is_default = Column(Integer, default=0, comment='是否默认数据')
    retry_count = Column(Integer, default=0, comment='重试次数')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        {'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )


def init_tables(engine):
    """创建所有市场数据表（幂等）"""
    Base.metadata.create_all(engine)
