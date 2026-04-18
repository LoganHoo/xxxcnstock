"""资金行为学报告数据库服务 - 支持周度/月度分析"""
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict
import json

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import Column, String, DateTime, Text, Float, Integer, Date, Enum as SQLEnum
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT
import enum


Base = declarative_base()


class ReportPeriod(enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class FundBehaviorDaily(Base):
    __tablename__ = 'xcn_fund_behavior_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(Date, nullable=False, unique=True, comment='报告日期')
    market_state = Column(String(20), comment='市场状态: STRONG/OSCILLATING/WEAK')
    market_tone = Column(String(50), comment='市场基调: 强势做多/震荡防守')
    v_total = Column(Float, comment='总成交额(亿元)')
    sentiment_temperature = Column(Float, comment='情绪温度')
    delta_temperature = Column(Float, comment='温差惯性')
    cost_peak = Column(Float, comment='核心筹码位')
    current_price = Column(Float, comment='当前价格')
    upward_pivot = Column(Integer, comment='向上变盘: 0/1')
    hedge_effect = Column(Integer, comment='对冲效果: 0/1')
    trend_position = Column(Float, comment='波段仓位')
    short_position = Column(Float, comment='短线仓位')
    cash_position = Column(Float, comment='现金仓位')
    total_position = Column(Float, comment='总仓位')
    trend_stock_count = Column(Integer, comment='波段股票数量')
    short_stock_count = Column(Integer, comment='短线股票数量')
    json_data = Column(LONGTEXT, comment='完整JSON数据')
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<FundBehaviorDaily(date={self.report_date}, state={self.market_state})>"


class FundBehaviorWeekly(Base):
    __tablename__ = 'xcn_fund_behavior_weekly'

    id = Column(Integer, primary_key=True, autoincrement=True)
    week_start = Column(Date, nullable=False, comment='周起始日期')
    week_end = Column(Date, nullable=False, comment='周结束日期')
    avg_v_total = Column(Float, comment='周均成交额(亿元)')
    avg_sentiment = Column(Float, comment='周均情绪温度')
    sentiment_trend = Column(String(20), comment='情绪趋势: rising/falling/stable')
    market_state_distribution = Column(Text, comment='市场状态分布JSON')
    strong_days = Column(Integer, comment='强势天数')
    oscillating_days = Column(Integer, comment='震荡天数')
    weak_days = Column(Integer, comment='弱势天数')
    upward_pivot_days = Column(Integer, comment='向上变盘天数')
    hedge_effect_days = Column(Integer, comment='对冲效果天数')
    total_position_avg = Column(Float, comment='平均总仓位')
    trend_vs_short_ratio = Column(Text, comment='波段/短线比例变化')
    top_trend_stocks = Column(Text, comment='波段高频股票')
    top_short_stocks = Column(Text, comment='短线高频股票')
    weekly_summary = Column(Text, comment='周度总结')
    warnings_summary = Column(Text, comment='风险预警汇总')
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<FundBehaviorWeekly({self.week_start} ~ {self.week_end})>"


class FundBehaviorMonthly(Base):
    __tablename__ = 'xcn_fund_behavior_monthly'

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False, comment='年份')
    month = Column(Integer, nullable=False, comment='月份')
    month_start = Column(Date, nullable=False, comment='月起始日期')
    month_end = Column(Date, nullable=False, comment='月结束日期')
    avg_v_total = Column(Float, comment='月均成交额(亿元)')
    max_v_total = Column(Float, comment='月最大成交额')
    min_v_total = Column(Float, comment='月最小成交额')
    avg_sentiment = Column(Float, comment='月均情绪温度')
    max_sentiment = Column(Float, comment='月最高情绪')
    min_sentiment = Column(Float, comment='月最低情绪')
    sentiment_trend = Column(String(20), comment='情绪趋势')
    market_state_distribution = Column(Text, comment='市场状态分布JSON')
    strong_days = Column(Integer, comment='强势天数')
    oscillating_days = Column(Integer, comment='震荡天数')
    weak_days = Column(Integer, comment='弱势天数')
    upward_pivot_ratio = Column(Float, comment='向上变盘天数占比')
    hedge_effect_ratio = Column(Float, comment='对冲效果天数占比')
    avg_position = Column(Float, comment='月均仓位')
    position_peak_date = Column(Date, comment='仓位峰值日期')
    top_trend_stocks = Column(Text, comment='波段高频股票')
    top_short_stocks = Column(Text, comment='短线高频股票')
    warning_types_distribution = Column(Text, comment='预警类型分布')
    monthly_summary = Column(Text, comment='月度总结')
    performance_review = Column(Text, comment='表现回顾')
    next_month_outlook = Column(Text, comment='下月展望')
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<FundBehaviorMonthly({self.year}-{self.month:02d})>"


class FundBehaviorPerformance(Base):
    __tablename__ = 'xcn_fund_behavior_performance'

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(Date, nullable=False, unique=True, comment='报告日期')
    period_type = Column(String(10), nullable=False, comment='统计周期: daily/weekly/monthly')
    period_start = Column(Date, nullable=False, comment='周期起始日期')
    period_end = Column(Date, nullable=False, comment='周期结束日期')

    total_return = Column(Float, comment='总收益率')
    annual_return = Column(Float, comment='年化收益率')
    max_drawdown = Column(Float, comment='最大回撤')
    sharpe_ratio = Column(Float, comment='夏普比率')
    win_rate = Column(Float, comment='胜率')
    total_trades = Column(Integer, comment='总交易次数')
    winning_trades = Column(Integer, comment='盈利交易次数')
    losing_trades = Column(Integer, comment='亏损交易次数')
    avg_profit = Column(Float, comment='平均盈利')
    avg_loss = Column(Float, comment='平均亏损')
    profit_loss_ratio = Column(Float, comment='盈亏比')
    final_value = Column(Float, comment='最终账户价值')
    initial_value = Column(Float, comment='初始账户价值')
    market_state = Column(String(20), comment='市场状态')
    market_tone = Column(String(50), comment='市场基调')

    top_trade_stocks = Column(Text, comment='最佳交易股票')
    worst_trade_stocks = Column(Text, comment='最差交易股票')
    daily_returns_json = Column(LONGTEXT, comment='每日收益JSON')
    trades_json = Column(LONGTEXT, comment='交易记录JSON')
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<FundBehaviorPerformance(date={self.report_date}, period={self.period_type})>"


from services.db_pool import get_db_pool, DatabasePoolManager


class FundBehaviorDBService:
    """资金行为学报告数据库服务 - 使用连接池"""

    def __init__(self, pool_manager: DatabasePoolManager = None):
        self.pool_manager = pool_manager or get_db_pool()
        
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '3306')
        db_user = os.getenv('DB_USER', 'root')
        db_password = os.getenv('DB_PASSWORD', '')
        db_name = os.getenv('DB_NAME', 'quantdb')

        conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4&collation=utf8mb4_unicode_ci'
        
        pool_info = self.pool_manager.get_pool('fund_behavior_db', conn_str)
        self.engine = pool_info['engine']
        self.Session = pool_info['Session']

    def init_tables(self):
        """初始化表"""
        Base.metadata.create_all(self.engine)

    def save_daily_report(self, report_date: str, result: Dict) -> bool:
        """保存每日报告 - 使用INSERT...ON DUPLICATE KEY UPDATE避免重复键错误"""
        try:
            import pymysql
            from pymysql.cursors import DictCursor
            from sqlalchemy import text

            position = result.get('position_size', {})
            trend_stocks = result.get('trend_stocks', [])
            short_stocks = result.get('short_term_stocks', [])

            report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()

            with self.Session() as session:
                # 先检查是否存在
                existing = session.query(FundBehaviorDaily).filter(
                    FundBehaviorDaily.report_date == report_date_obj
                ).first()

                if existing:
                    # 更新现有记录
                    existing.market_state = result.get('market_state', ['N/A'])[0] if result.get('market_state') else 'N/A'
                    existing.market_tone = '强势做多' if (result.get('is_strong_region') and result.get('upward_pivot')) else '震荡防守'
                    existing.v_total = result.get('v_total', 0)
                    existing.sentiment_temperature = result.get('sentiment_temperature', 0)
                    existing.delta_temperature = result.get('delta_temperature', 0)
                    existing.cost_peak = result.get('cost_peak', 0)
                    existing.current_price = result.get('current_price', 0)
                    existing.upward_pivot = 1 if result.get('upward_pivot') else 0
                    existing.hedge_effect = 1 if result.get('hedge_effect') else 0
                    existing.trend_position = position.get('trend', 0)
                    existing.short_position = position.get('short_term', 0)
                    existing.cash_position = position.get('cash', 0)
                    existing.total_position = sum(position.values()) if position else 0
                    existing.trend_stock_count = len(trend_stocks)
                    existing.short_stock_count = len(short_stocks)
                    existing.json_data = json.dumps(result, ensure_ascii=False)
                    existing.created_at = datetime.now()
                    print(f"✅ 日报已更新: {report_date}")
                else:
                    # 插入新记录
                    report = FundBehaviorDaily(
                        report_date=report_date_obj,
                        market_state=result.get('market_state', ['N/A'])[0] if result.get('market_state') else 'N/A',
                        market_tone='强势做多' if (result.get('is_strong_region') and result.get('upward_pivot')) else '震荡防守',
                        v_total=result.get('v_total', 0),
                        sentiment_temperature=result.get('sentiment_temperature', 0),
                        delta_temperature=result.get('delta_temperature', 0),
                        cost_peak=result.get('cost_peak', 0),
                        current_price=result.get('current_price', 0),
                        upward_pivot=1 if result.get('upward_pivot') else 0,
                        hedge_effect=1 if result.get('hedge_effect') else 0,
                        trend_position=position.get('trend', 0),
                        short_position=position.get('short_term', 0),
                        cash_position=position.get('cash', 0),
                        total_position=sum(position.values()) if position else 0,
                        trend_stock_count=len(trend_stocks),
                        short_stock_count=len(short_stocks),
                        json_data=json.dumps(result, ensure_ascii=False)
                    )
                    session.add(report)
                    print(f"✅ 日报已保存: {report_date}")

                session.commit()
                return True
        except Exception as e:
            print(f"❌ 保存日报失败: {e}")
            return False

    def get_daily_report(self, report_date: str) -> Optional[Dict]:
        """获取每日报告"""
        try:
            with self.Session() as session:
                report = session.query(FundBehaviorDaily).filter(
                    FundBehaviorDaily.report_date == datetime.strptime(report_date, '%Y-%m-%d').date()
                ).first()

                if report:
                    return {
                        'report_date': report.report_date,
                        'market_state': report.market_state,
                        'market_tone': report.market_tone,
                        'v_total': report.v_total,
                        'sentiment_temperature': report.sentiment_temperature,
                        'delta_temperature': report.delta_temperature,
                        'cost_peak': report.cost_peak,
                        'current_price': report.current_price,
                        'upward_pivot': bool(report.upward_pivot),
                        'hedge_effect': bool(report.hedge_effect),
                        'position_size': {
                            'trend': report.trend_position,
                            'short_term': report.short_position,
                            'cash': report.cash_position
                        },
                        'trend_stock_count': report.trend_stock_count,
                        'short_stock_count': report.short_stock_count,
                        'json_data': json.loads(report.json_data) if report.json_data else None
                    }
                return None
        except Exception as e:
            print(f"❌ 获取日报失败: {e}")
            return None

    def get_weekly_reports(self, weeks: int = 4) -> List[Dict]:
        """获取最近N周的周报"""
        try:
            with self.Session() as session:
                reports = session.query(FundBehaviorWeekly).order_by(
                    FundBehaviorWeekly.week_end.desc()
                ).limit(weeks).all()

                return [{
                    'week_start': r.week_start,
                    'week_end': r.week_end,
                    'avg_v_total': r.avg_v_total,
                    'avg_sentiment': r.avg_sentiment,
                    'sentiment_trend': r.sentiment_trend,
                    'strong_days': r.strong_days,
                    'oscillating_days': r.oscillating_days,
                    'weak_days': r.weak_days,
                    'upward_pivot_days': r.upward_pivot_days,
                    'created_at': r.created_at
                } for r in reports]
        except Exception as e:
            print(f"❌ 获取周报失败: {e}")
            return []

    def get_monthly_reports(self, months: int = 6) -> List[Dict]:
        """获取最近N月的月报"""
        try:
            with self.Session() as session:
                reports = session.query(FundBehaviorMonthly).order_by(
                    FundBehaviorMonthly.year.desc(),
                    FundBehaviorMonthly.month.desc()
                ).limit(months).all()

                return [{
                    'year': r.year,
                    'month': r.month,
                    'avg_v_total': r.avg_v_total,
                    'avg_sentiment': r.avg_sentiment,
                    'sentiment_trend': r.sentiment_trend,
                    'strong_days': r.strong_days,
                    'oscillating_days': r.oscillating_days,
                    'weak_days': r.weak_days,
                    'created_at': r.created_at
                } for r in reports]
        except Exception as e:
            print(f"❌ 获取月报失败: {e}")
            return []

    def calculate_and_save_weekly(self, week_end_date: str) -> bool:
        """计算并保存周报"""
        try:
            end_date = datetime.strptime(week_end_date, '%Y-%m-%d').date()
            start_date = end_date - timedelta(days=6)

            with self.Session() as session:
                daily_reports = session.query(FundBehaviorDaily).filter(
                    FundBehaviorDaily.report_date >= start_date,
                    FundBehaviorDaily.report_date <= end_date
                ).order_by(FundBehaviorDaily.report_date).all()

                if not daily_reports:
                    print(f"⚠️ 周 {start_date} ~ {end_date} 无数据")
                    return False

                total_days = len(daily_reports)
                avg_v_total = sum(r.v_total for r in daily_reports if r.v_total) / total_days
                avg_sentiment = sum(r.sentiment_temperature for r in daily_reports if r.sentiment_temperature) / total_days

                state_counts = {'STRONG': 0, 'OSCILLATING': 0, 'WEAK': 0}
                for r in daily_reports:
                    if r.market_state in state_counts:
                        state_counts[r.market_state] += 1

                sentiment_trend = 'rising' if daily_reports[-1].sentiment_temperature > daily_reports[0].sentiment_temperature else 'falling'
                if abs(daily_reports[-1].sentiment_temperature - daily_reports[0].sentiment_temperature) < 5:
                    sentiment_trend = 'stable'

                trend_stocks_all = []
                short_stocks_all = []
                for r in daily_reports:
                    if r.json_data:
                        data = json.loads(r.json_data)
                        trend_stocks_all.extend(data.get('trend_stocks', []))
                        short_stocks_all.extend(data.get('short_term_stocks', []))

                from collections import Counter
                top_trend = [s for s, _ in Counter(trend_stocks_all).most_common(10)]
                top_short = [s for s, _ in Counter(short_stocks_all).most_common(10)]

                weekly_report = FundBehaviorWeekly(
                    week_start=start_date,
                    week_end=end_date,
                    avg_v_total=avg_v_total,
                    avg_sentiment=avg_sentiment,
                    sentiment_trend=sentiment_trend,
                    market_state_distribution=json.dumps(state_counts),
                    strong_days=state_counts['STRONG'],
                    oscillating_days=state_counts['OSCILLATING'],
                    weak_days=state_counts['WEAK'],
                    upward_pivot_days=sum(1 for r in daily_reports if r.upward_pivot),
                    hedge_effect_days=sum(1 for r in daily_reports if r.hedge_effect),
                    total_position_avg=sum(r.total_position for r in daily_reports if r.total_position) / total_days,
                    top_trend_stocks=json.dumps(top_trend),
                    top_short_stocks=json.dumps(top_short),
                    weekly_summary=self._generate_weekly_summary(state_counts, avg_sentiment, sentiment_trend)
                )

                session.merge(weekly_report)
                session.commit()
                print(f"✅ 周报已生成: {start_date} ~ {end_date}")
                return True
        except Exception as e:
            print(f"❌ 生成周报失败: {e}")
            return False

    def calculate_and_save_monthly(self, year: int, month: int) -> bool:
        """计算并保存月报"""
        try:
            from calendar import monthrange
            _, last_day = monthrange(year, month)
            month_start = date(year, month, 1)
            month_end = date(year, month, last_day)

            with self.Session() as session:
                daily_reports = session.query(FundBehaviorDaily).filter(
                    FundBehaviorDaily.report_date >= month_start,
                    FundBehaviorDaily.report_date <= month_end
                ).order_by(FundBehaviorDaily.report_date).all()

                if not daily_reports:
                    print(f"⚠️ {year}-{month:02d} 无数据")
                    return False

                total_days = len(daily_reports)
                v_totals = [r.v_total for r in daily_reports if r.v_total]
                sentiments = [r.sentiment_temperature for r in daily_reports if r.sentiment_temperature]

                avg_v_total = sum(v_totals) / len(v_totals) if v_totals else 0
                max_v_total = max(v_totals) if v_totals else 0
                min_v_total = min(v_totals) if v_totals else 0
                avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
                max_sentiment = max(sentiments) if sentiments else 0
                min_sentiment = min(sentiments) if sentiments else 0

                state_counts = {'STRONG': 0, 'OSCILLATING': 0, 'WEAK': 0}
                for r in daily_reports:
                    if r.market_state in state_counts:
                        state_counts[r.market_state] += 1

                sentiment_trend = 'rising' if daily_reports[-1].sentiment_temperature > daily_reports[0].sentiment_temperature else 'falling'
                if abs(daily_reports[-1].sentiment_temperature - daily_reports[0].sentiment_temperature) < 5:
                    sentiment_trend = 'stable'

                trend_stocks_all = []
                short_stocks_all = []
                for r in daily_reports:
                    if r.json_data:
                        data = json.loads(r.json_data)
                        trend_stocks_all.extend(data.get('trend_stocks', []))
                        short_stocks_all.extend(data.get('short_term_stocks', []))

                from collections import Counter
                top_trend = [s for s, _ in Counter(trend_stocks_all).most_common(15)]
                top_short = [s for s, _ in Counter(short_stocks_all).most_common(15)]

                position_peak_report = max(daily_reports, key=lambda r: r.total_position or 0)

                monthly_report = FundBehaviorMonthly(
                    year=year,
                    month=month,
                    month_start=month_start,
                    month_end=month_end,
                    avg_v_total=avg_v_total,
                    max_v_total=max_v_total,
                    min_v_total=min_v_total,
                    avg_sentiment=avg_sentiment,
                    max_sentiment=max_sentiment,
                    min_sentiment=min_sentiment,
                    sentiment_trend=sentiment_trend,
                    market_state_distribution=json.dumps(state_counts),
                    strong_days=state_counts['STRONG'],
                    oscillating_days=state_counts['OSCILLATING'],
                    weak_days=state_counts['WEAK'],
                    upward_pivot_ratio=sum(1 for r in daily_reports if r.upward_pivot) / total_days,
                    hedge_effect_ratio=sum(1 for r in daily_reports if r.hedge_effect) / total_days,
                    avg_position=sum(r.total_position for r in daily_reports if r.total_position) / total_days,
                    position_peak_date=position_peak_report.report_date,
                    top_trend_stocks=json.dumps(top_trend),
                    top_short_stocks=json.dumps(top_short),
                    warning_types_distribution=json.dumps(self._extract_warning_types(daily_reports)),
                    monthly_summary=self._generate_monthly_summary(state_counts, avg_sentiment, sentiment_trend)
                )

                session.merge(monthly_report)
                session.commit()
                print(f"✅ 月报已生成: {year}-{month:02d}")
                return True
        except Exception as e:
            print(f"❌ 生成月报失败: {e}")
            return False

    def _generate_weekly_summary(self, state_counts: dict, avg_sentiment: float, sentiment_trend: str) -> str:
        """生成周报总结"""
        total_days = sum(state_counts.values())
        dominant_state = max(state_counts, key=state_counts.get)
        
        summary = f"本周市场以{dominant_state}状态为主，共{state_counts[dominant_state]}天。"
        summary += f"周均成交额{avg_sentiment:.1f}亿元，情绪温度{avg_sentiment:.1f}°。"
        summary += f"情绪趋势{'上升' if sentiment_trend == 'rising' else '下降' if sentiment_trend == 'falling' else '平稳'}。"
        return summary

    def _generate_monthly_summary(self, state_counts: dict, avg_sentiment: float, sentiment_trend: str) -> str:
        """生成月报总结"""
        total_days = sum(state_counts.values())
        dominant_state = max(state_counts, key=state_counts.get)
        
        summary = f"本月市场以{dominant_state}状态为主，共{state_counts[dominant_state]}天。"
        summary += f"月均成交额{avg_sentiment:.1f}亿元，情绪温度{avg_sentiment:.1f}°。"
        summary += f"情绪趋势{'上升' if sentiment_trend == 'rising' else '下降' if sentiment_trend == 'falling' else '平稳'}。"
        return summary

    def _extract_warning_types(self, daily_reports) -> dict:
        """提取预警类型分布"""
        warning_types = {}
        for r in daily_reports:
            if r.json_data:
                data = json.loads(r.json_data)
                warnings = data.get('warnings', [])
                for w in warnings:
                    warning_types[w] = warning_types.get(w, 0) + 1
        return warning_types

    def save_performance(
        self,
        period_type: str,
        period_start: date,
        period_end: date,
        performance_data: dict,
        market_state: str
    ) -> bool:
        """保存绩效数据"""
        try:
            with self.Session() as session:
                report = FundBehaviorPerformance(
                    report_date=period_end,
                    period_type=period_type,
                    period_start=period_start,
                    period_end=period_end,
                    total_return=performance_data.get('total_return', 0),
                    annual_return=performance_data.get('annual_return', 0),
                    max_drawdown=performance_data.get('max_drawdown', 0),
                    sharpe_ratio=performance_data.get('sharpe_ratio', 0),
                    win_rate=performance_data.get('win_rate', 0),
                    total_trades=performance_data.get('total_trades', 0),
                    winning_trades=performance_data.get('winning_trades', 0),
                    losing_trades=performance_data.get('losing_trades', 0),
                    avg_profit=performance_data.get('avg_profit', 0),
                    avg_loss=performance_data.get('avg_loss', 0),
                    profit_loss_ratio=performance_data.get('profit_loss_ratio', 0),
                    final_value=performance_data.get('final_value', 0),
                    initial_value=performance_data.get('initial_value', 0),
                    market_state=market_state,
                    market_tone='强势做多' if market_state == 'STRONG' else '震荡防守',
                    daily_returns_json=json.dumps(performance_data.get('daily_returns', [])),
                    trades_json=json.dumps(performance_data.get('trades', []))
                )
                session.merge(report)
                session.commit()
                print(f"✅ 绩效数据已保存: {period_type} {period_start} ~ {period_end}")
                return True
        except Exception as e:
            print(f"❌ 保存绩效数据失败: {e}")
            return False

    def calculate_weekly_performance(self, week_end_date: str) -> bool:
        """计算周度绩效"""
        try:
            end_date = datetime.strptime(week_end_date, '%Y-%m-%d').date()
            start_date = end_date - timedelta(days=6)

            with self.Session() as session:
                daily_reports = session.query(FundBehaviorDaily).filter(
                    FundBehaviorDaily.report_date >= start_date,
                    FundBehaviorDaily.report_date <= end_date
                ).order_by(FundBehaviorDaily.report_date).all()

                if len(daily_reports) < 2:
                    print(f"⚠️ 周 {start_date} ~ {end_date} 数据不足")
                    return False

                initial_value = 1000000
                values = [initial_value]
                for r in daily_reports:
                    position = r.total_position or 0
                    if position > 0:
                        daily_return = (r.sentiment_temperature - 30) / 100
                        values.append(values[-1] * (1 + daily_return))

                daily_returns = []
                for i in range(1, len(values)):
                    prev = values[i-1]
                    curr = values[i]
                    if prev > 0:
                        daily_returns.append((curr - prev) / prev)

                total_return = (values[-1] - initial_value) / initial_value
                final_value = values[-1]

                win_rate = sum(1 for r in daily_returns if r > 0) / len(daily_returns) if daily_returns else 0
                max_drawdown = 0
                peak = initial_value
                for v in values:
                    if v > peak:
                        peak = v
                    drawdown = (peak - v) / peak
                    if drawdown > max_drawdown:
                        max_drawdown = drawdown

                avg_profit = sum(r for r in daily_returns if r > 0) / max(1, sum(1 for r in daily_returns if r > 0))
                avg_loss = sum(abs(r) for r in daily_returns if r < 0) / max(1, sum(1 for r in daily_returns if r < 0))
                profit_loss_ratio = abs(avg_profit / avg_loss) if avg_loss != 0 else 0

                market_states = [r.market_state for r in daily_reports if r.market_state]
                dominant_state = max(set(market_states), key=market_states.count) if market_states else 'N/A'

                performance_data = {
                    'total_return': total_return,
                    'annual_return': (1 + total_return) ** (52 / len(daily_reports)) - 1 if daily_reports else 0,
                    'max_drawdown': max_drawdown,
                    'sharpe_ratio': 0,
                    'win_rate': win_rate,
                    'total_trades': 0,
                    'winning_trades': 0,
                    'losing_trades': 0,
                    'avg_profit': avg_profit,
                    'avg_loss': avg_loss,
                    'profit_loss_ratio': profit_loss_ratio,
                    'final_value': final_value,
                    'initial_value': initial_value,
                    'daily_returns': daily_returns,
                    'trades': []
                }

                return self.save_performance(
                    period_type='weekly',
                    period_start=start_date,
                    period_end=end_date,
                    performance_data=performance_data,
                    market_state=dominant_state
                )
        except Exception as e:
            print(f"❌ 计算周度绩效失败: {e}")
            return False

    def calculate_monthly_performance(self, year: int, month: int) -> bool:
        """计算月度绩效"""
        try:
            from calendar import monthrange
            _, last_day = monthrange(year, month)
            month_start = date(year, month, 1)
            month_end = date(year, month, last_day)

            with self.Session() as session:
                daily_reports = session.query(FundBehaviorDaily).filter(
                    FundBehaviorDaily.report_date >= month_start,
                    FundBehaviorDaily.report_date <= month_end
                ).order_by(FundBehaviorDaily.report_date).all()

                if len(daily_reports) < 2:
                    print(f"⚠️ {year}-{month:02d} 数据不足")
                    return False

                initial_value = 1000000
                values = [initial_value]
                for r in daily_reports:
                    position = r.total_position or 0
                    if position > 0:
                        daily_return = (r.sentiment_temperature - 30) / 100
                        values.append(values[-1] * (1 + daily_return))

                daily_returns = []
                for i in range(1, len(values)):
                    prev = values[i-1]
                    curr = values[i]
                    if prev > 0:
                        daily_returns.append((curr - prev) / prev)

                total_return = (values[-1] - initial_value) / initial_value
                final_value = values[-1]

                win_rate = sum(1 for r in daily_returns if r > 0) / len(daily_returns) if daily_returns else 0
                max_drawdown = 0
                peak = initial_value
                for v in values:
                    if v > peak:
                        peak = v
                    drawdown = (peak - v) / peak
                    if drawdown > max_drawdown:
                        max_drawdown = drawdown

                avg_profit = sum(r for r in daily_returns if r > 0) / max(1, sum(1 for r in daily_returns if r > 0))
                avg_loss = sum(abs(r) for r in daily_returns if r < 0) / max(1, sum(1 for r in daily_returns if r < 0))
                profit_loss_ratio = abs(avg_profit / avg_loss) if avg_loss != 0 else 0

                market_states = [r.market_state for r in daily_reports if r.market_state]
                dominant_state = max(set(market_states), key=market_states.count) if market_states else 'N/A'

                performance_data = {
                    'total_return': total_return,
                    'annual_return': (1 + total_return) ** (12 / len(daily_reports)) - 1 if daily_reports else 0,
                    'max_drawdown': max_drawdown,
                    'sharpe_ratio': 0,
                    'win_rate': win_rate,
                    'total_trades': 0,
                    'winning_trades': 0,
                    'losing_trades': 0,
                    'avg_profit': avg_profit,
                    'avg_loss': avg_loss,
                    'profit_loss_ratio': profit_loss_ratio,
                    'final_value': final_value,
                    'initial_value': initial_value,
                    'daily_returns': daily_returns,
                    'trades': []
                }

                return self.save_performance(
                    period_type='monthly',
                    period_start=month_start,
                    period_end=month_end,
                    performance_data=performance_data,
                    market_state=dominant_state
                )
        except Exception as e:
            print(f"❌ 计算月度绩效失败: {e}")
            return False


def main():
    service = FundBehaviorDBService()
    service.init_tables()
    print("✅ 资金行为学数据库服务初始化完成")
