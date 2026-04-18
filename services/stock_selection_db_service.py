#!/usr/bin/env python3
"""
每日选股数据库服务
- 保存每日选股结果
- 支持复盘比对
- 追踪选股表现
"""
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Tuple
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import Column, String, DateTime, Text, Float, Integer, Date, UniqueConstraint, Index, func
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT

Base = declarative_base()


class DailyStockSelection(Base):
    """每日选股结果表"""
    __tablename__ = 'daily_stock_selections'

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(Date, nullable=False, comment='报告日期')
    code = Column(String(20), nullable=False, comment='股票代码')
    name = Column(String(100), comment='股票名称')
    selection_type = Column(String(20), nullable=False, comment='选股类型: trend/short_term')
    score = Column(Float, comment='综合评分')
    rank = Column(Integer, comment='排名')

    # 选股因子
    v_ratio10 = Column(Float, comment='10日量比')
    v_total = Column(Float, comment='总成交额')
    cost_peak = Column(Float, comment='成本峰位')
    limit_up_score = Column(Float, comment='涨停评分')
    pioneer_status = Column(Float, comment='先锋状态')
    ma5_bias = Column(Float, comment='MA5偏离度')

    # 价格数据
    open_price = Column(Float, comment='开盘价')
    close_price = Column(Float, comment='收盘价')
    high_price = Column(Float, comment='最高价')
    low_price = Column(Float, comment='最低价')
    volume = Column(Float, comment='成交量')

    # 复盘数据（次日填充）
    next_day_open = Column(Float, comment='次日开盘价')
    next_day_close = Column(Float, comment='次日收盘价')
    next_day_high = Column(Float, comment='次日最高价')
    next_day_low = Column(Float, comment='次日最低价')
    next_day_return = Column(Float, comment='次日收益率%')
    max_intraday_return = Column(Float, comment='日内最大收益率%')
    max_intraday_loss = Column(Float, comment='日内最大亏损率%')

    # 多周期追踪（1天、4天、7天、11天、21天）
    day1_return = Column(Float, comment='1日收益率%')
    day4_return = Column(Float, comment='4日收益率%')
    day7_return = Column(Float, comment='7日收益率%')
    day11_return = Column(Float, comment='11日收益率%')
    day21_return = Column(Float, comment='21日收益率%')

    # 周期最高价/最低价（用于计算最大回撤和最大收益）
    day1_high = Column(Float, comment='1日最高价')
    day1_low = Column(Float, comment='1日最低价')
    day4_high = Column(Float, comment='4日最高价')
    day4_low = Column(Float, comment='4日最低价')
    day7_high = Column(Float, comment='7日最高价')
    day7_low = Column(Float, comment='7日最低价')
    day11_high = Column(Float, comment='11日最高价')
    day11_low = Column(Float, comment='11日最低价')
    day21_high = Column(Float, comment='21日最高价')
    day21_low = Column(Float, comment='21日最低价')

    # 最大回撤（从买入点到周期内最低点）
    day1_max_drawdown = Column(Float, comment='1日最大回撤%')
    day4_max_drawdown = Column(Float, comment='4日最大回撤%')
    day7_max_drawdown = Column(Float, comment='7日最大回撤%')
    day11_max_drawdown = Column(Float, comment='11日最大回撤%')
    day21_max_drawdown = Column(Float, comment='21日最大回撤%')

    # 最大涨幅（从买入点到周期内最高点）
    day1_max_gain = Column(Float, comment='1日最大涨幅%')
    day4_max_gain = Column(Float, comment='4日最大涨幅%')
    day7_max_gain = Column(Float, comment='7日最大涨幅%')
    day11_max_gain = Column(Float, comment='11日最大涨幅%')
    day21_max_gain = Column(Float, comment='21日最大涨幅%')

    # 元数据
    market_state = Column(String(20), comment='市场状态')
    filters_applied = Column(Text, comment='应用的过滤器JSON')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('report_date', 'code', 'selection_type', name='uk_date_code_type'),
        Index('idx_report_date', 'report_date'),
        Index('idx_code', 'code'),
        Index('idx_selection_type', 'selection_type'),
        Index('idx_report_date_type', 'report_date', 'selection_type'),
    )

    def __repr__(self):
        return f"<DailyStockSelection({self.report_date} {self.code} {self.selection_type})>"


class SelectionPerformance(Base):
    """选股表现统计表"""
    __tablename__ = 'selection_performance'

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(Date, nullable=False, comment='报告日期')
    selection_type = Column(String(20), nullable=False, comment='选股类型')

    # 选股数量
    total_selected = Column(Integer, comment='选股总数')

    # 次日表现统计
    next_day_win_count = Column(Integer, comment='次日盈利数量')
    next_day_loss_count = Column(Integer, comment='次日亏损数量')
    next_day_avg_return = Column(Float, comment='次日平均收益率%')
    next_day_max_return = Column(Float, comment='次日最大收益率%')
    next_day_min_return = Column(Float, comment='次日最小收益率%')

    # 胜率
    win_rate = Column(Float, comment='胜率%')

    # 对比基准
    benchmark_return = Column(Float, comment='基准收益率%(上证指数)')
    alpha = Column(Float, comment='超额收益%')

    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('report_date', 'selection_type', name='uk_date_type'),
        Index('idx_report_date', 'report_date'),
    )


from services.db_pool import get_db_pool, DatabasePoolManager


class StockSelectionDBService:
    """选股数据库服务"""

    def __init__(self, pool_manager: DatabasePoolManager = None):
        self.pool_manager = pool_manager or get_db_pool()

        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '3306')
        db_user = os.getenv('DB_USER', 'root')
        db_password = os.getenv('DB_PASSWORD', '')
        db_name = os.getenv('DB_NAME', 'quantdb')

        conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4&collation=utf8mb4_unicode_ci'

        pool_info = self.pool_manager.get_pool('stock_selection_db', conn_str)
        self.engine = pool_info['engine']
        self.Session = pool_info['Session']

    def init_tables(self):
        """初始化表"""
        Base.metadata.create_all(self.engine)
        print("✅ 选股数据表已初始化")

    def save_selections(self, report_date: str, selections: List[Dict], market_state: str = None) -> bool:
        """保存选股结果

        Args:
            report_date: 报告日期 (YYYY-MM-DD)
            selections: 选股列表，每个元素包含code, name, selection_type, score等
            market_state: 市场状态
        """
        try:
            report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()

            with self.Session() as session:
                saved_count = 0
                updated_count = 0

                for item in selections:
                    code = item.get('code')
                    selection_type = item.get('selection_type', 'trend')

                    # 检查是否已存在
                    existing = session.query(DailyStockSelection).filter(
                        DailyStockSelection.report_date == report_date_obj,
                        DailyStockSelection.code == code,
                        DailyStockSelection.selection_type == selection_type
                    ).first()

                    if existing:
                        # 更新
                        existing.name = item.get('name', existing.name)
                        existing.score = item.get('score', existing.score)
                        existing.rank = item.get('rank', existing.rank)
                        existing.v_ratio10 = item.get('v_ratio10', existing.v_ratio10)
                        existing.v_total = item.get('v_total', existing.v_total)
                        existing.cost_peak = item.get('cost_peak', existing.cost_peak)
                        existing.limit_up_score = item.get('limit_up_score', existing.limit_up_score)
                        existing.pioneer_status = item.get('pioneer_status', existing.pioneer_status)
                        existing.ma5_bias = item.get('ma5_bias', existing.ma5_bias)
                        existing.open_price = item.get('open', existing.open_price)
                        existing.close_price = item.get('close', existing.close_price)
                        existing.high_price = item.get('high', existing.high_price)
                        existing.low_price = item.get('low', existing.low_price)
                        existing.volume = item.get('volume', existing.volume)
                        existing.market_state = market_state or existing.market_state
                        updated_count += 1
                    else:
                        # 新建
                        selection = DailyStockSelection(
                            report_date=report_date_obj,
                            code=code,
                            name=item.get('name', ''),
                            selection_type=selection_type,
                            score=item.get('score'),
                            rank=item.get('rank'),
                            v_ratio10=item.get('v_ratio10'),
                            v_total=item.get('v_total'),
                            cost_peak=item.get('cost_peak'),
                            limit_up_score=item.get('limit_up_score'),
                            pioneer_status=item.get('pioneer_status'),
                            ma5_bias=item.get('ma5_bias'),
                            open_price=item.get('open'),
                            close_price=item.get('close'),
                            high_price=item.get('high'),
                            low_price=item.get('low'),
                            volume=item.get('volume'),
                            market_state=market_state,
                            filters_applied=json.dumps(item.get('filters', []), ensure_ascii=False)
                        )
                        session.add(selection)
                        saved_count += 1

                session.commit()
                print(f"✅ 选股结果已保存: {saved_count} 新增, {updated_count} 更新")
                return True

        except Exception as e:
            print(f"❌ 保存选股结果失败: {e}")
            return False

    def update_next_day_performance(self, report_date: str, performance_data: List[Dict]) -> bool:
        """更新次日表现数据

        Args:
            report_date: 报告日期
            performance_data: 表现数据列表
        """
        try:
            report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()

            with self.Session() as session:
                updated_count = 0

                for item in performance_data:
                    code = item.get('code')
                    selection_type = item.get('selection_type', 'trend')

                    record = session.query(DailyStockSelection).filter(
                        DailyStockSelection.report_date == report_date_obj,
                        DailyStockSelection.code == code,
                        DailyStockSelection.selection_type == selection_type
                    ).first()

                    if record:
                        record.next_day_open = item.get('next_day_open')
                        record.next_day_close = item.get('next_day_close')
                        record.next_day_high = item.get('next_day_high')
                        record.next_day_low = item.get('next_day_low')
                        record.next_day_return = item.get('next_day_return')
                        record.max_intraday_return = item.get('max_intraday_return')
                        record.max_intraday_loss = item.get('max_intraday_loss')
                        updated_count += 1

                session.commit()
                print(f"✅ 次日表现已更新: {updated_count} 条记录")
                return True

        except Exception as e:
            print(f"❌ 更新次日表现失败: {e}")
            return False

    def update_multi_period_performance(self, report_date: str, performance_data: List[Dict]) -> bool:
        """更新多周期表现数据（1天、4天、7天、11天、21天）

        Args:
            report_date: 报告日期
            performance_data: 多周期表现数据列表，每项包含各周期收益率、最高最低价等
        """
        try:
            report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()

            with self.Session() as session:
                updated_count = 0

                for item in performance_data:
                    code = item.get('code')
                    selection_type = item.get('selection_type', 'trend')

                    record = session.query(DailyStockSelection).filter(
                        DailyStockSelection.report_date == report_date_obj,
                        DailyStockSelection.code == code,
                        DailyStockSelection.selection_type == selection_type
                    ).first()

                    if record:
                        # 1日数据
                        record.day1_return = item.get('day1_return')
                        record.day1_high = item.get('day1_high')
                        record.day1_low = item.get('day1_low')
                        record.day1_max_gain = item.get('day1_max_gain')
                        record.day1_max_drawdown = item.get('day1_max_drawdown')

                        # 4日数据
                        record.day4_return = item.get('day4_return')
                        record.day4_high = item.get('day4_high')
                        record.day4_low = item.get('day4_low')
                        record.day4_max_gain = item.get('day4_max_gain')
                        record.day4_max_drawdown = item.get('day4_max_drawdown')

                        # 7日数据
                        record.day7_return = item.get('day7_return')
                        record.day7_high = item.get('day7_high')
                        record.day7_low = item.get('day7_low')
                        record.day7_max_gain = item.get('day7_max_gain')
                        record.day7_max_drawdown = item.get('day7_max_drawdown')

                        # 11日数据
                        record.day11_return = item.get('day11_return')
                        record.day11_high = item.get('day11_high')
                        record.day11_low = item.get('day11_low')
                        record.day11_max_gain = item.get('day11_max_gain')
                        record.day11_max_drawdown = item.get('day11_max_drawdown')

                        # 21日数据
                        record.day21_return = item.get('day21_return')
                        record.day21_high = item.get('day21_high')
                        record.day21_low = item.get('day21_low')
                        record.day21_max_gain = item.get('day21_max_gain')
                        record.day21_max_drawdown = item.get('day21_max_drawdown')

                        updated_count += 1

                session.commit()
                print(f"✅ 多周期表现已更新: {updated_count} 条记录")
                return True

        except Exception as e:
            print(f"❌ 更新多周期表现失败: {e}")
            return False

    def get_selections_by_date(self, report_date: str, selection_type: str = None) -> List[Dict]:
        """获取某日的选股结果"""
        try:
            report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()

            with self.Session() as session:
                query = session.query(DailyStockSelection).filter(
                    DailyStockSelection.report_date == report_date_obj
                )

                if selection_type:
                    query = query.filter(DailyStockSelection.selection_type == selection_type)

                results = query.order_by(DailyStockSelection.rank).all()

                return [{
                    'code': r.code,
                    'name': r.name,
                    'selection_type': r.selection_type,
                    'score': r.score,
                    'rank': r.rank,
                    'close_price': r.close_price,
                    'next_day_return': r.next_day_return,
                    'max_intraday_return': r.max_intraday_return,
                    'market_state': r.market_state
                } for r in results]

        except Exception as e:
            print(f"❌ 获取选股结果失败: {e}")
            return []

    def compare_dates(self, date1: str, date2: str, selection_type: str = 'trend') -> Dict:
        """比对两个日期的选股结果

        Returns:
            比对结果字典
        """
        try:
            selections1 = self.get_selections_by_date(date1, selection_type)
            selections2 = self.get_selections_by_date(date2, selection_type)

            codes1 = {s['code'] for s in selections1}
            codes2 = {s['code'] for s in selections2}

            common_codes = codes1 & codes2
            only_in_date1 = codes1 - codes2
            only_in_date2 = codes2 - codes1

            return {
                'date1': date1,
                'date2': date2,
                'selection_type': selection_type,
                'date1_count': len(selections1),
                'date2_count': len(selections2),
                'common_count': len(common_codes),
                'common_stocks': list(common_codes),
                'only_in_date1': list(only_in_date1),
                'only_in_date2': list(only_in_date2),
                'continuity_rate': len(common_codes) / len(codes1) * 100 if codes1 else 0
            }

        except Exception as e:
            print(f"❌ 比对失败: {e}")
            return {}

    def get_performance_summary(self, start_date: str = None, end_date: str = None,
                                selection_type: str = None, days: int = 30) -> Dict:
        """获取选股表现汇总

        Args:
            start_date: 开始日期
            end_date: 结束日期
            selection_type: 选股类型
            days: 最近N天
        """
        try:
            with self.Session() as session:
                # 默认查询最近N天
                if not end_date:
                    end_date_obj = date.today()
                else:
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()

                if not start_date:
                    start_date_obj = end_date_obj - timedelta(days=days)
                else:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()

                query = session.query(DailyStockSelection).filter(
                    DailyStockSelection.report_date >= start_date_obj,
                    DailyStockSelection.report_date <= end_date_obj
                )

                if selection_type:
                    query = query.filter(DailyStockSelection.selection_type == selection_type)

                # 统计
                total_selections = query.count()

                # 有次日收益数据的
                with_performance = query.filter(
                    DailyStockSelection.next_day_return.isnot(None)
                ).all()

                if not with_performance:
                    return {
                        'period': f"{start_date_obj} ~ {end_date_obj}",
                        'total_selections': total_selections,
                        'with_performance': 0,
                        'message': '暂无复盘数据'
                    }

                returns = [r.next_day_return for r in with_performance if r.next_day_return is not None]
                win_count = sum(1 for r in returns if r > 0)

                return {
                    'period': f"{start_date_obj} ~ {end_date_obj}",
                    'total_selections': total_selections,
                    'with_performance': len(with_performance),
                    'win_count': win_count,
                    'loss_count': len(returns) - win_count,
                    'win_rate': win_count / len(returns) * 100 if returns else 0,
                    'avg_return': sum(returns) / len(returns) if returns else 0,
                    'max_return': max(returns) if returns else 0,
                    'min_return': min(returns) if returns else 0
                }

        except Exception as e:
            print(f"❌ 获取表现汇总失败: {e}")
            return {}

    def get_continuity_analysis(self, code: str, days: int = 30) -> List[Dict]:
        """获取某只股票的连续选股记录

        Args:
            code: 股票代码
            days: 最近N天
        """
        try:
            with self.Session() as session:
                end_date = date.today()
                start_date = end_date - timedelta(days=days)

                results = session.query(DailyStockSelection).filter(
                    DailyStockSelection.code == code,
                    DailyStockSelection.report_date >= start_date,
                    DailyStockSelection.report_date <= end_date
                ).order_by(DailyStockSelection.report_date.desc()).all()

                return [{
                    'report_date': r.report_date.isoformat(),
                    'selection_type': r.selection_type,
                    'score': r.score,
                    'rank': r.rank,
                    'close_price': r.close_price,
                    'next_day_return': r.next_day_return
                } for r in results]

        except Exception as e:
            print(f"❌ 获取连续性分析失败: {e}")
            return []


if __name__ == '__main__':
    # 测试
    service = StockSelectionDBService()
    service.init_tables()
