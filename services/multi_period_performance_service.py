#!/usr/bin/env python3
"""
多周期收益计算服务
- 计算选股后1天、4天、7天、11天、21天的收益率
- 支持最大回撤和最大涨幅计算
"""
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import polars as pl
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.stock_selection_db_service import StockSelectionDBService


class MultiPeriodPerformanceService:
    """多周期收益计算服务"""

    # 定义周期：天数
    PERIODS = [1, 4, 7, 11, 21]

    def __init__(self):
        self.kline_dir = Path('data/kline')
        self.selection_service = StockSelectionDBService()

    def get_next_trading_date(self, start_date: date, days: int) -> Optional[date]:
        """获取N个交易日后的日期

        Args:
            start_date: 起始日期
            days: 交易日天数

        Returns:
            目标日期或None
        """
        # 这里简化处理，实际应该从交易日历查询
        # 跳过周末
        target_date = start_date
        trading_days = 0

        while trading_days < days:
            target_date += timedelta(days=1)
            # 跳过周末 (5=周六, 6=周日)
            if target_date.weekday() < 5:
                trading_days += 1

        return target_date

    def read_stock_kline(self, code: str) -> Optional[pl.DataFrame]:
        """读取股票K线数据"""
        parquet_file = self.kline_dir / f"{code}.parquet"
        if not parquet_file.exists():
            return None

        try:
            df = pl.read_parquet(parquet_file)
            # 确保日期格式正确
            if df['trade_date'].dtype == pl.Utf8:
                df = df.with_columns(pl.col('trade_date').str.to_date())
            return df
        except Exception as e:
            print(f"  ⚠️ 读取 {code} K线数据失败: {e}")
            return None

    def calculate_period_return(self, df: pl.DataFrame, start_date: date,
                                period_days: int) -> Optional[Dict]:
        """计算单个周期的收益率

        Args:
            df: K线数据DataFrame
            start_date: 起始日期（选股日）
            period_days: 周期天数

        Returns:
            周期收益数据字典
        """
        # 获取选股日数据
        start_data = df.filter(pl.col('trade_date') == start_date)
        if start_data.is_empty():
            return None

        start_row = start_data.row(0, named=True)
        start_close = start_row.get('close', 0)

        if not start_close or start_close <= 0:
            return None

        # 获取周期结束日期
        end_date = self.get_next_trading_date(start_date, period_days)
        if not end_date:
            return None

        # 获取周期内所有数据
        period_data = df.filter(
            (pl.col('trade_date') > start_date) &
            (pl.col('trade_date') <= end_date)
        ).sort('trade_date')

        if period_data.is_empty():
            return None

        # 获取周期结束日数据
        end_data = df.filter(pl.col('trade_date') == end_date)
        if end_data.is_empty():
            # 如果结束日没有数据，使用周期内最后一天
            end_row = period_data.tail(1).row(0, named=True)
        else:
            end_row = end_data.row(0, named=True)

        end_close = end_row.get('close', 0)

        # 计算收益率
        period_return = (end_close - start_close) / start_close * 100

        # 计算周期内最高最低价
        period_high = period_data['high'].max()
        period_low = period_data['low'].min()

        # 计算最大涨幅（从买入点到周期内最高点）
        max_gain = (period_high - start_close) / start_close * 100

        # 计算最大回撤（从买入点到周期内最低点）
        max_drawdown = (period_low - start_close) / start_close * 100

        return {
            'return': period_return,
            'high': period_high,
            'low': period_low,
            'max_gain': max_gain,
            'max_drawdown': max_drawdown
        }

    def calculate_multi_period_performance(self, code: str, report_date: str) -> Optional[Dict]:
        """计算股票的多周期表现

        Args:
            code: 股票代码
            report_date: 报告日期 (YYYY-MM-DD)

        Returns:
            多周期表现数据字典
        """
        report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()

        # 读取K线数据
        df = self.read_stock_kline(code)
        if df is None:
            return None

        result = {}

        for period in self.PERIODS:
            period_data = self.calculate_period_return(df, report_date_obj, period)
            if period_data:
                result[f'day{period}_return'] = period_data['return']
                result[f'day{period}_high'] = period_data['high']
                result[f'day{period}_low'] = period_data['low']
                result[f'day{period}_max_gain'] = period_data['max_gain']
                result[f'day{period}_max_drawdown'] = period_data['max_drawdown']

        return result if result else None

    def update_selections_performance(self, report_date: str,
                                       selection_type: Optional[str] = None) -> Dict:
        """更新某日的所有选股的多周期表现

        Args:
            report_date: 报告日期
            selection_type: 选股类型 (trend/short_term)，None表示全部

        Returns:
            更新统计信息
        """
        print(f"\n📊 计算多周期收益: {report_date}")

        # 获取选股列表
        selections = self.selection_service.get_selections_by_date(report_date, selection_type)

        if not selections:
            print(f"  ⚠️ {report_date} 无选股记录")
            return {'total': 0, 'updated': 0, 'failed': 0}

        print(f"  共 {len(selections)} 只选股")

        performance_data = []
        updated_count = 0
        failed_count = 0

        for sel in selections:
            code = sel['code']
            sel_type = sel['selection_type']

            # 计算多周期表现
            multi_period_data = self.calculate_multi_period_performance(code, report_date)

            if multi_period_data:
                multi_period_data['code'] = code
                multi_period_data['selection_type'] = sel_type
                performance_data.append(multi_period_data)
                updated_count += 1
            else:
                failed_count += 1

        # 批量更新数据库
        if performance_data:
            self.selection_service.update_multi_period_performance(report_date, performance_data)

        # 打印统计
        print(f"\n  ✅ 成功: {updated_count} 只")
        if failed_count > 0:
            print(f"  ⚠️ 失败: {failed_count} 只")

        return {
            'total': len(selections),
            'updated': updated_count,
            'failed': failed_count
        }

    def batch_update_performance(self, start_date: str, end_date: str,
                                  selection_type: Optional[str] = None) -> Dict:
        """批量更新日期范围内的多周期表现

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            selection_type: 选股类型

        Returns:
            批量更新统计
        """
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()

        print(f"\n🔄 批量更新多周期收益: {start_date} ~ {end_date}")

        total_stats = {'total': 0, 'updated': 0, 'failed': 0}
        current = start

        while current <= end:
            # 跳过周末
            if current.weekday() < 5:
                date_str = current.strftime('%Y-%m-%d')
                stats = self.update_selections_performance(date_str, selection_type)

                total_stats['total'] += stats['total']
                total_stats['updated'] += stats['updated']
                total_stats['failed'] += stats['failed']

            current += timedelta(days=1)

        print(f"\n📈 批量更新完成:")
        print(f"  总选股数: {total_stats['total']}")
        print(f"  成功更新: {total_stats['updated']}")
        print(f"  失败: {total_stats['failed']}")

        return total_stats

    def get_period_performance_summary(self, report_date: str,
                                        selection_type: str = 'trend') -> Dict:
        """获取某周期选股的表现汇总统计

        Args:
            report_date: 报告日期
            selection_type: 选股类型

        Returns:
            各周期表现统计
        """
        selections = self.selection_service.get_selections_by_date(report_date, selection_type)

        if not selections:
            return {}

        summary = {}

        for period in self.PERIODS:
            period_key = f'day{period}_return'
            returns = [s.get(period_key) for s in selections if s.get(period_key) is not None]

            if returns:
                summary[f'day{period}'] = {
                    'count': len(returns),
                    'avg_return': np.mean(returns),
                    'median_return': np.median(returns),
                    'max_return': max(returns),
                    'min_return': min(returns),
                    'win_rate': len([r for r in returns if r > 0]) / len(returns) * 100
                }

        return summary


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='多周期收益计算工具')
    parser.add_argument('command', choices=['update', 'batch'], help='命令')
    parser.add_argument('--date', help='日期 (YYYY-MM-DD)')
    parser.add_argument('--start', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--type', choices=['trend', 'short_term'], help='选股类型')

    args = parser.parse_args()

    service = MultiPeriodPerformanceService()

    if args.command == 'update':
        if not args.date:
            print("❌ 请指定 --date 参数")
            return
        service.update_selections_performance(args.date, args.type)

    elif args.command == 'batch':
        if not args.start or not args.end:
            print("❌ 请指定 --start 和 --end 参数")
            return
        service.batch_update_performance(args.start, args.end, args.type)


if __name__ == '__main__':
    main()
