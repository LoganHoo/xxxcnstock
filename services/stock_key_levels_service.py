#!/usr/bin/env python3
"""
个股关键位计算服务
- 批量计算选股股票的关键位
- 支持支撑位、压力位、均线等技术指标
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import date
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

from services.key_levels import KeyLevels


class StockKeyLevelsService:
    """个股关键位计算服务"""

    def __init__(self):
        self.kline_dir = Path('data/kline')
        self.key_levels = KeyLevels()

    def load_stock_data(self, stock_code: str, days: int = 120) -> Optional[pl.DataFrame]:
        """加载个股K线数据

        Args:
            stock_code: 股票代码
            days: 加载天数

        Returns:
            DataFrame或None
        """
        parquet_file = self.kline_dir / f"{stock_code}.parquet"

        if not parquet_file.exists():
            return None

        try:
            df = pl.read_parquet(parquet_file)

            # 确保日期格式正确
            if df['trade_date'].dtype == pl.Utf8:
                df = df.with_columns(pl.col('trade_date').str.to_date())

            # 按日期排序并取最近N天
            df = df.sort('trade_date').tail(days)

            return df

        except Exception as e:
            print(f"⚠️ 加载股票数据失败 {stock_code}: {e}")
            return None

    def calculate_stock_key_levels(self, stock_code: str, target_date: date = None) -> Dict[str, Any]:
        """计算指定股票的关键位

        Args:
            stock_code: 股票代码
            target_date: 目标日期，默认为最新日期

        Returns:
            关键位数据字典
        """
        df = self.load_stock_data(stock_code)

        if df is None or df.is_empty():
            return {'error': '数据不足', 'code': stock_code}

        # 获取最新数据
        if target_date:
            latest_data = df.filter(pl.col('trade_date') == target_date)
            if latest_data.is_empty():
                latest_data = df.tail(1)
        else:
            latest_data = df.tail(1)

        if latest_data.is_empty():
            return {'error': '无最新数据', 'code': stock_code}

        latest = latest_data.row(0, named=True)

        # 计算关键位
        closes = df['close'].to_list()
        highs = df['high'].to_list()
        lows = df['low'].to_list()

        key_levels = self.key_levels.calculate_key_levels(closes, highs, lows)

        # 添加股票信息
        key_levels['code'] = stock_code
        key_levels['name'] = latest.get('name', stock_code)
        key_levels['current_price'] = latest.get('close', 0)
        key_levels['change_pct'] = latest.get('pct_change', 0)
        key_levels['volume'] = latest.get('volume', 0)
        key_levels['trade_date'] = str(latest.get('trade_date', ''))

        return key_levels

    def calculate_batch_key_levels(self, stock_codes: List[str], target_date: date = None,
                                   max_workers: int = None) -> Dict[str, Dict[str, Any]]:
        """批量计算多只股票的关键位

        Args:
            stock_codes: 股票代码列表
            target_date: 目标日期
            max_workers: 并行 workers 数量

        Returns:
            各股票关键位字典
        """
        if max_workers is None:
            max_workers = min(multiprocessing.cpu_count(), 8)

        results = {}

        print(f"\n📊 批量计算 {len(stock_codes)} 只股票的关键位...")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_code = {
                executor.submit(self.calculate_stock_key_levels, code, target_date): code
                for code in stock_codes
            }

            completed = 0
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    key_levels = future.result()
                    if 'error' not in key_levels:
                        results[code] = key_levels
                    completed += 1
                    if completed % 10 == 0:
                        print(f"  进度: {completed}/{len(stock_codes)}")
                except Exception as e:
                    print(f"⚠️ 计算 {code} 关键位失败: {e}")

        print(f"✅ 完成: {len(results)}/{len(stock_codes)} 只股票")
        return results

    def format_key_levels_for_report(self, key_levels: Dict[str, Any]) -> Dict[str, Any]:
        """格式化关键位用于报告展示

        Args:
            key_levels: 关键位原始数据

        Returns:
            格式化后的数据
        """
        if 'error' in key_levels:
            return key_levels

        current = key_levels.get('current_price', 0)

        # 计算各关键位与当前价的距离
        def calc_distance(level):
            if current and level:
                return (level - current) / current * 100
            return 0

        formatted = {
            'code': key_levels.get('code', ''),
            'name': key_levels.get('name', ''),
            'current_price': current,
            'change_pct': key_levels.get('change_pct', 0),

            # 均线
            'ma5': key_levels.get('ma5', 0),
            'ma10': key_levels.get('ma10', 0),
            'ma20': key_levels.get('ma20', 0),
            'ma60': key_levels.get('ma60', 0),

            # 支撑位（按距离当前价从近到远排序）
            'supports': [
                {'name': 'MA5', 'value': key_levels.get('ma5', 0), 'distance': calc_distance(key_levels.get('ma5', 0))},
                {'name': 'MA10', 'value': key_levels.get('ma10', 0), 'distance': calc_distance(key_levels.get('ma10', 0))},
                {'name': 'MA20', 'value': key_levels.get('ma20', 0), 'distance': calc_distance(key_levels.get('ma20', 0))},
                {'name': 'MA60', 'value': key_levels.get('ma60', 0), 'distance': calc_distance(key_levels.get('ma60', 0))},
                {'name': '近期低点', 'value': key_levels.get('support_recent', 0), 'distance': calc_distance(key_levels.get('support_recent', 0))},
            ],

            # 压力位（按距离当前价从近到远排序）
            'resistances': [
                {'name': 'MA5', 'value': key_levels.get('ma5', 0), 'distance': calc_distance(key_levels.get('ma5', 0))},
                {'name': '近期高点', 'value': key_levels.get('resistance_recent', 0), 'distance': calc_distance(key_levels.get('resistance_recent', 0))},
                {'name': '20日高点', 'value': key_levels.get('resistance_high', 0), 'distance': calc_distance(key_levels.get('resistance_high', 0))},
            ],

            # 布林带
            'bb_upper': key_levels.get('bb_upper', 0),
            'bb_mid': key_levels.get('bb_mid', 0),
            'bb_lower': key_levels.get('bb_lower', 0),

            # Pivot点
            'pivot': key_levels.get('pivot', 0),
            'pivot_r1': key_levels.get('pivot_r1', 0),
            'pivot_s1': key_levels.get('pivot_s1', 0),

            # 强支撑/压力区间
            'strong_support': key_levels.get('strong_support_zone', 0),
            'strong_resistance': key_levels.get('strong_resistance_zone', 0),

            # 价格位置
            'price_position': key_levels.get('price_position', 'neutral'),
        }

        # 过滤掉无效的支撑位（高于当前价的不是支撑）
        formatted['supports'] = [s for s in formatted['supports'] if s['value'] < current * 1.02]
        formatted['supports'].sort(key=lambda x: x['value'], reverse=True)

        # 过滤掉无效的阻力位（低于当前价的不是阻力）
        formatted['resistances'] = [r for r in formatted['resistances'] if r['value'] > current * 0.98]
        formatted['resistances'].sort(key=lambda x: x['value'])

        return formatted


def main():
    """测试关键位计算"""
    service = StockKeyLevelsService()

    # 测试单只股票
    test_code = '000001'

    print("\n" + "="*60)
    print(f"股票 {test_code} 关键位")
    print("="*60)

    key_levels = service.calculate_stock_key_levels(test_code)
    formatted = service.format_key_levels_for_report(key_levels)

    if 'error' not in formatted:
        print(f"\n当前价格: {formatted['current_price']:.2f} ({formatted['change_pct']:+.2f}%)")
        print(f"\n均线:")
        print(f"  MA5:  {formatted['ma5']:.2f}")
        print(f"  MA10: {formatted['ma10']:.2f}")
        print(f"  MA20: {formatted['ma20']:.2f}")
        print(f"  MA60: {formatted['ma60']:.2f}")

        print(f"\n支撑位:")
        for s in formatted['supports'][:3]:
            print(f"  {s['name']}: {s['value']:.2f} ({s['distance']:+.2f}%)")

        print(f"\n压力位:")
        for r in formatted['resistances'][:3]:
            print(f"  {r['name']}: {r['value']:.2f} ({r['distance']:+.2f}%)")
    else:
        print(f"错误: {formatted['error']}")


if __name__ == '__main__':
    main()
