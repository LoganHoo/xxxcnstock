#!/usr/bin/env python3
"""
大盘指数关键位计算服务
- 计算上证指数、深证成指、创业板指的关键位
- 支持支撑位、压力位、均线、斐波那契等技术指标
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import date

from services.key_levels import KeyLevels


class IndexKeyLevelsService:
    """大盘指数关键位计算服务"""

    # 指数代码映射（兼容多种格式）
    INDEX_CODES = {
        'sh000001': '000001',   # 上证指数
        'sz399001': '399001',   # 深证成指
        'sz399006': '399006',   # 创业板指
        'sh000688': '000688',   # 科创50
        'sh000016': '000016',   # 上证50
        'sh000300': '000300',   # 沪深300
        'sh000905': '000905',   # 中证500
        'sh000852': '000852',   # 中证1000
    }

    # 反向映射
    CODE_TO_NAME = {
        '000001': '上证指数',
        '399001': '深证成指',
        '399006': '创业板指',
        '000688': '科创50',
        '000016': '上证50',
        '000300': '沪深300',
        '000905': '中证500',
        '000852': '中证1000',
    }

    def __init__(self):
        self.kline_dir = Path('data/index')
        self.key_levels = KeyLevels()

    def load_index_data(self, index_code: str, days: int = 120) -> Optional[pl.DataFrame]:
        """加载指数K线数据（优先从Parquet，其次从MySQL）

        Args:
            index_code: 指数代码
            days: 加载天数

        Returns:
            DataFrame或None
        """
        # 首先尝试从Parquet文件加载
        parquet_file = self.kline_dir / f"{index_code}.parquet"

        if parquet_file.exists():
            try:
                df = pl.read_parquet(parquet_file)

                # 确保日期格式正确
                if df['trade_date'].dtype == pl.Utf8:
                    df = df.with_columns(pl.col('trade_date').str.to_date())

                # 按日期排序并取最近N天
                df = df.sort('trade_date').tail(days)

                return df
            except Exception as e:
                print(f"⚠️ Parquet加载失败，尝试MySQL: {e}")

        # 从MySQL加载
        return self._load_from_mysql(index_code, days)

    def _load_from_mysql(self, index_code: str, days: int = 120) -> Optional[pl.DataFrame]:
        """从MySQL加载指数数据"""
        try:
            import pymysql
            from dotenv import load_dotenv
            load_dotenv()

            conn = pymysql.connect(
                host=os.getenv('DB_HOST', '49.233.10.199'),
                port=int(os.getenv('DB_PORT', '3306')),
                user=os.getenv('DB_USER', 'nextai'),
                password=os.getenv('DB_PASSWORD', '100200'),
                database=os.getenv('DB_NAME', 'xcn_db'),
                charset='utf8mb4'
            )

            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM index_daily
                    WHERE code = %s
                    ORDER BY trade_date DESC
                    LIMIT %s
                """, (index_code, days))

                rows = cursor.fetchall()
                conn.close()

                if not rows:
                    return None

                # 转换为DataFrame
                data = {
                    'trade_date': [r[0] for r in rows],
                    'open': [float(r[1]) for r in rows],
                    'high': [float(r[2]) for r in rows],
                    'low': [float(r[3]) for r in rows],
                    'close': [float(r[4]) for r in rows],
                    'volume': [int(r[5]) for r in rows],
                    'amount': [float(r[6]) if r[6] else 0 for r in rows],
                }

                df = pl.DataFrame(data)
                df = df.sort('trade_date')

                return df

        except Exception as e:
            print(f"❌ MySQL加载失败 {index_code}: {e}")
            return None

    def calculate_index_key_levels(self, index_code: str, target_date: date = None) -> Dict[str, Any]:
        """计算指定指数的关键位

        Args:
            index_code: 指数代码
            target_date: 目标日期，默认为最新日期

        Returns:
            关键位数据字典
        """
        # 标准化代码
        normalized_code = self._normalize_code(index_code)
        df = self.load_index_data(normalized_code)

        if df is None or df.is_empty():
            return {'error': '数据不足'}

        # 获取最新数据
        if target_date:
            latest_data = df.filter(pl.col('trade_date') == target_date)
            if latest_data.is_empty():
                latest_data = df.tail(1)
        else:
            latest_data = df.tail(1)

        if latest_data.is_empty():
            return {'error': '无最新数据'}

        latest = latest_data.row(0, named=True)

        # 计算关键位
        closes = df['close'].to_list()
        highs = df['high'].to_list()
        lows = df['low'].to_list()

        key_levels = self.key_levels.calculate_key_levels(closes, highs, lows)

        # 添加指数信息
        key_levels['index_code'] = normalized_code
        key_levels['index_name'] = self.get_index_name(normalized_code)
        key_levels['current_price'] = latest.get('close', 0)
        key_levels['change_pct'] = latest.get('pct_change', 0)
        key_levels['volume'] = latest.get('volume', 0)
        key_levels['trade_date'] = str(latest.get('trade_date', ''))

        return key_levels

    def _normalize_code(self, index_code: str) -> str:
        """标准化指数代码"""
        # 如果已经是6位数字，直接返回
        if len(index_code) == 6 and index_code.isdigit():
            return index_code
        # 如果带前缀（如 sh000001），去掉前缀
        if len(index_code) == 8:
            return index_code[2:]
        # 尝试从映射获取
        return self.INDEX_CODES.get(index_code, index_code)

    def get_index_name(self, index_code: str) -> str:
        """获取指数名称"""
        code = self._normalize_code(index_code)
        return self.CODE_TO_NAME.get(code, index_code)

    def get_all_index_key_levels(self, target_date: date = None) -> Dict[str, Dict[str, Any]]:
        """获取所有主要指数的关键位

        Args:
            target_date: 目标日期

        Returns:
            各指数关键位字典
        """
        results = {}

        # 主要指数
        main_indices = ['sh000001', 'sz399001', 'sz399006']

        for index_code in main_indices:
            key_levels = self.calculate_index_key_levels(index_code, target_date)
            if 'error' not in key_levels:
                results[index_code] = key_levels

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
            'index_name': key_levels.get('index_name', ''),
            'index_code': key_levels.get('index_code', ''),
            'current_price': current,
            'change_pct': key_levels.get('change_pct', 0),

            # 均线
            'ma5': key_levels.get('ma5', 0),
            'ma10': key_levels.get('ma10', 0),
            'ma20': key_levels.get('ma20', 0),
            'ma60': key_levels.get('ma60', 0),

            # 支撑位
            'supports': [
                {'name': 'MA20', 'value': key_levels.get('ma20', 0), 'distance': calc_distance(key_levels.get('ma20', 0))},
                {'name': 'MA60', 'value': key_levels.get('ma60', 0), 'distance': calc_distance(key_levels.get('ma60', 0))},
                {'name': '近期低点', 'value': key_levels.get('support_recent', 0), 'distance': calc_distance(key_levels.get('support_recent', 0))},
                {'name': '20日低点', 'value': key_levels.get('support_low', 0), 'distance': calc_distance(key_levels.get('support_low', 0))},
            ],

            # 压力位
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

        return formatted


def main():
    """测试关键位计算"""
    service = IndexKeyLevelsService()

    # 获取上证指数关键位
    print("\n" + "="*60)
    print("上证指数关键位")
    print("="*60)

    key_levels = service.calculate_index_key_levels('sh000001')
    formatted = service.format_key_levels_for_report(key_levels)

    if 'error' not in formatted:
        print(f"\n当前价格: {formatted['current_price']:.2f} ({formatted['change_pct']:+.2f}%)")
        print(f"\n均线:")
        print(f"  MA5:  {formatted['ma5']:.2f}")
        print(f"  MA10: {formatted['ma10']:.2f}")
        print(f"  MA20: {formatted['ma20']:.2f}")
        print(f"  MA60: {formatted['ma60']:.2f}")

        print(f"\n支撑位:")
        for s in formatted['supports']:
            print(f"  {s['name']}: {s['value']:.2f} ({s['distance']:+.2f}%)")

        print(f"\n压力位:")
        for r in formatted['resistances']:
            print(f"  {r['name']}: {r['value']:.2f} ({r['distance']:+.2f}%)")

        print(f"\n布林带:")
        print(f"  上轨: {formatted['bb_upper']:.2f}")
        print(f"  中轨: {formatted['bb_mid']:.2f}")
        print(f"  下轨: {formatted['bb_lower']:.2f}")
    else:
        print(f"错误: {formatted['error']}")


if __name__ == '__main__':
    main()
