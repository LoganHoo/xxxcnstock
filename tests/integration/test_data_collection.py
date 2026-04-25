#!/usr/bin/env python3
"""
数据采集集成测试
测试完整的数据采集流程
"""
import pytest
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os


class TestDataCollectionIntegration:
    """数据采集集成测试"""

    def test_kline_data_schema(self):
        """测试K线数据结构"""
        # 读取示例K线数据
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path)

        # 验证必需列
        required_columns = [
            'trade_date', 'code', 'open', 'high', 'low', 'close',
            'volume', 'amount'
        ]
        for col in required_columns:
            assert col in df.columns, f"缺少必需列: {col}"

    def test_kline_data_quality(self):
        """测试K线数据质量"""
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path)

        # 验证价格逻辑
        assert (df['high'] >= df['low']).all(), "最高价应 >= 最低价"
        assert (df['high'] >= df['open']).all(), "最高价应 >= 开盘价"
        assert (df['high'] >= df['close']).all(), "最高价应 >= 收盘价"
        assert (df['low'] <= df['open']).all(), "最低价应 <= 开盘价"
        assert (df['low'] <= df['close']).all(), "最低价应 <= 收盘价"

        # 验证成交量
        assert (df['volume'] >= 0).all(), "成交量不应为负"

    def test_kline_data_freshness(self):
        """测试K线数据新鲜度"""
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path)

        # 获取最新日期
        latest_date = df['trade_date'].max()
        latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')

        # 数据不应超过30天
        days_diff = (datetime.now() - latest_dt).days
        assert days_diff <= 30, f"数据过旧，最新数据是 {days_diff} 天前"

    def test_multiple_stocks_consistency(self):
        """测试多只股票数据一致性"""
        kline_dir = Path("data/kline")
        if not kline_dir.exists():
            pytest.skip("K线数据目录不存在")

        # 随机选取5只股票
        parquet_files = list(kline_dir.glob("*.parquet"))[:5]
        if len(parquet_files) < 5:
            pytest.skip("股票数量不足")

        date_ranges = []
        for file in parquet_files:
            df = pl.read_parquet(file)
            date_ranges.append({
                'code': file.stem,
                'min_date': df['trade_date'].min(),
                'max_date': df['trade_date'].max(),
                'count': len(df)
            })

        # 验证所有股票都有数据
        for info in date_ranges:
            assert info['count'] > 0, f"股票 {info['code']} 没有数据"

    def test_stock_list_exists(self):
        """测试股票列表文件存在"""
        stock_list_path = Path("data/stock_list.parquet")
        if not stock_list_path.exists():
            pytest.skip("股票列表文件不存在")

        df = pl.read_parquet(stock_list_path)
        assert len(df) > 0, "股票列表不应为空"
        assert 'code' in df.columns, "股票列表应包含code列"
        assert 'name' in df.columns, "股票列表应包含name列"


class TestMarketGuardian:
    """市场守护者测试"""

    def test_is_trading_day_weekend(self):
        """测试周末判断"""
        from core.market_guardian import MarketGuardian

        # 周六 (2026-04-18)
        saturday = datetime(2026, 4, 18)
        assert not MarketGuardian.is_trading_day(saturday), "周六不应是交易日"

        # 周日 (2026-04-19)
        sunday = datetime(2026, 4, 19)
        assert not MarketGuardian.is_trading_day(sunday), "周日不应是交易日"

    def test_is_trading_day_holiday(self):
        """测试节假日判断"""
        from core.market_guardian import MarketGuardian

        # 2026年春节
        spring_festival = datetime(2026, 2, 10)
        assert not MarketGuardian.is_trading_day(spring_festival), "春节不应是交易日"

    def test_check_collection_allowed_historical(self):
        """测试历史数据采集允许"""
        from core.market_guardian import MarketGuardian

        # 任何时间都可以采集历史数据
        allowed, message = MarketGuardian.check_collection_allowed(
            target_date="2026-04-10"
        )
        assert allowed, "历史数据应允许采集"

    def test_check_collection_allowed_force_date(self):
        """测试强制日期采集"""
        from core.market_guardian import MarketGuardian

        # 强制指定日期可以采集
        allowed, message = MarketGuardian.check_collection_allowed(
            force_date="2026-04-10"
        )
        assert allowed, "强制指定日期应允许采集"


class TestDataQualityMetrics:
    """数据质量指标测试"""

    def test_data_completeness(self):
        """测试数据完整性"""
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path)

        # 检查核心列缺失值 (排除可选列如 fetch_time)
        core_columns = ['trade_date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']
        for col in core_columns:
            if col in df.columns:
                null_count = df[col].null_count()
                total_count = len(df)
                null_ratio = null_count / total_count
                assert null_ratio < 0.05, f"核心列 {col} 缺失率过高: {null_ratio:.2%}"

    def test_price_continuity(self):
        """测试价格连续性"""
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path).sort('trade_date')

        # 检查价格是否为零
        zero_prices = (df['close'] == 0).sum()
        assert zero_prices == 0, f"存在 {zero_prices} 条收盘价为0的记录"

    def test_volume_reasonableness(self):
        """测试成交量合理性"""
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path)

        # 检查异常成交量 (超过均值10倍)
        mean_volume = df['volume'].mean()
        extreme_volume = (df['volume'] > mean_volume * 10).sum()
        assert extreme_volume < len(df) * 0.01, "异常成交量记录过多"


class TestDataPipeline:
    """数据管道测试"""

    def test_kline_to_factor_pipeline(self):
        """测试K线到因子计算管道"""
        kline_path = Path("data/kline/000001.parquet")
        if not kline_path.exists():
            pytest.skip("K线数据文件不存在")

        df = pl.read_parquet(kline_path)

        # 计算简单移动平均
        df = df.with_columns([
            pl.col('close').rolling_mean(5).alias('ma5'),
            pl.col('close').rolling_mean(10).alias('ma10'),
        ])

        # 验证因子计算成功
        assert 'ma5' in df.columns, "MA5计算失败"
        assert 'ma10' in df.columns, "MA10计算失败"

        # 验证因子值合理
        assert df['ma5'].null_count() < len(df) * 0.2, "MA5缺失值过多"
        assert df['ma10'].null_count() < len(df) * 0.3, "MA10缺失值过多"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
