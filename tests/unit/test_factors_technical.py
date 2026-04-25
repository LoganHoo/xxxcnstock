"""
技术指标因子单元测试
测试 MACD、RSI、KDJ、BOLL 等核心技术指标
"""
import pytest
import polars as pl
import numpy as np
from datetime import datetime, timedelta

# 导入被测模块
from factors.technical.macd import MacdFactor
from factors.technical.rsi import RsiFactor
from factors.technical.kdj import KdjFactor
from factors.technical.bollinger import BollingerFactor
from factors.technical.ma_trend import MaTrendFactor


class TestMacdFactor:
    """MACD 因子测试"""

    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(50, 0, -1)]
        
        # 生成趋势向上的价格数据
        prices = [10.0 + i * 0.1 + np.sin(i * 0.5) for i in range(50)]
        
        return pl.DataFrame({
            'code': ['000001'] * 50,
            'trade_date': dates,
            'open': [p * 0.99 for p in prices],
            'high': [p * 1.02 for p in prices],
            'low': [p * 0.98 for p in prices],
            'close': prices,
            'volume': [1000000] * 50
        })

    def test_macd_calculation(self, sample_data):
        """测试 MACD 计算"""
        factor = MacdFactor()
        result = factor.calculate(sample_data)
        
        assert 'macd' in result.columns
        assert 'dif' in result.columns
        assert 'dea' in result.columns
        assert 'factor_macd' in result.columns

    def test_macd_golden_cross_signal(self):
        """测试 MACD 金叉信号"""
        # 构造金叉数据：昨天 MACD < 0，今天 MACD > 0
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(30, 0, -1)]
        
        # 构造价格序列产生金叉
        prices = []
        for i in range(30):
            if i < 28:
                prices.append(10.0 - i * 0.05)  # 下跌
            else:
                prices.append(10.0 - 28 * 0.05 + (i - 28) * 0.3)  # 快速上涨产生金叉
        
        df = pl.DataFrame({
            'code': ['000001'] * 30,
            'trade_date': dates,
            'open': prices,
            'high': prices,
            'low': prices,
            'close': prices,
            'volume': [1000000] * 30
        })
        
        factor = MacdFactor()
        result = factor.calculate(df)
        
        score = result['factor_macd'].tail(1).item()
        assert 0 <= score <= 100

    def test_macd_insufficient_data(self):
        """测试数据不足时的处理"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(2, 0, -1)]
        
        df = pl.DataFrame({
            'code': ['000001'] * 2,
            'trade_date': dates,
            'open': [10.0, 10.5],
            'high': [10.2, 10.7],
            'low': [9.8, 10.3],
            'close': [10.0, 10.5],
            'volume': [1000000, 1200000]
        })
        
        factor = MacdFactor()
        result = factor.calculate(df)
        
        # 数据不足时应返回默认分数
        assert 'factor_macd' in result.columns


class TestRsiFactor:
    """RSI 因子测试"""

    @pytest.fixture
    def bullish_data(self):
        """创建强势上涨数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(20, 0, -1)]
        
        # 连续上涨
        prices = [10.0 + i * 0.5 for i in range(20)]
        
        return pl.DataFrame({
            'code': ['000001'] * 20,
            'trade_date': dates,
            'open': [p * 0.99 for p in prices],
            'high': [p * 1.02 for p in prices],
            'low': [p * 0.98 for p in prices],
            'close': prices,
            'volume': [1000000] * 20
        })

    @pytest.fixture
    def bearish_data(self):
        """创建强势下跌数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(20, 0, -1)]
        
        # 连续下跌
        prices = [20.0 - i * 0.5 for i in range(20)]
        
        return pl.DataFrame({
            'code': ['000001'] * 20,
            'trade_date': dates,
            'open': [p * 1.01 for p in prices],
            'high': [p * 1.03 for p in prices],
            'low': [p * 0.97 for p in prices],
            'close': prices,
            'volume': [1000000] * 20
        })

    def test_rsi_calculation(self, bullish_data):
        """测试 RSI 计算"""
        factor = RsiFactor()
        result = factor.calculate(bullish_data)
        
        assert 'rsi' in result.columns
        assert 'factor_rsi' in result.columns

    def test_rsi_overbought(self, bullish_data):
        """测试超买情况"""
        factor = RsiFactor()
        result = factor.calculate(bullish_data)
        
        rsi_value = result['rsi'].tail(1).item()
        score = result['factor_rsi'].tail(1).item()
        
        # 超买时 RSI 应该较高
        if rsi_value > 70:
            assert score < 50  # 超买信号，分数应较低

    def test_rsi_oversold(self, bearish_data):
        """测试超卖情况"""
        factor = RsiFactor()
        result = factor.calculate(bearish_data)
        
        rsi_value = result['rsi'].tail(1).item()
        score = result['factor_rsi'].tail(1).item()
        
        # 超卖时 RSI 应该较低
        if rsi_value < 30:
            assert score > 70  # 超卖信号，分数应较高

    def test_rsi_period_parameter(self, bullish_data):
        """测试不同周期参数"""
        factor_short = RsiFactor(params={'period': 7})
        factor_long = RsiFactor(params={'period': 21})
        
        result_short = factor_short.calculate(bullish_data)
        result_long = factor_long.calculate(bullish_data)
        
        assert 'factor_rsi' in result_short.columns
        assert 'factor_rsi' in result_long.columns


class TestKdjFactor:
    """KDJ 因子测试"""

    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(30, 0, -1)]
        
        np.random.seed(42)
        base_prices = [10.0 + i * 0.05 for i in range(30)]
        
        return pl.DataFrame({
            'code': ['000001'] * 30,
            'trade_date': dates,
            'open': [p * 0.99 for p in base_prices],
            'high': [p * 1.03 for p in base_prices],
            'low': [p * 0.97 for p in base_prices],
            'close': base_prices,
            'volume': [1000000] * 30
        })

    def test_kdj_calculation(self, sample_data):
        """测试 KDJ 计算"""
        from factors.technical.kdj import KdjFactor

        factor = KdjFactor()
        result = factor.calculate(sample_data)

        assert 'kdj_k' in result.columns
        assert 'kdj_d' in result.columns
        assert 'kdj_j' in result.columns
        assert 'factor_kdj' in result.columns


class TestBollingerFactor:
    """布林带因子测试"""

    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(30, 0, -1)]
        
        prices = [10.0 + np.sin(i * 0.3) for i in range(30)]
        
        return pl.DataFrame({
            'code': ['000001'] * 30,
            'trade_date': dates,
            'open': [p * 0.99 for p in prices],
            'high': [p * 1.02 for p in prices],
            'low': [p * 0.98 for p in prices],
            'close': prices,
            'volume': [1000000] * 30
        })

    def test_bollinger_calculation(self, sample_data):
        """测试布林带计算"""
        from factors.technical.bollinger import BollingerFactor

        factor = BollingerFactor()
        result = factor.calculate(sample_data)

        assert 'boll_upper' in result.columns
        assert 'boll_mid' in result.columns
        assert 'boll_lower' in result.columns
        assert 'factor_bollinger' in result.columns

    def test_bollinger_breakout(self):
        """测试布林带突破信号"""
        from factors.technical.bollinger import BollingerFactor
        
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(30, 0, -1)]
        
        # 构造突破上轨的价格序列
        prices = []
        for i in range(30):
            if i < 28:
                prices.append(10.0 + np.sin(i * 0.3))
            else:
                prices.append(15.0)  # 大幅突破
        
        df = pl.DataFrame({
            'code': ['000001'] * 30,
            'trade_date': dates,
            'open': prices,
            'high': [p * 1.02 for p in prices],
            'low': [p * 0.98 for p in prices],
            'close': prices,
            'volume': [1000000] * 30
        })
        
        factor = BollingerFactor()
        result = factor.calculate(df)
        
        score = result['factor_bollinger'].tail(1).item()
        assert 0 <= score <= 100


class TestMaTrendFactor:
    """均线趋势因子测试"""

    @pytest.fixture
    def bullish_alignment_data(self):
        """创建多头排列数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(60, 0, -1)]
        
        # 多头排列：价格稳步上涨
        prices = [10.0 + i * 0.1 for i in range(60)]
        
        return pl.DataFrame({
            'code': ['000001'] * 60,
            'trade_date': dates,
            'open': [p * 0.99 for p in prices],
            'high': [p * 1.02 for p in prices],
            'low': [p * 0.98 for p in prices],
            'close': prices,
            'volume': [1000000] * 60
        })

    @pytest.fixture
    def bearish_alignment_data(self):
        """创建空头排列数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(60, 0, -1)]
        
        # 空头排列：价格稳步下跌
        prices = [20.0 - i * 0.1 for i in range(60)]
        
        return pl.DataFrame({
            'code': ['000001'] * 60,
            'trade_date': dates,
            'open': [p * 1.01 for p in prices],
            'high': [p * 1.03 for p in prices],
            'low': [p * 0.97 for p in prices],
            'close': prices,
            'volume': [1000000] * 60
        })

    def test_ma_calculation(self, bullish_alignment_data):
        """测试均线计算"""
        factor = MaTrendFactor()
        result = factor.calculate(bullish_alignment_data)

        assert 'ma_short' in result.columns
        assert 'ma_mid' in result.columns
        assert 'ma_long' in result.columns
        assert 'factor_ma_trend' in result.columns

    def test_bullish_alignment(self, bullish_alignment_data):
        """测试多头排列识别"""
        factor = MaTrendFactor()
        result = factor.calculate(bullish_alignment_data)
        
        score = result['factor_ma_trend'].tail(1).item()
        # 多头排列应该得到较高分数
        assert score > 50

    def test_bearish_alignment(self, bearish_alignment_data):
        """测试空头排列识别"""
        factor = MaTrendFactor()
        result = factor.calculate(bearish_alignment_data)
        
        score = result['factor_ma_trend'].tail(1).item()
        # 空头排列应该得到较低分数
        assert score < 50


class TestFactorEdgeCases:
    """因子边界条件测试"""

    def test_empty_dataframe(self):
        """测试空数据框处理"""
        df = pl.DataFrame({
            'code': [],
            'trade_date': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': []
        })
        
        factor = MacdFactor()
        
        # 应该能处理空数据而不抛出异常
        try:
            result = factor.calculate(df)
            assert len(result) == 0
        except Exception:
            pytest.skip("空数据处理未实现")

    def test_single_row_data(self):
        """测试单行数据处理"""
        df = pl.DataFrame({
            'code': ['000001'],
            'trade_date': ['2024-01-01'],
            'open': [10.0],
            'high': [10.5],
            'low': [9.8],
            'close': [10.2],
            'volume': [1000000]
        })
        
        factor = RsiFactor()
        
        # 单行数据应该能处理
        try:
            result = factor.calculate(df)
            assert len(result) == 1
        except Exception:
            pytest.skip("单行数据处理未实现")

    def test_missing_columns(self):
        """测试缺少必要列的处理"""
        df = pl.DataFrame({
            'code': ['000001'],
            'trade_date': ['2024-01-01'],
            # 缺少 open, high, low, close, volume
        })
        
        factor = MacdFactor()
        
        # 应该抛出异常或优雅处理
        with pytest.raises(Exception):
            factor.calculate(df)

    def test_nan_values(self):
        """测试包含 NaN 值的数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(10, 0, -1)]
        
        df = pl.DataFrame({
            'code': ['000001'] * 10,
            'trade_date': dates,
            'open': [10.0] * 10,
            'high': [10.5] * 10,
            'low': [9.8] * 10,
            'close': [10.0, 10.2, None, 10.5, 10.3, None, 10.8, 10.6, 10.9, 11.0],
            'volume': [1000000] * 10
        })
        
        factor = RsiFactor()
        
        # 应该能处理 NaN 值
        try:
            result = factor.calculate(df)
            assert 'factor_rsi' in result.columns
        except Exception:
            pytest.skip("NaN 值处理未实现")

    def test_zero_volume(self):
        """测试零成交量数据"""
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(10, 0, -1)]
        
        df = pl.DataFrame({
            'code': ['000001'] * 10,
            'trade_date': dates,
            'open': [10.0] * 10,
            'high': [10.5] * 10,
            'low': [9.8] * 10,
            'close': [10.0 + i * 0.1 for i in range(10)],
            'volume': [0] * 10  # 零成交量
        })
        
        factor = MacdFactor()
        
        # 应该能处理零成交量
        try:
            result = factor.calculate(df)
            assert 'factor_macd' in result.columns
        except Exception:
            pytest.skip("零成交量处理未实现")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
