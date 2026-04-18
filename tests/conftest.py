#!/usr/bin/env python3
"""
Pytest全局配置和Fixture定义

提供测试所需的共享fixture和数据工厂
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# =============================================================================
# K线数据Fixture
# =============================================================================

@pytest.fixture
def sample_kline_data():
    """样本K线数据 - 30天日线数据"""
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    base_price = 10.0
    
    # 生成带有趋势的价格数据
    closes = [base_price + i * 0.1 + np.sin(i * 0.5) * 0.5 for i in range(30)]
    
    df = pd.DataFrame({
        'date': dates,
        'open': [c - 0.2 + np.random.random() * 0.4 for c in closes],
        'high': [c + 0.3 + np.random.random() * 0.3 for c in closes],
        'low': [c - 0.3 - np.random.random() * 0.3 for c in closes],
        'close': closes,
        'volume': [int(10000 + i * 100 + np.random.random() * 5000) for i in range(30)],
        'amount': [0.0] * 30  # 将在下面计算
    })
    
    # 确保OHLC逻辑正确
    for i in range(len(df)):
        df.loc[i, 'high'] = max(df.loc[i, 'open'], df.loc[i, 'close'], df.loc[i, 'high'])
        df.loc[i, 'low'] = min(df.loc[i, 'open'], df.loc[i, 'close'], df.loc[i, 'low'])
        df.loc[i, 'amount'] = df.loc[i, 'volume'] * df.loc[i, 'close']
    
    return df


@pytest.fixture
def sample_kline_with_gap():
    """带有缺口（跳空）的K线数据"""
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    
    df = pd.DataFrame({
        'date': dates,
        'open': [10.0, 10.5, 11.0, 10.8, 12.0, 11.8, 12.5, 12.3, 13.0, 12.8],
        'high': [10.5, 11.0, 11.3, 11.2, 12.5, 12.2, 13.0, 12.8, 13.5, 13.2],
        'low': [9.8, 10.2, 10.8, 10.5, 11.8, 11.5, 12.2, 12.0, 12.8, 12.5],
        'close': [10.5, 10.8, 11.2, 11.0, 12.2, 12.0, 12.8, 12.5, 13.2, 13.0],
        'volume': [10000, 12000, 15000, 11000, 20000, 18000, 22000, 19000, 25000, 21000]
    })
    
    return df


@pytest.fixture
def sample_kline_limitup():
    """涨停股票K线数据"""
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    # 连续涨停: 10 -> 11 -> 12.1 -> 13.31 -> 14.64
    df = pd.DataFrame({
        'date': dates,
        'open': [10.0, 11.0, 12.1, 13.31, 14.64],
        'high': [11.0, 12.1, 13.31, 14.64, 14.64],
        'low': [10.0, 11.0, 12.1, 13.31, 14.64],
        'close': [11.0, 12.1, 13.31, 14.64, 14.64],  # 连续涨停
        'volume': [50000, 30000, 20000, 15000, 10000],  # 涨停后缩量
        'is_limitup': [True, True, True, True, True]
    })
    
    return df


# =============================================================================
# 财务数据Fixture
# =============================================================================

@pytest.fixture
def sample_financial_data():
    """样本财务数据"""
    return {
        'code': '000001',
        'name': '平安银行',
        'roe': 15.5,
        'roe_diluted': 15.2,
        'gross_margin': 35.0,
        'net_margin': 25.0,
        'profit_growth': 30.0,
        'revenue_growth': 20.0,
        'pe': 15.0,
        'pb': 2.0,
        'ps': 3.0,
        'eps': 2.5,
        'bps': 15.0,
        'total_assets': 500000000000,
        'total_liabilities': 450000000000,
        'equity': 50000000000
    }


@pytest.fixture
def sample_financial_dataframe():
    """样本财务数据DataFrame"""
    return pd.DataFrame({
        'code': ['000001', '000002', '000003', '000004', '000005'],
        'name': ['Stock A', 'Stock B', 'Stock C', 'Stock D', 'Stock E'],
        'roe': [15.0, 8.0, 12.0, 20.0, 18.0],
        'gross_margin': [25.0, 30.0, 15.0, 35.0, 28.0],
        'profit_growth': [30.0, 25.0, 35.0, 15.0, 22.0],
        'pe': [15.0, 20.0, 25.0, 55.0, 30.0],
        'pb': [2.0, 3.0, 4.0, 5.0, 12.0],
        'market_cap': [500, 300, 200, 800, 150]  # 亿
    })


# =============================================================================
# 宏观数据Fixture
# =============================================================================

@pytest.fixture
def mock_bullish_market():
    """模拟看涨市场环境"""
    return {
        'shibor': pd.Series([2.5, 2.4, 2.3, 2.2, 2.1]),  # 下降趋势
        'liquidity_score': 75,  # 高流动性
        'credit_pulse': 1.2,    # 信贷脉冲为正
        'm2_growth': 10.5,
        'macd_signal': 'golden_cross',
        'index_trend': 'up'
    }


@pytest.fixture
def mock_bearish_market():
    """模拟看跌市场环境"""
    return {
        'shibor': pd.Series([2.0, 2.3, 2.6, 3.0, 3.5]),  # 上升趋势
        'liquidity_score': 25,  # 低流动性
        'credit_pulse': -0.5,   # 信贷脉冲为负
        'm2_growth': 5.0,
        'macd_signal': 'death_cross',
        'index_trend': 'down'
    }


@pytest.fixture
def mock_neutral_market():
    """模拟中性市场环境"""
    return {
        'shibor': pd.Series([2.5, 2.5, 2.5, 2.5, 2.5]),  # 平稳
        'liquidity_score': 50,  # 中性流动性
        'credit_pulse': 0.0,    # 信贷脉冲中性
        'm2_growth': 8.0,
        'macd_signal': 'flat',
        'index_trend': 'sideways'
    }


# =============================================================================
# 持仓数据Fixture
# =============================================================================

@pytest.fixture
def sample_position():
    """样本持仓数据"""
    return {
        'code': '000001',
        'name': '平安银行',
        'cost_price': 15.0,
        'current_price': 16.5,
        'quantity': 1000,
        'market_value': 16500,
        'profit_pct': 10.0,
        'profit_amount': 1500,
        'entry_date': datetime(2024, 1, 1),
        'ema_20': 15.5,
        'highest_price': 17.0,
        'lowest_price': 14.5
    }


@pytest.fixture
def sample_position_profit():
    """盈利持仓数据"""
    return {
        'code': '000001',
        'cost_price': 10.0,
        'current_price': 12.0,  # 盈利20%
        'quantity': 1000,
        'market_value': 12000,
        'profit_pct': 20.0,
        'profit_amount': 2000,
        'entry_date': datetime(2024, 1, 1),
        'ema_20': 11.0,
        'highest_price': 12.5
    }


@pytest.fixture
def sample_position_loss():
    """亏损持仓数据"""
    return {
        'code': '000001',
        'cost_price': 15.0,
        'current_price': 13.0,  # 亏损13.3%
        'quantity': 1000,
        'market_value': 13000,
        'profit_pct': -13.3,
        'profit_amount': -2000,
        'entry_date': datetime(2024, 1, 1),
        'ema_20': 14.0,
        'highest_price': 15.5
    }


# =============================================================================
# 市场数据Fixture
# =============================================================================

@pytest.fixture
def sample_market_data():
    """样本市场数据"""
    return {
        'index_code': '000001.SH',
        'index_name': '上证指数',
        'close': 3050.0,
        'change_pct': 1.5,
        'volume': 350000000,
        'turnover': 450000000000,
        'advance_count': 3000,
        'decline_count': 1500,
        'flat_count': 200
    }


@pytest.fixture
def sample_market_crash():
    """市场暴跌数据"""
    return {
        'index_code': '000001.SH',
        'index_name': '上证指数',
        'close': 2950.0,
        'change_pct': -5.0,  # 暴跌5%
        'volume': 500000000,
        'turnover': 600000000000,
        'advance_count': 200,
        'decline_count': 4500,
        'flat_count': 0
    }


# =============================================================================
# 策略信号Fixture
# =============================================================================

@pytest.fixture
def sample_buy_signal():
    """样本买入信号"""
    return {
        'code': '000001',
        'name': '平安银行',
        'signal_type': 'buy',
        'trigger_price': 15.0,
        'stoploss_price': 14.5,
        'take_profit_1': 16.5,
        'take_profit_2': 18.0,
        'confidence': 0.85,
        'strategy': 'limitup_callback',
        'reason': '涨停回调至20日均线，放量阳线',
        'timestamp': datetime.now()
    }


@pytest.fixture
def sample_sell_signal():
    """样本卖出信号"""
    return {
        'code': '000001',
        'name': '平安银行',
        'signal_type': 'sell',
        'trigger_price': 14.5,
        'reason': '跌破20日均线3%',
        'strategy': 'stoploss',
        'timestamp': datetime.now()
    }


# =============================================================================
# 股票列表Fixture
# =============================================================================

@pytest.fixture
def sample_stock_list():
    """样本股票列表"""
    return pd.DataFrame({
        'code': ['000001', '000002', '600000', '600001', '300001'],
        'name': ['平安银行', '万科A', '浦发银行', '邯郸钢铁', '特锐德'],
        'industry': ['银行', '房地产', '银行', '钢铁', '电气设备'],
        'market': ['主板', '主板', '主板', '主板', '创业板'],
        'list_date': ['1991-04-03', '1991-01-29', '1999-11-10', '1998-01-22', '2009-10-30']
    })


# =============================================================================
# 配置Fixture
# =============================================================================

@pytest.fixture
def sample_strategy_config():
    """样本策略配置"""
    return {
        'limitup_callback': {
            'max_limitup_days': 3,
            'max_turnover': 20,
            'min_roe': 0,
            'ema_tolerance': 0.02,
            'volume_surge_ratio': 1.5
        },
        'endstock_pick': {
            'time': '14:30',
            'price_change_min': 3,
            'price_change_max': 5,
            'volume_ratio_min': 1,
            'volume_ratio_max': 5,
            'market_cap_min': 50,
            'market_cap_max': 200
        },
        'dragon_head': {
            'min_consecutive_limitup': 3,
            'max_pullback_pct': 20,
            'entry_pullback_pct': 15
        }
    }


@pytest.fixture
def sample_risk_config():
    """样本风控配置"""
    return {
        'position': {
            'max_single_stock': 0.2,
            'max_sector': 0.3,
            'max_total': 0.8,
            'use_kelly': True,
            'kelly_fraction': 0.5
        },
        'stoploss': {
            'stoploss_type': 'ema20_down_3pct',
            'take_profit_1': {'gain': 10, 'action': 'reduce_half'},
            'take_profit_2': {'gain': 20, 'action': 'close_all'},
            'use_trailing': True,
            'trailing_pct': 0.10
        },
        'circuit_breaker': {
            'market_drop_2pct': {'action': 'pause_buy', 'duration': '1d'},
            'market_drop_5pct': {'action': 'close_all', 'duration': '1d'}
        }
    }


# =============================================================================
# 辅助函数
# =============================================================================

@pytest.fixture
def mock_tushare_response():
    """模拟Tushare API响应"""
    def _mock_response(data_type='kline'):
        if data_type == 'kline':
            return pd.DataFrame({
                'trade_date': ['20240101', '20240102', '20240103'],
                'open': [10.0, 10.5, 11.0],
                'high': [10.8, 11.0, 11.5],
                'low': [9.8, 10.2, 10.8],
                'close': [10.5, 10.8, 11.2],
                'vol': [10000, 12000, 15000],
                'amount': [105000, 129600, 168000]
            })
        elif data_type == 'daily':
            return pd.DataFrame({
                'ts_code': ['000001.SZ', '000002.SZ'],
                'trade_date': ['20240103', '20240103'],
                'close': [11.2, 15.5],
                'change': [0.4, 0.5],
                'pct_chg': [3.7, 3.3]
            })
        elif data_type == 'stock_list':
            return pd.DataFrame({
                'ts_code': ['000001.SZ', '000002.SZ'],
                'name': ['平安银行', '万科A'],
                'industry': ['银行', '全国地产'],
                'list_date': ['19910403', '19910129']
            })
        return pd.DataFrame()
    
    return _mock_response


@pytest.fixture
def temp_directory(tmp_path):
    """临时目录"""
    return tmp_path


# =============================================================================
# Pytest配置钩子
# =============================================================================

def pytest_configure(config):
    """Pytest配置钩子"""
    config.addinivalue_line("markers", "unit: 单元测试")
    config.addinivalue_line("markers", "integration: 集成测试")
    config.addinivalue_line("markers", "e2e: 端到端测试")
    config.addinivalue_line("markers", "slow: 慢速测试")
    config.addinivalue_line("markers", "data: 数据相关测试")


def pytest_collection_modifyitems(config, items):
    """修改测试项 - 添加标记"""
    for item in items:
        # 根据路径自动添加标记
        if "unit" in item.nodeid:
            item.add_marker(pytest.mark.unit)
        elif "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        elif "e2e" in item.nodeid:
            item.add_marker(pytest.mark.e2e)


# =============================================================================
# 清理Fixture
# =============================================================================

@pytest.fixture(autouse=True)
def cleanup():
    """自动清理fixture - 每个测试后执行"""
    yield
    # 清理代码在这里执行
    pass
