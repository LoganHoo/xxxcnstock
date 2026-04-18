# 量化交易系统 - TDD测试计划

**版本**: v1.0  
**日期**: 2026-04-19  
**关联文档**: 
- PRD: prd_quantitative_trading_system.md
- TASK_FLOW: TASK_FLOW_quantitative_trading.md  

---

## 1. TDD方法论

### 1.1 测试驱动开发流程

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  编写测试    │ → │  运行测试    │ → │  编写代码    │ → │  重构优化    │
│  (Red)      │    │  (Red)      │    │  (Green)    │    │  (Refactor) │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       ↑                                                    │
       └────────────────────────────────────────────────────┘
```

### 1.2 测试金字塔

```
         /\
        /  \
       / E2E \          端到端测试 (10%)
      /────────\
     /  集成测试  \       集成测试 (20%)
    /──────────────\
   /    单元测试      \    单元测试 (70%)
  /────────────────────\
```

### 1.3 测试原则

1. **FIRST原则**
   - **F**ast: 测试快速执行
   - **I**ndependent: 测试相互独立
   - **R**epeatable: 结果可重复
   - **S**elf-validating: 自我验证
   - **T**imely: 及时编写

2. **AAA模式**
   - **A**rrange: 准备测试数据
   - **A**ct: 执行被测操作
   - **A**ssert: 验证结果

---

## 2. 测试环境配置

### 2.1 测试框架

```python
# requirements-test.txt
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0
factory-boy>=3.3.0
freezegun>=1.2.0
responses>=0.23.0
```

### 2.2 测试配置

```python
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --cov=services
    --cov=core
    --cov-report=term-missing
    --cov-report=html:htmlcov
markers =
    unit: 单元测试
    integration: 集成测试
    e2e: 端到端测试
    slow: 慢速测试
    data: 数据相关测试
```

### 2.3 测试目录结构

```
tests/
├── conftest.py                 # 全局fixture
├── unit/                       # 单元测试
│   ├── __init__.py
│   ├── test_datasource.py      # 数据源测试
│   ├── test_analysis.py        # 分析引擎测试
│   ├── test_strategy.py        # 策略测试
│   ├── test_risk.py            # 风控测试
│   └── test_indicators.py      # 指标测试
├── integration/                # 集成测试
│   ├── __init__.py
│   ├── test_data_pipeline.py   # 数据流水线
│   ├── test_analysis_pipeline.py # 分析流水线
│   └── test_strategy_execution.py # 策略执行
├── e2e/                        # 端到端测试
│   ├── __init__.py
│   ├── test_limitup_callback.py # 涨停回调战法
│   ├── test_endstock_pick.py   # 尾盘选股
│   └── test_backtest.py        # 回测流程
├── fixtures/                   # 测试数据
│   ├── kline_data.json
│   ├── financial_data.json
│   └── macro_data.json
└── factories.py                # 数据工厂
```

---

## 3. 单元测试计划

### 3.1 数据源层测试

#### Test Suite: DataSourceManager

```python
# tests/unit/test_datasource.py

class TestDataSourceManager:
    """数据源管理器测试"""
    
    def test_initialization_with_primary_source(self):
        """测试使用主源初始化"""
        # Arrange
        config = {'primary': 'tushare', 'backup': 'akshare'}
        
        # Act
        manager = DataSourceManager(config)
        
        # Assert
        assert manager.current_source == 'tushare'
        assert manager.is_primary_active == True
    
    def test_failover_to_backup_when_primary_fails(self):
        """测试主源失效时切换到备源"""
        # Arrange
        manager = DataSourceManager()
        manager.initialize()
        
        # Act - 模拟主源失效
        with mock.patch.object(manager.primary_provider, 'fetch', side_effect=Exception('Timeout')):
            result = manager.fetch_kline('000001', '2024-01-01', '2024-01-31')
        
        # Assert
        assert manager.current_source == 'akshare'
        assert result is not None
    
    def test_recovery_to_primary_when_available(self):
        """测试主源恢复后切回"""
        # Arrange
        manager = DataSourceManager()
        manager.current_source = 'akshare'  # 当前使用备源
        
        # Act
        manager.check_primary_health()
        
        # Assert
        assert manager.current_source == 'tushare'
    
    def test_all_sources_failure_raises_exception(self):
        """测试所有数据源失效时抛出异常"""
        # Arrange
        manager = DataSourceManager()
        
        # Act & Assert
        with mock.patch.object(manager, '_fetch_from_all', side_effect=Exception('All failed')):
            with pytest.raises(DataSourceException):
                manager.fetch_kline('000001', '2024-01-01', '2024-01-31')
```

#### Test Suite: DataValidator

```python
class TestDataValidator:
    """数据验证器测试"""
    
    def test_validate_price_with_normal_data(self):
        """测试正常价格数据验证通过"""
        # Arrange
        df = pd.DataFrame({
            'open': [10.0, 10.5, 11.0],
            'high': [10.8, 11.0, 11.5],
            'low': [9.8, 10.2, 10.8],
            'close': [10.5, 10.8, 11.2]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_price(df)
        
        # Assert
        assert result.is_valid == True
        assert len(result.errors) == 0
    
    def test_validate_price_with_negative_value(self):
        """测试负价格数据验证失败"""
        # Arrange
        df = pd.DataFrame({
            'open': [10.0, -5.0, 11.0],  # 负数价格
            'high': [10.8, 11.0, 11.5],
            'low': [9.8, 10.2, 10.8],
            'close': [10.5, 10.8, 11.2]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_price(df)
        
        # Assert
        assert result.is_valid == False
        assert 'negative_price' in result.errors
    
    def test_validate_ohlc_logic(self):
        """测试OHLC逻辑验证"""
        # Arrange - high < low 的错误数据
        df = pd.DataFrame({
            'open': [10.0],
            'high': [9.0],   # high < low
            'low': [10.5],
            'close': [10.2]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_ohlc(df)
        
        # Assert
        assert result.is_valid == False
        assert 'ohlc_logic_error' in result.errors
    
    def test_validate_volume_with_zero(self):
        """测试成交量为零的停牌情况"""
        # Arrange
        df = pd.DataFrame({
            'volume': [10000, 0, 15000]  # 中间一天停牌
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_volume(df)
        
        # Assert
        assert result.is_valid == True  # 停牌是正常情况
        assert result.warnings == ['suspension_detected']
```

### 3.2 分析引擎测试

#### Test Suite: MacroTimingModel

```python
# tests/unit/test_analysis.py

class TestMacroTimingModel:
    """宏观择时模型测试"""
    
    def test_bullish_signal_when_liquidity_high(self):
        """测试流动性宽松时产生看涨信号"""
        # Arrange
        macro_data = {
            'shibor': pd.Series([2.5, 2.4, 2.3, 2.2]),  # 下降趋势
            'liquidity_score': 75  # 高流动性
        }
        model = MacroTimingModel()
        
        # Act
        signal = model.generate_signal(macro_data)
        
        # Assert
        assert signal == Signal.BULLISH
    
    def test_bearish_signal_when_liquidity_tight(self):
        """测试流动性紧张时产生看跌信号"""
        # Arrange
        macro_data = {
            'shibor': pd.Series([2.0, 2.3, 2.6, 3.0]),  # 上升趋势
            'liquidity_score': 25  # 低流动性
        }
        model = MacroTimingModel()
        
        # Act
        signal = model.generate_signal(macro_data)
        
        # Assert
        assert signal == Signal.BEARISH
    
    def test_neutral_signal_when_mixed_conditions(self):
        """测试混合条件下产生中性信号"""
        # Arrange
        macro_data = {
            'shibor': pd.Series([2.5, 2.5, 2.5, 2.5]),  # 平稳
            'liquidity_score': 50  # 中性
        }
        model = MacroTimingModel()
        
        # Act
        signal = model.generate_signal(macro_data)
        
        # Assert
        assert signal == Signal.NEUTRAL
```

#### Test Suite: FundamentalScreener

```python
class TestFundamentalScreener:
    """基本面筛选器测试"""
    
    def test_screen_with_roe_filter(self):
        """测试ROE筛选"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'roe': [15.0, 8.0, 12.0],  # 第二个不满足>10%
            'gross_margin': [25.0, 30.0, 22.0]
        })
        screener = FundamentalScreener()
        
        # Act
        result = screener.screen(stocks, {'roe_min': 10})
        
        # Assert
        assert len(result) == 2
        assert '000002' not in result['code'].values
    
    def test_screen_with_multiple_criteria(self):
        """测试多条件联合筛选"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'roe': [15.0, 8.0, 12.0, 20.0],
            'gross_margin': [25.0, 30.0, 15.0, 35.0],  # 第三个不满足>20%
            'profit_growth': [30.0, 25.0, 35.0, 15.0]  # 第四个不满足>20%
        })
        screener = FundamentalScreener()
        criteria = {
            'roe_min': 10,
            'gross_margin_min': 20,
            'profit_growth_min': 20
        }
        
        # Act
        result = screener.screen(stocks, criteria)
        
        # Assert
        assert len(result) == 2
        assert set(result['code'].values) == {'000001', '000003'}
```

#### Test Suite: TechnicalIndicators

```python
class TestTechnicalIndicators:
    """技术指标测试"""
    
    def test_ema_calculation(self):
        """测试EMA计算"""
        # Arrange
        prices = pd.Series([10, 11, 12, 11, 13, 14, 15])
        
        # Act
        ema = calculate_ema(prices, period=5)
        
        # Assert
        assert len(ema) == len(prices)
        assert not ema.isna().all()
        assert ema.iloc[-1] > ema.iloc[0]  # 上升趋势
    
    def test_macd_golden_cross_detection(self):
        """测试MACD金叉检测"""
        # Arrange - 模拟金叉数据
        prices = pd.Series([10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5])
        
        # Act
        macd_line, signal_line, histogram = calculate_macd(prices)
        is_golden_cross = detect_macd_cross(macd_line, signal_line)
        
        # Assert
        assert is_golden_cross == True
    
    def test_morning_star_pattern_detection(self):
        """测试早晨之星形态识别"""
        # Arrange - 早晨之星K线数据
        df = pd.DataFrame({
            'open': [15, 14.5, 13],   # 第一根阴线
            'high': [15.2, 14.8, 14], # 第二根十字星
            'low': [14.5, 13.5, 12.8],# 第三根阳线
            'close': [14.5, 13.5, 14] # 收盘价反转
        })
        
        # Act
        pattern = detect_candlestick_pattern(df)
        
        # Assert
        assert pattern == 'morning_star'
```

### 3.3 策略层测试

#### Test Suite: LimitupCallbackStrategy

```python
# tests/unit/test_strategy.py

class TestLimitupCallbackStrategy:
    """涨停回调策略测试"""
    
    def test_step1_filter_excludes_high_limitup_stocks(self):
        """测试Step1筛除三连板以上股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'limitup_days': [2, 4, 1],  # 000002三连板以上
            'turnover': [15, 18, 12],
            'roe': [15, 12, 10]
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step1_filter(stocks)
        
        # Assert
        assert len(result) == 2
        assert '000002' not in result['code'].values
    
    def test_step1_filter_excludes_high_turnover_stocks(self):
        """测试Step1筛除高换手率股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002'],
            'limitup_days': [1, 1],
            'turnover': [15, 25],  # 000002换手率>20%
            'roe': [15, 12]
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step1_filter(stocks)
        
        # Assert
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'
    
    def test_step2_confirm_macd_golden_cross(self):
        """测试Step2确认月线MACD金叉"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002'],
            'macd_monthly': ['golden_cross', 'death_cross'],
            'close': [15, 20],
            'ema_60_monthly': [14, 22]  # 000002股价<60月线
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step2_confirm(stocks)
        
        # Assert
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'
    
    def test_step3_timing_at_ema20_with_volume_surge(self):
        """测试Step3在20日均线且放量时触发"""
        # Arrange
        stock = {
            'code': '000001',
            'close': 15.0,
            'open': 14.5,
            'ema_20': 15.0,
            'volume': 150000,
            'volume_20_avg': 100000
        }
        strategy = LimitupCallbackStrategy()
        
        # Act
        signal = strategy.step3_timing([stock])
        
        # Assert
        assert len(signal) == 1
        assert signal[0].code == '000001'
        assert signal[0].trigger_price == 15.0
```

### 3.4 风控层测试

#### Test Suite: KellyCalculator

```python
# tests/unit/test_risk.py

class TestKellyCalculator:
    """凯利公式计算器测试"""
    
    def test_calculate_with_high_win_rate(self):
        """测试高胜率下的仓位计算"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.6
        win_loss_ratio = 2.0
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        assert position > 0
        assert position <= 0.2  # 不超过20%
    
    def test_calculate_with_low_win_rate(self):
        """测试低胜率下的仓位计算"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.4
        win_loss_ratio = 1.5
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        assert position < 0.1  # 低胜率应降低仓位
    
    def test_calculate_limits_max_position(self):
        """测试仓位上限限制"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.8
        win_loss_ratio = 3.0
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        assert position <= 0.2  # 单票最多20%


class TestStopLossManager:
    """止盈止损管理器测试"""
    
    def test_stoploss_triggered_at_ema20_down_3pct(self):
        """测试20日均线下3%触发止损"""
        # Arrange
        position = {
            'code': '000001',
            'cost_price': 15.0,
            'current_price': 13.5,  # 下跌10%
            'ema_20': 14.0          # 20日均线
        }
        manager = StopLossManager()
        
        # Act
        should_stop = manager.check_stoploss(position)
        
        # Assert
        assert should_stop == True
        assert manager.stoploss_price == 14.0 * 0.97  # 20日均线下3%
    
    def test_take_profit_at_10_percent(self):
        """测试盈利10%减仓一半"""
        # Arrange
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 11.0,  # 盈利10%
            'quantity': 1000
        }
        manager = StopLossManager()
        
        # Act
        action = manager.check_take_profit(position)
        
        # Assert
        assert action['type'] == 'reduce_half'
        assert action['quantity'] == 500
    
    def test_take_profit_at_20_percent(self):
        """测试盈利20%清仓"""
        # Arrange
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 12.0,  # 盈利20%
            'quantity': 1000
        }
        manager = StopLossManager()
        
        # Act
        action = manager.check_take_profit(position)
        
        # Assert
        assert action['type'] == 'close_all'
        assert action['quantity'] == 1000


class TestCircuitBreaker:
    """熔断机制测试"""
    
    def test_pause_buy_when_market_drop_2pct(self):
        """测试大盘跌超2%暂停买入"""
        # Arrange
        market_data = {
            'index_change_pct': -2.5  # 下跌2.5%
        }
        breaker = CircuitBreaker()
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == True
        assert action['rule'] == 'market_drop_2pct'
        assert action['action'] == 'pause_buy'
    
    def test_reduce_50pct_when_macd_death_cross(self):
        """测试MACD死叉减仓50%"""
        # Arrange
        market_data = {
            'macd_signal': 'death_cross'
        }
        breaker = CircuitBreaker()
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == True
        assert action['rule'] == 'macd_death_cross'
        assert action['action'] == 'reduce_50pct'
```

---

## 4. 集成测试计划

### 4.1 数据流水线集成测试

```python
# tests/integration/test_data_pipeline.py

class TestDataPipeline:
    """数据流水线集成测试"""
    
    @pytest.mark.integration
    def test_full_data_collection_pipeline(self):
        """测试完整数据采集流水线"""
        # Arrange
        pipeline = DataCollectionPipeline()
        
        # Act
        result = pipeline.run(
            codes=['000001'],
            start_date='2024-01-01',
            end_date='2024-01-31'
        )
        
        # Assert
        assert result.success == True
        assert result.records_count > 0
        assert result.validation_passed == True
    
    @pytest.mark.integration
    def test_data_pipeline_with_failover(self):
        """测试带故障转移的数据流水线"""
        # Arrange
        pipeline = DataCollectionPipeline()
        
        # Act - 模拟主源故障
        with mock.patch('datasource.tushare.fetch', side_effect=Exception('Timeout')):
            result = pipeline.run(codes=['000001'])
        
        # Assert
        assert result.success == True
        assert result.source_used == 'akshare'
```

### 4.2 策略信号集成测试

```python
# tests/integration/test_strategy_signal.py

class TestStrategySignalIntegration:
    """策略信号集成测试"""
    
    @pytest.mark.integration
    def test_limitup_callback_full_flow(self):
        """测试涨停回调策略完整流程"""
        # Arrange
        strategy = LimitupCallbackStrategy()
        market_data = load_test_data('limitup_callback_scenario')
        
        # Act
        signals = strategy.execute(market_data)
        
        # Assert
        assert len(signals) > 0
        for signal in signals:
            assert signal.code is not None
            assert signal.trigger_price > 0
            assert signal.confidence > 0.5
```

---

## 5. 端到端测试计划

### 5.1 涨停回调战法E2E测试

```python
# tests/e2e/test_limitup_callback.py

class TestLimitupCallbackE2E:
    """涨停回调战法端到端测试"""
    
    @pytest.mark.e2e
    def test_complete_limitup_callback_workflow(self):
        """测试涨停回调战法完整工作流"""
        # Arrange - 准备测试数据
        test_date = '2024-03-15'
        
        # Act
        # 1. 数据采集
        data_service.collect_data(date=test_date)
        
        # 2. 分析计算
        analysis_service.run_analysis(date=test_date)
        
        # 3. 策略执行
        signals = strategy_service.execute_limitup_callback(date=test_date)
        
        # 4. 风控检查
        risk_service.validate_signals(signals)
        
        # Assert
        assert len(signals) >= 0  # 可能有信号也可能没有
        for signal in signals:
            assert self.validate_signal_quality(signal)
    
    def validate_signal_quality(self, signal):
        """验证信号质量"""
        return (
            signal.code is not None and
            signal.trigger_price > 0 and
            signal.stoploss_price < signal.trigger_price and
            signal.take_profit_price > signal.trigger_price
        )
```

### 5.2 回测流程E2E测试

```python
# tests/e2e/test_backtest.py

class TestBacktestE2E:
    """回测流程端到端测试"""
    
    @pytest.mark.e2e
    def test_strategy_backtest_with_report(self):
        """测试策略回测并生成报告"""
        # Arrange
        strategy = LimitupCallbackStrategy()
        period = {'start': '2023-01-01', 'end': '2023-12-31'}
        
        # Act
        result = backtest_service.run(
            strategy=strategy,
            period=period,
            initial_capital=1000000
        )
        
        # Assert
        assert result is not None
        assert 'total_return' in result
        assert 'sharpe_ratio' in result
        assert 'max_drawdown' in result
        assert result['total_trades'] > 0
```

---

## 6. 测试数据管理

### 6.1 Fixture定义

```python
# tests/conftest.py

import pytest
import pandas as pd
from datetime import datetime, timedelta

@pytest.fixture
def sample_kline_data():
    """样本K线数据"""
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    return pd.DataFrame({
        'date': dates,
        'open': [10 + i * 0.1 for i in range(30)],
        'high': [10.5 + i * 0.1 for i in range(30)],
        'low': [9.5 + i * 0.1 for i in range(30)],
        'close': [10.2 + i * 0.1 for i in range(30)],
        'volume': [10000 + i * 100 for i in range(30)]
    })

@pytest.fixture
def sample_financial_data():
    """样本财务数据"""
    return {
        'code': '000001',
        'roe': 15.5,
        'gross_margin': 25.0,
        'profit_growth': 30.0,
        'pe': 15.0,
        'pb': 2.0
    }

@pytest.fixture
def mock_bullish_market():
    """模拟看涨市场环境"""
    return {
        'shibor': pd.Series([2.5, 2.4, 2.3, 2.2]),
        'liquidity_score': 75,
        'macd_signal': 'golden_cross'
    }

@pytest.fixture
def mock_bearish_market():
    """模拟看跌市场环境"""
    return {
        'shibor': pd.Series([2.0, 2.3, 2.6, 3.0]),
        'liquidity_score': 25,
        'macd_signal': 'death_cross'
    }
```

### 6.2 数据工厂

```python
# tests/factories.py

import factory
import pandas as pd
from datetime import datetime

class KlineDataFactory(factory.Factory):
    """K线数据工厂"""
    class Meta:
        model = dict
    
    code = factory.Sequence(lambda n: f'{n:06d}')
    date = factory.LazyFunction(lambda: datetime.now().strftime('%Y-%m-%d'))
    open = factory.Faker('pyfloat', min_value=10, max_value=100)
    high = factory.LazyAttribute(lambda o: o.open * 1.05)
    low = factory.LazyAttribute(lambda o: o.open * 0.95)
    close = factory.LazyAttribute(lambda o: (o.open + o.high + o.low) / 3)
    volume = factory.Faker('random_int', min=10000, max=1000000)

class FinancialDataFactory(factory.Factory):
    """财务数据工厂"""
    class Meta:
        model = dict
    
    code = factory.Sequence(lambda n: f'{n:06d}')
    roe = factory.Faker('pyfloat', min_value=5, max_value=30)
    gross_margin = factory.Faker('pyfloat', min_value=10, max_value=50)
    profit_growth = factory.Faker('pyfloat', min_value=-20, max_value=100)
    pe = factory.Faker('pyfloat', min_value=5, max_value=100)
    pb = factory.Faker('pyfloat', min_value=0.5, max_value=10)
```

---

## 7. 覆盖率目标

### 7.1 覆盖率要求

| 模块 | 行覆盖率 | 分支覆盖率 |
|------|---------|-----------|
| services/data_service | 90% | 85% |
| services/analysis_service | 85% | 80% |
| services/strategy_service | 85% | 80% |
| services/risk_service | 90% | 85% |
| services/backtest_service | 80% | 75% |
| core/indicators | 90% | 85% |

### 7.2 覆盖率检查

```bash
# 运行测试并生成覆盖率报告
pytest --cov=services --cov=core --cov-report=html --cov-report=term-missing

# 检查覆盖率是否达标
pytest --cov-fail-under=85
```

---

## 8. CI/CD集成

### 8.1 GitHub Actions配置

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      mysql:
        image: mysql:5.7
        env:
          MYSQL_ROOT_PASSWORD: test
      redis:
        image: redis:6-alpine
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    
    - name: Run unit tests
      run: pytest tests/unit -v --cov --cov-report=xml
    
    - name: Run integration tests
      run: pytest tests/integration -v
      env:
        TEST_ENV: integration
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

---

## 9. 测试执行计划

### 9.1 开发阶段

| 阶段 | 测试类型 | 执行频率 |
|------|---------|---------|
| 编码时 | 单元测试 | 每次保存 |
| 提交前 | 单元+集成 | 每次提交 |
| PR时 | 全量测试 | 每次PR |

### 9.2 测试命令

```bash
# 运行所有单元测试
pytest tests/unit -v

# 运行特定模块测试
pytest tests/unit/test_strategy.py -v

# 运行集成测试
pytest tests/integration -v -m integration

# 运行端到端测试
pytest tests/e2e -v -m e2e

# 运行并生成覆盖率报告
pytest --cov=services --cov-report=html

# 只运行快速测试
pytest -m "not slow"
```

---

## 10. 附录

### 10.1 测试命名规范

- 测试文件: `test_<module>.py`
- 测试类: `Test<Feature>`
- 测试方法: `test_<scenario>_<expected_result>`

### 10.2 Mock规范

```python
# 推荐做法
with mock.patch('module.Class.method') as mock_method:
    mock_method.return_value = expected_value
    # 测试代码

# 不推荐做法
module.Class.method = mock.MagicMock()  # 会污染全局状态
```

### 10.3 断言规范

```python
# 推荐做法
assert result.is_valid == True
assert len(items) == expected_count

# 不推荐做法
assert result  # 不明确
assert len(items)  # 不明确
```

---

**文档维护记录**

| 版本 | 日期 | 修改人 | 修改内容 |
|------|------|-------|---------|
| v1.0 | 2026-04-19 | AI Assistant | 初始版本 |
