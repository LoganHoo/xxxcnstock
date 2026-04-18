# 量化交易系统 - 详细实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建一个基于数据驱动、严格风控的量化交易系统，实现从数据采集到策略执行的完整闭环

**Architecture:** 采用分层微服务架构，数据服务层负责多源数据采集与存储，分析服务层提供宏观/基本面/技术面/情绪面四维分析，策略服务层实现涨停回调/尾盘选股/龙回头三大战法，风控服务层保障资金安全，回测服务层支持策略验证与优化

**Tech Stack:** Python 3.11+, FastAPI, SQLAlchemy, Pandas, NumPy, Backtrader, Redis, MySQL, Docker

---

## Phase 1: 基础设施 (Week 1-2)

### Task 1.1: 创建项目基础结构

**Files:**
- Create: `core/__init__.py`
- Create: `core/models/__init__.py`
- Create: `services/__init__.py`
- Create: `services/data_service/__init__.py`
- Create: `services/data_service/datasource/__init__.py`

**Step 1: 创建目录结构**

```bash
mkdir -p core/{models,indicators,utils}
mkdir -p services/{data_service/{datasource,processors,storage,quality},analysis_service/{macro,fundamental,technical,sentiment},strategy_service/{limitup_callback,endstock_pick,dragon_head},risk_service/{position,stoploss,circuit_breaker},backtest_service/engine}
mkdir -p tests/{unit,integration,e2e,fixtures}
touch core/__init__.py core/models/__init__.py core/indicators/__init__.py core/utils/__init__.py
touch services/__init__.py services/data_service/__init__.py services/data_service/datasource/__init__.py
```

**Step 2: 验证目录结构**

Run: `find services core tests -type f -name "__init__.py" | head -20`
Expected: 显示所有 __init__.py 文件路径

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: initialize project structure for quantitative trading system"
```

---

### Task 1.2: 实现数据源抽象基类

**Files:**
- Create: `services/data_service/datasource/base.py`
- Test: `tests/unit/test_datasource.py` (已存在，添加测试)

**Step 1: 编写抽象基类测试**

在 `tests/unit/test_datasource.py` 中添加:

```python
def test_base_provider_interface():
    """测试数据源提供者接口定义"""
    from services.data_service.datasource.base import DataSourceProvider
    
    # 抽象类不能直接实例化
    with pytest.raises(TypeError):
        provider = DataSourceProvider()
```

**Step 2: 运行测试验证失败**

Run: `pytest tests/unit/test_datasource.py::test_base_provider_interface -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'services.data_service.datasource.base'"

**Step 3: 实现抽象基类**

Create `services/data_service/datasource/base.py`:

```python
#!/usr/bin/env python3
"""
数据源提供者抽象基类

定义所有数据源提供者必须实现的接口
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
import pandas as pd
from datetime import datetime


class DataSourceProvider(ABC):
    """数据源提供者抽象基类"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self._is_connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """连接数据源"""
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """断开数据源连接"""
        pass
    
    @abstractmethod
    def fetch_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = 'd'
    ) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 频率 (d=日线, w=周线, m=月线)
        
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        pass
    
    @abstractmethod
    def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """健康检查"""
        pass
    
    @property
    def is_connected(self) -> bool:
        return self._is_connected
```

**Step 4: 运行测试验证通过**

Run: `pytest tests/unit/test_datasource.py::test_base_provider_interface -v`
Expected: PASS

**Step 5: Commit**

```bash
git add services/data_service/datasource/base.py
git commit -m "feat: add datasource provider abstract base class"
```

---

### Task 1.3: 实现 Tushare 数据提供者

**Files:**
- Create: `services/data_service/datasource/tushare_provider.py`
- Modify: `tests/unit/test_datasource.py` (添加 TestTushareProvider 类)

**Step 1: 编写 Tushare 提供者测试**

在 `tests/unit/test_datasource.py` 中添加:

```python
class TestTushareProvider:
    """Tushare数据源提供者测试"""
    
    def test_fetch_kline_with_valid_code(self):
        """测试获取有效股票K线 - 应返回DataFrame"""
        from services.data_service.datasource.tushare_provider import TushareProvider
        
        provider = TushareProvider(token='test_token')
        
        with mock.patch.object(provider, 'pro') as mock_pro:
            mock_pro.daily.return_value = pd.DataFrame({
                'trade_date': ['20240101', '20240102'],
                'open': [10.0, 10.5],
                'high': [10.8, 11.0],
                'low': [9.8, 10.2],
                'close': [10.5, 10.8],
                'vol': [10000, 12000]
            })
            
            result = provider.fetch_kline('000001.SZ', '2024-01-01', '2024-01-02')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert 'open' in result.columns
            assert 'close' in result.columns
```

**Step 2: 运行测试验证失败**

Run: `pytest tests/unit/test_datasource.py::TestTushareProvider::test_fetch_kline_with_valid_code -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: 实现 Tushare 提供者**

Create `services/data_service/datasource/tushare_provider.py`:

```python
#!/usr/bin/env python3
"""
Tushare 数据提供者实现

使用 Tushare Pro API 获取股票数据
"""
import os
import logging
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime

from .base import DataSourceProvider

logger = logging.getLogger(__name__)


class TushareProvider(DataSourceProvider):
    """Tushare Pro 数据提供者"""
    
    def __init__(self, token: Optional[str] = None, config: Dict[str, Any] = None):
        super().__init__('tushare', config)
        self.token = token or os.getenv('TUSHARE_TOKEN')
        self.pro = None
        
        if not self.token:
            raise ValueError("Tushare token is required. Set TUSHARE_TOKEN env var or pass token parameter.")
    
    def connect(self) -> bool:
        """连接 Tushare"""
        try:
            import tushare as ts
            self.pro = ts.pro_api(self.token)
            self._is_connected = True
            logger.info("Tushare connected successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect Tushare: {e}")
            return False
    
    def disconnect(self) -> bool:
        """断开连接"""
        self.pro = None
        self._is_connected = False
        return True
    
    def fetch_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = 'd'
    ) -> pd.DataFrame:
        """获取K线数据"""
        if not self.is_connected:
            self.connect()
        
        try:
            # 转换日期格式
            start = start_date.replace('-', '')
            end = end_date.replace('-', '')
            
            # 调用 Tushare API
            df = self.pro.daily(
                ts_code=code,
                start_date=start,
                end_date=end
            )
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 标准化列名
            df = df.rename(columns={
                'trade_date': 'date',
                'vol': 'volume',
                'amount': 'turnover'
            })
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df = df.sort_values('date')
            
            return df[['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
            
        except Exception as e:
            logger.error(f"Failed to fetch kline for {code}: {e}")
            return pd.DataFrame()
    
    def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self.is_connected:
            self.connect()
        
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            return df
        except Exception as e:
            logger.error(f"Failed to fetch stock list: {e}")
            return pd.DataFrame()
    
    def health_check(self) -> bool:
        """健康检查"""
        if not self.is_connected:
            return self.connect()
        
        try:
            # 尝试获取交易日历
            df = self.pro.trade_cal(start_date='20240101', end_date='20240101')
            return df is not None and not df.empty
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
```

**Step 4: 运行测试验证通过**

Run: `pytest tests/unit/test_datasource.py::TestTushareProvider -v`
Expected: PASS

**Step 5: Commit**

```bash
git add services/data_service/datasource/tushare_provider.py
git commit -m "feat: add Tushare data provider implementation"
```

---

### Task 1.4: 实现数据源管理器

**Files:**
- Create: `services/data_service/datasource/manager.py`
- Modify: `tests/unit/test_datasource.py` (添加 TestDataSourceManager 类)

**Step 1: 编写管理器测试**

测试已在 `tests/unit/test_datasource.py` 中存在 (TestDataSourceManager 类)

**Step 2: 运行测试验证失败**

Run: `pytest tests/unit/test_datasource.py::TestDataSourceManager -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: 实现数据源管理器**

Create `services/data_service/datasource/manager.py`:

```python
#!/usr/bin/env python3
"""
数据源管理器

管理多个数据源，实现自动故障转移
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

from .base import DataSourceProvider
from .tushare_provider import TushareProvider

logger = logging.getLogger(__name__)


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.primary_provider: Optional[DataSourceProvider] = None
        self.backup_providers: List[DataSourceProvider] = []
        self.current_source: Optional[str] = None
        self._health_status: Dict[str, Dict] = {}
        
        self._initialize_providers()
    
    def _initialize_providers(self):
        """初始化数据源提供者"""
        # 主源: Tushare
        try:
            self.primary_provider = TushareProvider(
                token=self.config.get('tushare_token')
            )
            self.current_source = 'tushare'
            logger.info("Primary provider (Tushare) initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Tushare: {e}")
        
        # 备源将在后续任务中实现
    
    @property
    def is_primary_active(self) -> bool:
        """检查主源是否活跃"""
        return self.current_source == 'tushare' and self.primary_provider is not None
    
    def fetch_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = 'd'
    ) -> pd.DataFrame:
        """
        获取K线数据，自动故障转移
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率
        
        Returns:
            K线数据DataFrame
        """
        # 尝试主源
        if self.primary_provider:
            try:
                df = self.primary_provider.fetch_kline(code, start_date, end_date, frequency)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"Primary source failed: {e}")
        
        # 主源失败，尝试备源
        for provider in self.backup_providers:
            try:
                df = provider.fetch_kline(code, start_date, end_date, frequency)
                if not df.empty:
                    self.current_source = provider.name
                    logger.info(f"Switched to backup source: {provider.name}")
                    return df
            except Exception as e:
                logger.warning(f"Backup source {provider.name} failed: {e}")
        
        raise Exception("All data sources failed")
    
    def check_primary_health(self) -> bool:
        """检查主源健康状态"""
        if not self.primary_provider:
            return False
        
        is_healthy = self.primary_provider.health_check()
        
        if is_healthy and self.current_source != 'tushare':
            # 主源恢复，切回主源
            self.current_source = 'tushare'
            logger.info("Primary source recovered, switched back")
        
        self._health_status['tushare'] = {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'last_check': datetime.now()
        }
        
        return is_healthy
    
    def get_health_status(self) -> Dict[str, Dict]:
        """获取所有数据源健康状态"""
        return self._health_status.copy()
    
    def simulate_primary_failure(self):
        """模拟主源失效 (用于测试)"""
        self.current_source = 'akshare'
    
    def simulate_primary_recovery(self):
        """模拟主源恢复 (用于测试)"""
        if self.primary_provider and self.primary_provider.health_check():
            self.current_source = 'tushare'
```

**Step 4: 运行测试验证通过**

Run: `pytest tests/unit/test_datasource.py::TestDataSourceManager -v`
Expected: PASS

**Step 5: Commit**

```bash
git add services/data_service/datasource/manager.py
git commit -m "feat: add datasource manager with failover support"
```

---

### Task 1.5: 实现数据验证器

**Files:**
- Create: `services/data_service/quality/validator.py`
- Test: `tests/unit/test_datasource.py` (TestDataValidator 类已存在)

**Step 1: 运行测试验证失败**

Run: `pytest tests/unit/test_datasource.py::TestDataValidator -v`
Expected: FAIL

**Step 2: 实现数据验证器**

Create `services/data_service/quality/validator.py`:

```python
#!/usr/bin/env python3
"""
数据质量验证器

验证股票数据的完整性和合理性
"""
import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    checks_passed: List[str]


class DataValidator:
    """数据验证器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.max_price = self.config.get('max_price', 5000)
        self.max_volume = self.config.get('max_volume', 5e9)
        self.max_turnover = self.config.get('max_turnover', 1e11)
    
    def validate_all(self, df: pd.DataFrame) -> ValidationResult:
        """执行所有验证"""
        errors = []
        warnings = []
        checks_passed = []
        
        # 价格验证
        price_result = self.validate_price(df)
        if price_result.is_valid:
            checks_passed.append('price')
        else:
            errors.extend(price_result.errors)
        warnings.extend(price_result.warnings)
        
        # OHLC逻辑验证
        ohlc_result = self.validate_ohlc(df)
        if ohlc_result.is_valid:
            checks_passed.append('ohlc')
        else:
            errors.extend(ohlc_result.errors)
        
        # 成交量验证
        volume_result = self.validate_volume(df)
        if volume_result.is_valid:
            checks_passed.append('volume')
        else:
            errors.extend(volume_result.errors)
        warnings.extend(volume_result.warnings)
        
        # 连续性验证
        if 'date' in df.columns:
            continuity_result = self.validate_continuity(df)
            if continuity_result.is_valid:
                checks_passed.append('continuity')
            warnings.extend(continuity_result.warnings)
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checks_passed=checks_passed
        )
    
    def validate_price(self, df: pd.DataFrame) -> ValidationResult:
        """验证价格数据"""
        errors = []
        warnings = []
        
        required_cols = ['open', 'high', 'low', 'close']
        for col in required_cols:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")
                continue
            
            # 检查负数价格
            if (df[col] < 0).any():
                errors.append(f"negative_price_in_{col}")
            
            # 检查极端价格
            if (df[col] > self.max_price).any():
                warnings.append(f"extreme_price_in_{col}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checks_passed=[]
        )
    
    def validate_ohlc(self, df: pd.DataFrame) -> ValidationResult:
        """验证OHLC逻辑"""
        errors = []
        
        if not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            return ValidationResult(False, ["Missing OHLC columns"], [], [])
        
        # high应>=open, close, low
        if (df['high'] < df[['open', 'close', 'low']].max(axis=1)).any():
            errors.append("ohlc_logic_error: high < max(open, close, low)")
        
        # low应<=open, close, high
        if (df['low'] > df[['open', 'close', 'high']].min(axis=1)).any():
            errors.append("ohlc_logic_error: low > min(open, close, high)")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=[],
            checks_passed=[]
        )
    
    def validate_volume(self, df: pd.DataFrame) -> ValidationResult:
        """验证成交量数据"""
        errors = []
        warnings = []
        
        if 'volume' not in df.columns:
            return ValidationResult(False, ["Missing volume column"], [], [])
        
        # 检查负数成交量
        if (df['volume'] < 0).any():
            errors.append("negative_volume")
        
        # 检查停牌 (成交量为0)
        if (df['volume'] == 0).any():
            warnings.append("suspension_detected")
        
        # 检查极端成交量
        if (df['volume'] > self.max_volume).any():
            warnings.append("extreme_volume")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checks_passed=[]
        )
    
    def validate_continuity(self, df: pd.DataFrame, expected_freq: str = 'D') -> ValidationResult:
        """验证数据连续性"""
        warnings = []
        
        if 'date' not in df.columns or df.empty:
            return ValidationResult(True, [], [], [])
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # 检查日期间隔
        date_diff = df['date'].diff().dropna()
        
        if expected_freq == 'D':
            # 日线数据，检查是否有超过5天的间隔 (排除周末)
            long_gaps = date_diff[date_diff > pd.Timedelta(days=5)]
            if not long_gaps.empty:
                warnings.append(f"data_gap_detected: {len(long_gaps)} gaps > 5 days")
        
        return ValidationResult(
            is_valid=True,
            errors=[],
            warnings=warnings,
            checks_passed=[]
        )
```

**Step 3: 运行测试验证通过**

Run: `pytest tests/unit/test_datasource.py::TestDataValidator -v`
Expected: PASS

**Step 4: Commit**

```bash
git add services/data_service/quality/validator.py
git commit -m "feat: add data quality validator"
```

---

## Phase 2: 分析引擎 (Week 3-4)

### Task 2.1: 实现技术指标基类

**Files:**
- Create: `core/indicators/base.py`
- Create: `core/indicators/trend.py`
- Test: `tests/unit/test_indicators.py` (新建)

**Step 1: 编写测试**

Create `tests/unit/test_indicators.py`:

```python
def test_ema_calculation():
    """测试EMA计算"""
    from core.indicators.trend import calculate_ema
    
    prices = pd.Series([10, 11, 12, 11, 13, 14, 15])
    ema = calculate_ema(prices, period=5)
    
    assert len(ema) == len(prices)
    assert not ema.isna().all()
```

**Step 2: 实现 EMA 指标**

Create `core/indicators/trend.py`:

```python
#!/usr/bin/env python3
"""
趋势指标

包含EMA、MACD等趋势跟踪指标
"""
import pandas as pd
import numpy as np


def calculate_ema(prices: pd.Series, period: int = 20) -> pd.Series:
    """
    计算指数移动平均线
    
    Args:
        prices: 价格序列
        period: 周期
    
    Returns:
        EMA序列
    """
    return prices.ewm(span=period, adjust=False).mean()


def calculate_macd(
    prices: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> tuple:
    """
    计算MACD指标
    
    Args:
        prices: 价格序列
        fast: 快线周期
        slow: 慢线周期
        signal: 信号线周期
    
    Returns:
        (macd_line, signal_line, histogram)
    """
    ema_fast = calculate_ema(prices, fast)
    ema_slow = calculate_ema(prices, slow)
    
    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line, signal)
    histogram = macd_line - signal_line
    
    return macd_line, signal_line, histogram


def detect_macd_cross(
    macd_line: pd.Series,
    signal_line: pd.Series,
    cross_type: str = 'golden'
) -> bool:
    """
    检测MACD交叉
    
    Args:
        macd_line: MACD线
        signal_line: 信号线
        cross_type: 'golden' (金叉) 或 'death' (死叉)
    
    Returns:
        是否发生交叉
    """
    if len(macd_line) < 2 or len(signal_line) < 2:
        return False
    
    # 前一天
    prev_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
    # 今天
    curr_diff = macd_line.iloc[-1] - signal_line.iloc[-1]
    
    if cross_type == 'golden':
        # 金叉: 前一天MACD<信号线，今天MACD>信号线
        return prev_diff < 0 and curr_diff > 0
    elif cross_type == 'death':
        # 死叉: 前一天MACD>信号线，今天MACD<信号线
        return prev_diff > 0 and curr_diff < 0
    
    return False
```

**Step 3: 运行测试**

Run: `pytest tests/unit/test_indicators.py::test_ema_calculation -v`
Expected: PASS

**Step 4: Commit**

```bash
git add core/indicators/trend.py tests/unit/test_indicators.py
git commit -m "feat: add EMA and MACD trend indicators"
```

---

### Task 2.2: 实现涨停回调策略核心

**Files:**
- Create: `services/strategy_service/limitup_callback/strategy.py`
- Create: `services/strategy_service/limitup_callback/signals.py`
- Test: `tests/unit/test_strategy.py` (TestLimitupCallbackStrategy 已存在)

**Step 1: 运行测试验证失败**

Run: `pytest tests/unit/test_strategy.py::TestLimitupCallbackStrategy::test_step1_filter_excludes_high_limitup_stocks -v`
Expected: FAIL

**Step 2: 实现策略核心**

Create `services/strategy_service/limitup_callback/strategy.py`:

```python
#!/usr/bin/env python3
"""
涨停回调战法策略

核心逻辑:
1. 筛除三连板以上、换手率>20%、业绩亏损股
2. 确认月线MACD金叉且股价>60月线
3. 回调至20日均线且放量阳线时买入
"""
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd

from .signals import BuySignal

logger = logging.getLogger(__name__)


@dataclass
class StrategyConfig:
    """策略配置"""
    max_limitup_days: int = 3
    max_turnover: float = 20.0
    min_roe: float = 0.0
    ema_tolerance: float = 0.02
    volume_surge_ratio: float = 1.5


class LimitupCallbackStrategy:
    """涨停回调战法策略"""
    
    def __init__(self, config: Optional[StrategyConfig] = None):
        self.config = config or StrategyConfig()
    
    def execute(self, stock_data: pd.DataFrame) -> List[BuySignal]:
        """
        执行策略
        
        Args:
            stock_data: 股票数据DataFrame
        
        Returns:
            买入信号列表
        """
        # Step 1: 初步筛选
        filtered = self.step1_filter(stock_data)
        logger.info(f"Step 1 filtered: {len(filtered)} stocks")
        
        if filtered.empty:
            return []
        
        # Step 2: 趋势确认
        confirmed = self.step2_confirm(filtered)
        logger.info(f"Step 2 confirmed: {len(confirmed)} stocks")
        
        if confirmed.empty:
            return []
        
        # Step 3: 买入时机
        signals = self.step3_timing(confirmed.to_dict('records'))
        logger.info(f"Step 3 signals: {len(signals)} stocks")
        
        return signals
    
    def step1_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        初步筛选
        
        筛除:
        - 三连板以上
        - 换手率>20%
        - 业绩亏损股 (ROE <= 0)
        """
        filtered = data.copy()
        
        # 筛除三连板以上
        if 'limitup_days' in filtered.columns:
            filtered = filtered[filtered['limitup_days'] <= self.config.max_limitup_days]
        
        # 筛除高换手率
        if 'turnover' in filtered.columns:
            filtered = filtered[filtered['turnover'] <= self.config.max_turnover]
        
        # 筛除亏损股
        if 'roe' in filtered.columns:
            filtered = filtered[filtered['roe'] > self.config.min_roe]
        
        return filtered
    
    def step2_confirm(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        趋势确认
        
        确认:
        - 月线MACD金叉
        - 股价>60月线
        """
        confirmed = data.copy()
        
        # 确认月线MACD金叉
        if 'macd_monthly' in confirmed.columns:
            confirmed = confirmed[confirmed['macd_monthly'] == 'golden_cross']
        
        # 确认股价>60月线
        if 'close' in confirmed.columns and 'ema_60_monthly' in confirmed.columns:
            confirmed = confirmed[confirmed['close'] > confirmed['ema_60_monthly']]
        
        return confirmed
    
    def step3_timing(self, stocks: List[Dict]) -> List[BuySignal]:
        """
        买入时机判断
        
        条件:
        - 回调至20日均线附近 (±2%)
        - 放量阳线 (成交量>20日均量*1.5)
        """
        signals = []
        
        for stock in stocks:
            close = stock.get('close', 0)
            open_price = stock.get('open', 0)
            ema_20 = stock.get('ema_20', 0)
            volume = stock.get('volume', 0)
            volume_20_avg = stock.get('volume_20_avg', 0)
            
            # 检查是否在20日均线附近
            if ema_20 <= 0:
                continue
            
            ema_upper = ema_20 * (1 + self.config.ema_tolerance)
            ema_lower = ema_20 * (1 - self.config.ema_tolerance)
            
            near_ema = ema_lower <= close <= ema_upper
            
            # 检查是否放量
            volume_surge = volume > volume_20_avg * self.config.volume_surge_ratio
            
            # 检查是否阳线
            is_red = close > open_price
            
            if near_ema and volume_surge and is_red:
                signal = BuySignal(
                    code=stock.get('code'),
                    name=stock.get('name'),
                    trigger_price=close,
                    stoploss_price=self.calculate_stoploss(stock),
                    take_profit_1=close * 1.10,
                    take_profit_2=close * 1.20,
                    confidence=0.8,
                    reason='涨停回调至20日均线，放量阳线'
                )
                signals.append(signal)
        
        return signals
    
    def calculate_stoploss(self, stock: Dict) -> float:
        """计算止损价 (20日均线下3%)"""
        ema_20 = stock.get('ema_20', 0)
        return ema_20 * 0.97 if ema_20 > 0 else 0
    
    def calculate_take_profit(self, entry_price: float) -> List[Dict]:
        """计算止盈价"""
        return [
            {'level': 1, 'price': entry_price * 1.10, 'action': 'reduce_half'},
            {'level': 2, 'price': entry_price * 1.20, 'action': 'close_all'}
        ]
```

Create `services/strategy_service/limitup_callback/signals.py`:

```python
#!/usr/bin/env python3
"""
策略信号定义
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class BuySignal:
    """买入信号"""
    code: str
    name: Optional[str] = None
    trigger_price: float = 0.0
    stoploss_price: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float = 0.0
    confidence: float = 0.0
    reason: str = ''
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
```

**Step 3: 运行测试验证通过**

Run: `pytest tests/unit/test_strategy.py::TestLimitupCallbackStrategy -v`
Expected: PASS

**Step 4: Commit**

```bash
git add services/strategy_service/limitup_callback/
git commit -m "feat: implement limitup callback strategy core logic"
```

---

## Phase 3-5 概要

由于篇幅限制，以下是 Phase 3-5 的核心任务概要：

### Phase 3: 策略实现
- Task 3.1: 尾盘选股策略 (`services/strategy_service/endstock_pick/`)
- Task 3.2: 龙回头策略 (`services/strategy_service/dragon_head/`)

### Phase 4: 风控系统
- Task 4.1: 凯利公式仓位计算 (`services/risk_service/position/kelly_calculator.py`)
- Task 4.2: 止盈止损管理器 (`services/risk_service/stoploss/manager.py`)
- Task 4.3: 熔断机制 (`services/risk_service/circuit_breaker/manager.py`)

### Phase 5: 回测优化
- Task 5.1: Backtrader 集成适配器
- Task 5.2: 回测结果分析器

---

## 执行建议

**Plan complete and saved to `docs/plans/2026-04-19-quantitative-trading-implementation.md`. Two execution options:**

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
