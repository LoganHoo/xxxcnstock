# 因子体系实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 建立可配置的多策略选股系统，支持通过配置文件灵活组合多个因子形成选股策略。

**Architecture:** 采用模块化设计，因子基类定义统一接口，因子引擎负责加载和计算，策略引擎负责组合因子和选股。使用 Polars 进行高效数据处理，YAML 配置文件管理因子和策略。

**Tech Stack:** Python 3.11+, Polars, DuckDB, YAML, Pytest

---

## Task 1: 创建因子基类

**Files:**
- Create: `core/factor_library.py`
- Create: `core/__init__.py`

**Step 1: 创建 core 目录**

```bash
mkdir -p core
touch core/__init__.py
```

**Step 2: 编写因子基类**

```python
# core/factor_library.py
"""
因子库基类
定义所有因子的统一接口
"""
from abc import ABC, abstractmethod
import polars as pl
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class BaseFactor(ABC):
    """因子基类"""
    
    def __init__(
        self, 
        name: str, 
        category: str, 
        params: Dict[str, Any] = None,
        description: str = ""
    ):
        self.name = name
        self.category = category
        self.params = params or {}
        self.description = description
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        计算因子值
        
        Args:
            data: 包含 K 线数据的 DataFrame
                  必须包含: code, trade_date, open, high, low, close, volume
        
        Returns:
            添加了因子列的 DataFrame
        """
        pass
    
    def normalize(self, value: float, min_val: float = 0, max_val: float = 100) -> float:
        """
        标准化因子值到 0-1 区间
        
        Args:
            value: 原始值
            min_val: 最小值
            max_val: 最大值
        
        Returns:
            标准化后的值 (0-1)
        """
        if max_val == min_val:
            return 0.5
        return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))
    
    def get_score(self, factor_value: float) -> float:
        """
        将因子值转换为得分 (0-100)
        
        Args:
            factor_value: 因子值
        
        Returns:
            得分 (0-100)
        """
        return self.normalize(factor_value) * 100
    
    def get_factor_column_name(self) -> str:
        """获取因子列名"""
        return f"factor_{self.name}"
    
    def __repr__(self) -> str:
        return f"Factor(name={self.name}, category={self.category}, params={self.params})"


class FactorRegistry:
    """因子注册表"""
    
    _instance = None
    _factors: Dict[str, type] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def register(cls, name: str, factor_class: type):
        """注册因子"""
        cls._factors[name] = factor_class
        logger.info(f"注册因子: {name}")
    
    @classmethod
    def get(cls, name: str) -> Optional[type]:
        """获取因子类"""
        return cls._factors.get(name)
    
    @classmethod
    def list_all(cls) -> Dict[str, type]:
        """列出所有已注册因子"""
        return cls._factors.copy()


def register_factor(name: str):
    """因子注册装饰器"""
    def decorator(cls):
        FactorRegistry.register(name, cls)
        return cls
    return decorator
```

**Step 3: 创建测试文件**

```python
# tests/test_factor_library.py
"""
因子基类测试
"""
import pytest
import polars as pl
from core.factor_library import BaseFactor, FactorRegistry, register_factor


class TestBaseFactor:
    """BaseFactor 测试"""
    
    def test_normalize(self):
        """测试标准化函数"""
        class DummyFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor = DummyFactor("test", "test")
        
        assert factor.normalize(50, 0, 100) == 0.5
        assert factor.normalize(0, 0, 100) == 0.0
        assert factor.normalize(100, 0, 100) == 1.0
        assert factor.normalize(150, 0, 100) == 1.0  # 超出范围
        assert factor.normalize(-50, 0, 100) == 0.0  # 超出范围
    
    def test_get_score(self):
        """测试得分计算"""
        class DummyFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor = DummyFactor("test", "test")
        
        assert factor.get_score(50) == 50.0
        assert factor.get_score(0) == 0.0
        assert factor.get_score(100) == 100.0
    
    def test_get_factor_column_name(self):
        """测试因子列名"""
        class DummyFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor = DummyFactor("ma_trend", "technical")
        assert factor.get_factor_column_name() == "factor_ma_trend"


class TestFactorRegistry:
    """FactorRegistry 测试"""
    
    def test_register_and_get(self):
        """测试注册和获取"""
        @register_factor("test_factor")
        class TestFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor_class = FactorRegistry.get("test_factor")
        assert factor_class is TestFactor
    
    def test_list_all(self):
        """测试列出所有因子"""
        factors = FactorRegistry.list_all()
        assert isinstance(factors, dict)
```

**Step 4: 运行测试**

```bash
pytest tests/test_factor_library.py -v
```

Expected: PASS

**Step 5: 提交**

```bash
git add core/__init__.py core/factor_library.py tests/test_factor_library.py
git commit -m "feat: add factor base class and registry"
```

---

## Task 2: 创建因子计算引擎

**Files:**
- Create: `core/factor_engine.py`
- Create: `config/factors/technical.yaml`

**Step 1: 创建配置目录**

```bash
mkdir -p config/factors config/strategies
```

**Step 2: 创建技术指标因子配置**

```yaml
# config/factors/technical.yaml
factors:
  - name: ma_trend
    category: technical
    description: "均线趋势因子 - 判断多头/空头排列"
    params:
      short_period: 5
      mid_period: 10
      long_period: 20
    weight: 0.15
    enabled: true
    
  - name: macd
    category: technical
    description: "MACD因子 - 金叉死叉信号"
    params:
      fast: 12
      slow: 26
      signal: 9
    weight: 0.10
    enabled: true
    
  - name: rsi
    category: technical
    description: "RSI因子 - 超买超卖判断"
    params:
      period: 14
    weight: 0.10
    enabled: true
    
  - name: kdj
    category: technical
    description: "KDJ因子 - 随机指标"
    params:
      n: 9
      m1: 3
      m2: 3
    weight: 0.08
    enabled: true
    
  - name: bollinger
    category: technical
    description: "布林带因子 - 波动区间"
    params:
      period: 20
      std_dev: 2
    weight: 0.07
    enabled: true
```

**Step 3: 编写因子引擎**

```python
# core/factor_engine.py
"""
因子计算引擎
负责加载因子配置和计算因子值
"""
import yaml
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import importlib
import logging

from core.factor_library import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


class FactorEngine:
    """因子计算引擎"""
    
    def __init__(self, config_dir: str = "config/factors"):
        self.config_dir = Path(config_dir)
        self.factor_configs: Dict[str, dict] = {}
        self._load_factor_configs()
    
    def _load_factor_configs(self):
        """加载所有因子配置"""
        if not self.config_dir.exists():
            logger.warning(f"因子配置目录不存在: {self.config_dir}")
            return
        
        for config_file in self.config_dir.glob("*.yaml"):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    for factor_config in config.get("factors", []):
                        name = factor_config["name"]
                        self.factor_configs[name] = factor_config
                        logger.debug(f"加载因子配置: {name}")
            except Exception as e:
                logger.error(f"加载配置文件失败 {config_file}: {e}")
        
        logger.info(f"加载了 {len(self.factor_configs)} 个因子配置")
    
    def get_factor(self, name: str, params: Dict[str, Any] = None) -> Optional[BaseFactor]:
        """
        获取因子实例
        
        Args:
            name: 因子名称
            params: 自定义参数 (覆盖默认参数)
        
        Returns:
            因子实例
        """
        # 1. 尝试从注册表获取
        factor_class = FactorRegistry.get(name)
        
        if factor_class:
            config = self.factor_configs.get(name, {})
            merged_params = {**config.get("params", {}), **(params or {})}
            return factor_class(
                name=name,
                category=config.get("category", "unknown"),
                params=merged_params,
                description=config.get("description", "")
            )
        
        # 2. 尝试动态导入
        config = self.factor_configs.get(name)
        if config:
            category = config.get("category", "technical")
            try:
                module_path = f"factors.{category}.{name}"
                module = importlib.import_module(module_path)
                factor_class = getattr(module, f"{self._to_class_name(name)}Factor")
                
                merged_params = {**config.get("params", {}), **(params or {})}
                return factor_class(
                    name=name,
                    category=category,
                    params=merged_params,
                    description=config.get("description", "")
                )
            except (ImportError, AttributeError) as e:
                logger.warning(f"动态导入因子失败 {name}: {e}")
        
        logger.warning(f"因子 {name} 未找到")
        return None
    
    def _to_class_name(self, name: str) -> str:
        """将因子名转换为类名"""
        parts = name.split("_")
        return "".join(p.capitalize() for p in parts)
    
    def calculate_factor(
        self, 
        data: pl.DataFrame, 
        factor_name: str, 
        params: Dict[str, Any] = None
    ) -> pl.DataFrame:
        """
        计算单个因子
        
        Args:
            data: K线数据
            factor_name: 因子名称
            params: 自定义参数
        
        Returns:
            添加了因子列的 DataFrame
        """
        factor = self.get_factor(factor_name, params)
        
        if factor is None:
            logger.warning(f"因子 {factor_name} 不存在，返回默认值 50")
            return data.with_columns([
                pl.lit(50.0).alias(f"factor_{factor_name}")
            ])
        
        return factor.calculate(data)
    
    def calculate_all_factors(
        self, 
        data: pl.DataFrame,
        factor_names: List[str] = None,
        enabled_only: bool = True
    ) -> pl.DataFrame:
        """
        计算多个因子
        
        Args:
            data: K线数据
            factor_names: 指定因子列表 (None 则计算所有)
            enabled_only: 是否只计算启用的因子
        
        Returns:
            添加了所有因子列的 DataFrame
        """
        df = data.clone()
        
        if factor_names is None:
            factor_names = list(self.factor_configs.keys())
        
        for name in factor_names:
            config = self.factor_configs.get(name, {})
            
            if enabled_only and not config.get("enabled", True):
                continue
            
            df = self.calculate_factor(df, name)
        
        return df
    
    def list_factors(self, category: str = None, enabled_only: bool = False) -> List[dict]:
        """
        列出因子
        
        Args:
            category: 按类别筛选
            enabled_only: 只列出启用的因子
        
        Returns:
            因子配置列表
        """
        factors = []
        
        for name, config in self.factor_configs.items():
            if category and config.get("category") != category:
                continue
            if enabled_only and not config.get("enabled", True):
                continue
            
            factors.append({
                "name": name,
                "category": config.get("category"),
                "description": config.get("description"),
                "weight": config.get("weight", 0),
                "enabled": config.get("enabled", True)
            })
        
        return factors
    
    def get_factor_info(self, name: str) -> Optional[dict]:
        """获取因子详细信息"""
        return self.factor_configs.get(name)
```

**Step 4: 创建测试**

```python
# tests/test_factor_engine.py
"""
因子引擎测试
"""
import pytest
import polars as pl
from pathlib import Path
from core.factor_engine import FactorEngine


class TestFactorEngine:
    """FactorEngine 测试"""
    
    @pytest.fixture
    def engine(self):
        """创建引擎实例"""
        return FactorEngine(config_dir="config/factors")
    
    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        return pl.DataFrame({
            "code": ["000001"] * 30,
            "trade_date": [f"2026-03-{i:02d}" for i in range(1, 31)],
            "open": [10.0 + i * 0.1 for i in range(30)],
            "high": [10.5 + i * 0.1 for i in range(30)],
            "low": [9.5 + i * 0.1 for i in range(30)],
            "close": [10.0 + i * 0.1 for i in range(30)],
            "volume": [1000000 + i * 10000 for i in range(30)]
        })
    
    def test_load_configs(self, engine):
        """测试加载配置"""
        assert len(engine.factor_configs) > 0
        assert "ma_trend" in engine.factor_configs
    
    def test_list_factors(self, engine):
        """测试列出因子"""
        factors = engine.list_factors()
        assert len(factors) > 0
        
        technical_factors = engine.list_factors(category="technical")
        assert all(f["category"] == "technical" for f in technical_factors)
    
    def test_get_factor_info(self, engine):
        """测试获取因子信息"""
        info = engine.get_factor_info("ma_trend")
        assert info is not None
        assert info["category"] == "technical"
    
    def test_calculate_factor_missing(self, engine, sample_data):
        """测试计算不存在的因子"""
        result = engine.calculate_factor(sample_data, "nonexistent_factor")
        assert "factor_nonexistent_factor" in result.columns
```

**Step 5: 运行测试**

```bash
pytest tests/test_factor_engine.py -v
```

Expected: PASS

**Step 6: 提交**

```bash
git add core/factor_engine.py config/factors/technical.yaml tests/test_factor_engine.py
git commit -m "feat: add factor calculation engine"
```

---

## Task 3: 实现技术指标因子

**Files:**
- Create: `factors/__init__.py`
- Create: `factors/technical/__init__.py`
- Create: `factors/technical/ma_trend.py`
- Create: `factors/technical/macd.py`
- Create: `factors/technical/rsi.py`

**Step 1: 创建目录结构**

```bash
mkdir -p factors/technical factors/volume_price factors/fundamental factors/capital_flow
touch factors/__init__.py
touch factors/technical/__init__.py
```

**Step 2: 实现均线趋势因子**

```python
# factors/technical/ma_trend.py
"""
均线趋势因子
判断多头排列、空头排列
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("ma_trend")
class MaTrendFactor(BaseFactor):
    """均线趋势因子"""
    
    def __init__(self, name: str = "ma_trend", category: str = "technical", 
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "均线趋势因子")
        
        self.short_period = self.params.get("short_period", 5)
        self.mid_period = self.params.get("mid_period", 10)
        self.long_period = self.params.get("long_period", 20)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算均线趋势得分"""
        df = data.sort("trade_date")
        
        # 计算均线
        df = df.with_columns([
            pl.col("close").rolling_mean(self.short_period).alias("ma_short"),
            pl.col("close").rolling_mean(self.mid_period).alias("ma_mid"),
            pl.col("close").rolling_mean(self.long_period).alias("ma_long"),
        ])
        
        # 获取最新数据
        latest = df.tail(1)
        
        close = latest["close"].item()
        ma_short = latest["ma_short"].item()
        ma_mid = latest["ma_mid"].item()
        ma_long = latest["ma_long"].item()
        
        # 计算趋势得分
        if ma_short is None or ma_mid is None or ma_long is None:
            score = 50.0
        elif close > ma_short > ma_mid > ma_long:
            score = 100.0  # 完美多头排列
        elif close > ma_short > ma_mid:
            score = 80.0   # 短中期多头
        elif close > ma_short:
            score = 60.0   # 短期多头
        elif close > ma_long:
            score = 40.0   # 站上长期均线
        elif close < ma_short < ma_mid < ma_long:
            score = 0.0    # 完美空头排列
        else:
            score = 30.0   # 弱势
        
        return data.with_columns([
            pl.lit(ma_short).alias("ma_short"),
            pl.lit(ma_mid).alias("ma_mid"),
            pl.lit(ma_long).alias("ma_long"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
```

**Step 3: 实现 MACD 因子**

```python
# factors/technical/macd.py
"""
MACD 因子
金叉死叉信号
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("macd")
class MacdFactor(BaseFactor):
    """MACD 因子"""
    
    def __init__(self, name: str = "macd", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "MACD因子")
        
        self.fast = self.params.get("fast", 12)
        self.slow = self.params.get("slow", 26)
        self.signal = self.params.get("signal", 9)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 MACD 因子"""
        df = data.sort("trade_date")
        
        # 计算 EMA
        df = df.with_columns([
            pl.col("close").ewm_mean(span=self.fast).alias("ema_fast"),
            pl.col("close").ewm_mean(span=self.slow).alias("ema_slow"),
        ])
        
        # 计算 DIF 和 DEA
        df = df.with_columns([
            (pl.col("ema_fast") - pl.col("ema_slow")).alias("dif"),
        ])
        
        df = df.with_columns([
            pl.col("dif").ewm_mean(span=self.signal).alias("dea"),
        ])
        
        # 计算 MACD
        df = df.with_columns([
            (pl.col("dif") - pl.col("dea")).alias("macd"),
        ])
        
        # 获取最近两天数据
        recent = df.tail(2)
        
        if len(recent) < 2:
            return data.with_columns([
                pl.lit(50.0).alias(self.get_factor_column_name())
            ])
        
        macd_today = recent["macd"].tail(1).item()
        macd_yest = recent["macd"].head(1).item()
        dif_today = recent["dif"].tail(1).item()
        
        # 计算得分
        if macd_today > 0 and macd_yest <= 0:
            score = 100.0  # 金叉
        elif macd_today > 0 and dif_today > 0:
            score = 80.0   # 多头区域
        elif macd_today > 0:
            score = 60.0   # MACD 为正
        elif macd_today < 0 and macd_yest >= 0:
            score = 20.0   # 死叉
        elif macd_today < 0:
            score = 40.0   # 空头区域
        else:
            score = 50.0
        
        return data.with_columns([
            pl.lit(dif_today).alias("dif"),
            pl.lit(recent["dea"].tail(1).item()).alias("dea"),
            pl.lit(macd_today).alias("macd"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
```

**Step 4: 实现 RSI 因子**

```python
# factors/technical/rsi.py
"""
RSI 因子
相对强弱指标
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("rsi")
class RsiFactor(BaseFactor):
    """RSI 因子"""
    
    def __init__(self, name: str = "rsi", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "RSI因子")
        
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 RSI 因子"""
        df = data.sort("trade_date")
        
        # 计算价格变化
        df = df.with_columns([
            pl.col("close").diff().alias("price_change"),
        ])
        
        # 计算上涨和下跌
        df = df.with_columns([
            pl.when(pl.col("price_change") > 0)
              .then(pl.col("price_change"))
              .otherwise(0).alias("gain"),
            pl.when(pl.col("price_change") < 0)
              .then(-pl.col("price_change"))
              .otherwise(0).alias("loss"),
        ])
        
        # 计算平均上涨和下跌
        df = df.with_columns([
            pl.col("gain").rolling_mean(self.period).alias("avg_gain"),
            pl.col("loss").rolling_mean(self.period).alias("avg_loss"),
        ])
        
        # 计算 RSI
        df = df.with_columns([
            pl.when(pl.col("avg_loss") == 0)
              .then(100.0)
              .otherwise(100 - 100 / (1 + pl.col("avg_gain") / pl.col("avg_loss")))
              .alias("rsi")
        ])
        
        # 获取最新 RSI
        latest = df.tail(1)
        rsi_value = latest["rsi"].item()
        
        # 计算得分
        if rsi_value is None:
            score = 50.0
        elif rsi_value < 30:
            score = 80.0   # 超卖，可能反弹
        elif rsi_value < 50:
            score = 60.0   # 相对弱势
        elif rsi_value < 70:
            score = 50.0   # 正常区间
        elif rsi_value < 80:
            score = 30.0   # 超买，注意风险
        else:
            score = 20.0   # 严重超买
        
        return data.with_columns([
            pl.lit(rsi_value).alias("rsi"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
```

**Step 5: 创建测试**

```python
# tests/test_technical_factors.py
"""
技术指标因子测试
"""
import pytest
import polars as pl
from factors.technical.ma_trend import MaTrendFactor
from factors.technical.macd import MacdFactor
from factors.technical.rsi import RsiFactor


@pytest.fixture
def sample_data():
    """创建测试数据"""
    return pl.DataFrame({
        "code": ["000001"] * 50,
        "trade_date": [f"2026-03-{i:02d}" for i in range(1, 51)],
        "open": [10.0 + i * 0.1 + (i % 3 - 1) * 0.05 for i in range(50)],
        "high": [10.5 + i * 0.1 for i in range(50)],
        "low": [9.5 + i * 0.1 for i in range(50)],
        "close": [10.0 + i * 0.1 for i in range(50)],
        "volume": [1000000 + i * 10000 for i in range(50)]
    })


class TestMaTrendFactor:
    """均线趋势因子测试"""
    
    def test_calculate(self, sample_data):
        """测试计算"""
        factor = MaTrendFactor()
        result = factor.calculate(sample_data)
        
        assert "factor_ma_trend" in result.columns
        assert "ma_short" in result.columns
        assert "ma_mid" in result.columns
        assert "ma_long" in result.columns
    
    def test_bullish_alignment(self, sample_data):
        """测试多头排列"""
        factor = MaTrendFactor()
        result = factor.calculate(sample_data)
        
        score = result["factor_ma_trend"].item()
        assert 0 <= score <= 100


class TestMacdFactor:
    """MACD 因子测试"""
    
    def test_calculate(self, sample_data):
        """测试计算"""
        factor = MacdFactor()
        result = factor.calculate(sample_data)
        
        assert "factor_macd" in result.columns
        assert "dif" in result.columns
        assert "dea" in result.columns
        assert "macd" in result.columns


class TestRsiFactor:
    """RSI 因子测试"""
    
    def test_calculate(self, sample_data):
        """测试计算"""
        factor = RsiFactor()
        result = factor.calculate(sample_data)
        
        assert "factor_rsi" in result.columns
        assert "rsi" in result.columns
```

**Step 6: 运行测试**

```bash
pytest tests/test_technical_factors.py -v
```

Expected: PASS

**Step 7: 提交**

```bash
git add factors/ tests/test_technical_factors.py
git commit -m "feat: implement technical factors (MA, MACD, RSI)"
```

---

## Task 4: 创建策略引擎

**Files:**
- Create: `core/strategy_engine.py`
- Create: `config/strategies/trend_following.yaml`

**Step 1: 创建策略配置**

```yaml
# config/strategies/trend_following.yaml
strategy:
  name: "趋势跟踪策略"
  description: "捕捉中期趋势，顺势而为"
  version: "1.0"
  
  factors:
    - name: ma_trend
      weight: 0.30
      threshold: 60
      params:
        short_period: 5
        mid_period: 10
        long_period: 20
    
    - name: macd
      weight: 0.25
      threshold: 50
      params:
        fast: 12
        slow: 26
        signal: 9
    
    - name: rsi
      weight: 0.20
      threshold: 40
      params:
        period: 14
    
    - name: volume_ratio
      weight: 0.25
      threshold: 50
  
  filters:
    - type: price
      min: 3
      max: 100
    
    - type: change_pct
      min: -10
      max: 10
  
  output:
    top_n: 20
    min_score: 60
    formats: ["json", "txt"]
```

**Step 2: 编写策略引擎**

```python
# core/strategy_engine.py
"""
策略引擎
负责组合因子和执行选股
"""
import yaml
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

from core.factor_engine import FactorEngine

logger = logging.getLogger(__name__)


class StrategyEngine:
    """策略引擎"""
    
    def __init__(self, strategy_config: str, factor_engine: FactorEngine = None):
        self.config_path = Path(strategy_config)
        self.config = self._load_config()
        self.factor_engine = factor_engine or FactorEngine()
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self) -> dict:
        """加载策略配置"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    @property
    def strategy_name(self) -> str:
        """策略名称"""
        return self.config["strategy"]["name"]
    
    @property
    def factors(self) -> List[dict]:
        """因子配置"""
        return self.config["strategy"]["factors"]
    
    @property
    def filters(self) -> List[dict]:
        """筛选条件"""
        return self.config["strategy"].get("filters", [])
    
    @property
    def output_config(self) -> dict:
        """输出配置"""
        return self.config["strategy"]["output"]
    
    def calculate_factor_scores(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算所有因子得分"""
        df = data.clone()
        
        for factor_config in self.factors:
            factor_name = factor_config["name"]
            params = factor_config.get("params")
            
            self.logger.debug(f"计算因子: {factor_name}")
            df = self.factor_engine.calculate_factor(df, factor_name, params)
        
        return df
    
    def calculate_weighted_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算加权综合得分"""
        score_expr = pl.lit(0.0)
        
        for factor_config in self.factors:
            factor_name = factor_config["name"]
            weight = factor_config["weight"]
            threshold = factor_config.get("threshold", 0)
            
            factor_col = f"factor_{factor_name}"
            
            # 应用阈值过滤
            score_expr = score_expr + pl.when(
                pl.col(factor_col) >= threshold
            ).then(
                pl.col(factor_col) * weight
            ).otherwise(
                pl.lit(0.0)
            )
        
        return df.with_columns([
            score_expr.alias("strategy_score")
        ])
    
    def apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用筛选条件"""
        for f in self.filters:
            filter_type = f["type"]
            
            if filter_type == "price":
                df = df.filter(
                    (pl.col("close") >= f["min"]) & 
                    (pl.col("close") <= f["max"])
                )
            elif filter_type == "change_pct":
                df = df.filter(
                    (pl.col("change_pct") >= f["min"]) & 
                    (pl.col("change_pct") <= f["max"])
                )
            elif filter_type == "market_cap":
                # TODO: 关联市值数据
                pass
        
        return df
    
    def select_stocks(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        执行选股
        
        Args:
            data: K线数据 DataFrame
        
        Returns:
            选中的股票 DataFrame
        """
        self.logger.info(f"开始执行策略: {self.strategy_name}")
        
        # 1. 计算因子得分
        df = self.calculate_factor_scores(data)
        
        # 2. 计算综合得分
        df = self.calculate_weighted_score(df)
        
        # 3. 应用筛选条件
        df = self.apply_filters(df)
        
        # 4. 过滤低分股票
        min_score = self.output_config.get("min_score", 0)
        df = df.filter(pl.col("strategy_score") >= min_score)
        
        # 5. 排序取前 N
        top_n = self.output_config.get("top_n", 20)
        df = df.sort("strategy_score", descending=True).head(top_n)
        
        self.logger.info(f"选出 {len(df)} 只股票")
        
        return df
    
    def get_strategy_info(self) -> dict:
        """获取策略信息"""
        return {
            "name": self.strategy_name,
            "description": self.config["strategy"].get("description", ""),
            "version": self.config["strategy"].get("version", "1.0"),
            "factors": [
                {"name": f["name"], "weight": f["weight"]}
                for f in self.factors
            ],
            "filters": self.filters,
            "output": self.output_config
        }
```

**Step 3: 创建测试**

```python
# tests/test_strategy_engine.py
"""
策略引擎测试
"""
import pytest
import polars as pl
from pathlib import Path
from core.strategy_engine import StrategyEngine
from core.factor_engine import FactorEngine


@pytest.fixture
def sample_data():
    """创建测试数据"""
    data = pl.DataFrame({
        "code": [f"00000{i}" for i in range(1, 11)],
        "trade_date": ["2026-03-26"] * 10,
        "open": [10.0 + i for i in range(10)],
        "high": [10.5 + i for i in range(10)],
        "low": [9.5 + i for i in range(10)],
        "close": [10.0 + i for i in range(10)],
        "volume": [1000000 + i * 100000 for i in range(10)],
        "change_pct": [1.0 + i * 0.5 for i in range(10)]
    })
    return data


class TestStrategyEngine:
    """StrategyEngine 测试"""
    
    def test_load_config(self):
        """测试加载配置"""
        engine = StrategyEngine("config/strategies/trend_following.yaml")
        
        assert engine.strategy_name == "趋势跟踪策略"
        assert len(engine.factors) > 0
    
    def test_get_strategy_info(self):
        """测试获取策略信息"""
        engine = StrategyEngine("config/strategies/trend_following.yaml")
        info = engine.get_strategy_info()
        
        assert "name" in info
        assert "factors" in info
    
    def test_calculate_weighted_score(self, sample_data):
        """测试加权得分计算"""
        engine = StrategyEngine("config/strategies/trend_following.yaml")
        
        # 添加模拟因子得分
        df = sample_data.with_columns([
            pl.lit(70.0).alias("factor_ma_trend"),
            pl.lit(60.0).alias("factor_macd"),
            pl.lit(50.0).alias("factor_rsi"),
            pl.lit(80.0).alias("factor_volume_ratio"),
        ])
        
        result = engine.calculate_weighted_score(df)
        
        assert "strategy_score" in result.columns
```

**Step 4: 运行测试**

```bash
pytest tests/test_strategy_engine.py -v
```

Expected: PASS

**Step 5: 提交**

```bash
git add core/strategy_engine.py config/strategies/trend_following.yaml tests/test_strategy_engine.py
git commit -m "feat: add strategy engine with weighted scoring"
```

---

## Task 5: 创建运行脚本

**Files:**
- Create: `scripts/run_strategy.py`

**Step 1: 编写运行脚本**

```python
# scripts/run_strategy.py
"""
运行选股策略
"""
import sys
from pathlib import Path
import argparse
import polars as pl
import json
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.factor_engine import FactorEngine
from core.strategy_engine import StrategyEngine


def main():
    parser = argparse.ArgumentParser(description="运行选股策略")
    parser.add_argument(
        "--strategy", "-s",
        default="config/strategies/trend_following.yaml",
        help="策略配置文件路径"
    )
    parser.add_argument(
        "--output", "-o",
        default="reports/strategy_result.json",
        help="输出文件路径"
    )
    parser.add_argument(
        "--top-n", "-n",
        type=int,
        default=20,
        help="输出股票数量"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"策略选股系统")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    # 初始化引擎
    print("初始化因子引擎...")
    factor_engine = FactorEngine()
    
    print(f"加载策略: {args.strategy}")
    strategy_engine = StrategyEngine(args.strategy, factor_engine)
    
    # 打印策略信息
    info = strategy_engine.get_strategy_info()
    print(f"\n策略名称: {info['name']}")
    print(f"策略描述: {info['description']}")
    print(f"\n因子配置:")
    for f in info["factors"]:
        print(f"  - {f['name']}: 权重 {f['weight']:.0%}")
    
    # 加载股票数据
    print("\n加载股票数据...")
    kline_pattern = str(project_root / "data" / "kline" / "*.parquet")
    
    try:
        stock_data = pl.read_parquet(kline_pattern)
        print(f"加载了 {len(stock_data)} 条记录")
    except Exception as e:
        print(f"加载数据失败: {e}")
        return
    
    # 执行选股
    print("\n执行选股...")
    result = strategy_engine.select_stocks(stock_data)
    
    # 输出结果
    print(f"\n选出 {len(result)} 只股票:")
    print("-" * 60)
    
    if len(result) > 0:
        # 选择要显示的列
        display_cols = ["code", "close", "strategy_score"]
        available_cols = [c for c in display_cols if c in result.columns]
        
        print(result.select(available_cols))
        
        # 保存结果
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        result_json = {
            "timestamp": datetime.now().isoformat(),
            "strategy": info,
            "stocks": result.to_dicts()
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n结果已保存到: {output_path}")
    else:
        print("未选出符合条件的股票")
    
    print("\n" + "=" * 60)
    print("选股完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

**Step 2: 测试运行**

```bash
python scripts/run_strategy.py --strategy config/strategies/trend_following.yaml
```

**Step 3: 提交**

```bash
git add scripts/run_strategy.py
git commit -m "feat: add strategy runner script"
```

---

## 执行选项

计划完成并保存到 `docs/plans/2026-03-26-factor-system-impl.md`。

**两种执行方式:**

**1. Subagent-Driven (本会话)** - 我为每个任务派发新的子代理，任务间进行审查，快速迭代

**2. Parallel Session (单独会话)** - 打开新会话使用 executing-plans，批量执行并设置检查点

**您选择哪种方式？**
