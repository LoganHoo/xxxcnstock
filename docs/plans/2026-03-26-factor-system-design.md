# 指标与因子体系设计文档

## 一、概述

### 1.1 目标
建立可配置的多策略选股系统，支持通过配置文件灵活组合多个因子形成选股策略。

### 1.2 设计原则
- **模块化**: 每个因子独立实现，便于维护和扩展
- **可配置**: 通过 YAML 配置文件管理因子和策略
- **可扩展**: 支持新增因子类别和策略类型
- **高性能**: 使用 Polars + DuckDB 保证计算效率

### 1.3 因子类别
| 类别 | 说明 | 示例因子 |
|------|------|----------|
| 技术指标因子 | 基于价格和成交量的技术分析 | MA、MACD、RSI、KDJ、布林带 |
| 量价因子 | 分析成交量与价格关系 | 量比、换手率、CVD、量价背离 |
| 基本面因子 | 基于财务数据分析 | PE、PB、ROE、营收增长、净利润增长 |
| 资金流向因子 | 分析市场资金动向 | 北向资金、主力资金、融资融券 |

---

## 二、系统架构

### 2.1 目录结构

```
config/
├── factors/                    # 因子配置目录
│   ├── technical.yaml          # 技术指标因子
│   ├── volume_price.yaml       # 量价因子
│   ├── fundamental.yaml        # 基本面因子
│   └── capital_flow.yaml       # 资金流向因子
├── strategies/                 # 策略配置目录
│   ├── trend_following.yaml    # 趋势跟踪策略
│   ├── reversal.yaml           # 反转策略
│   └── momentum.yaml           # 动量策略
└── factor_weights.yaml         # 因子权重配置

core/
├── factor_engine.py            # 因子计算引擎
├── factor_library.py           # 因子库基类
├── strategy_engine.py          # 策略引擎
└── backtest.py                 # 回测框架

factors/                        # 因子实现目录
├── technical/                  # 技术指标因子
│   ├── __init__.py
│   ├── ma.py                   # 均线因子
│   ├── macd.py                 # MACD因子
│   ├── rsi.py                  # RSI因子
│   ├── kdj.py                  # KDJ因子
│   └── bollinger.py            # 布林带因子
├── volume_price/               # 量价因子
│   ├── __init__.py
│   ├── volume_ratio.py         # 量比因子
│   ├── turnover.py             # 换手率因子
│   └── cvd.py                  # CVD因子
├── fundamental/                # 基本面因子
│   ├── __init__.py
│   ├── pe_pb.py                # 估值因子
│   ├── roe.py                  # ROE因子
│   └── growth.py               # 成长因子
└── capital_flow/               # 资金流向因子
    ├── __init__.py
    ├── north_money.py          # 北向资金因子
    ├── main_force.py           # 主力资金因子
    └── margin.py               # 融资融券因子

scripts/
├── run_strategy.py             # 运行策略脚本
└── backtest_strategy.py        # 回测策略脚本
```

### 2.2 数据流

```
K线数据 (Parquet)
       ↓
  因子计算引擎
       ↓
  因子得分表 (DataFrame)
       ↓
  策略引擎
       ↓
  选股结果
       ↓
  输出报告 (JSON/HTML/TXT)
```

---

## 三、因子设计

### 3.1 因子基类

```python
# core/factor_library.py
from abc import ABC, abstractmethod
import polars as pl
from typing import Dict, Any

class BaseFactor(ABC):
    """因子基类"""
    
    def __init__(self, name: str, category: str, params: Dict[str, Any] = None):
        self.name = name
        self.category = category
        self.params = params or {}
    
    @abstractmethod
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        计算因子值
        
        Args:
            data: 包含 K 线数据的 DataFrame
        
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
        return max(0, min(1, (value - min_val) / (max_val - min_val)))
    
    def get_score(self, factor_value: float) -> float:
        """
        将因子值转换为得分 (0-100)
        
        Args:
            factor_value: 因子值
        
        Returns:
            得分 (0-100)
        """
        return self.normalize(factor_value) * 100
```

### 3.2 技术指标因子

#### 3.2.1 均线趋势因子 (MA Trend)

```python
# factors/technical/ma.py
class MATrendFactor(BaseFactor):
    """均线趋势因子"""
    
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(
            name="ma_trend",
            category="technical",
            params=params or {"short": 5, "mid": 10, "long": 20}
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算均线趋势得分"""
        short = self.params["short"]
        mid = self.params["mid"]
        long = self.params["long"]
        
        # 计算均线
        df = data.sort("trade_date").with_columns([
            pl.col("close").rolling_mean(short).alias("ma_short"),
            pl.col("close").rolling_mean(mid).alias("ma_mid"),
            pl.col("close").rolling_mean(long).alias("ma_long"),
        ])
        
        # 计算趋势得分
        latest = df.tail(1)
        close = latest["close"].item()
        ma_short = latest["ma_short"].item()
        ma_mid = latest["ma_mid"].item()
        ma_long = latest["ma_long"].item()
        
        # 多头排列: close > ma_short > ma_mid > ma_long
        if close > ma_short > ma_mid > ma_long:
            score = 100
        elif close > ma_short > ma_mid:
            score = 80
        elif close > ma_short:
            score = 60
        elif close > ma_long:
            score = 40
        else:
            score = 20
        
        return data.with_columns([
            pl.lit(score).alias("factor_ma_trend")
        ])
```

#### 3.2.2 MACD 因子

```python
# factors/technical/macd.py
class MACDFactor(BaseFactor):
    """MACD 金叉死叉因子"""
    
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(
            name="macd",
            category="technical",
            params=params or {"fast": 12, "slow": 26, "signal": 9}
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 MACD 因子"""
        fast = self.params["fast"]
        slow = self.params["slow"]
        signal = self.params["signal"]
        
        # 计算 EMA
        df = data.sort("trade_date").with_columns([
            pl.col("close").ewm_mean(span=fast).alias("ema_fast"),
            pl.col("close").ewm_mean(span=slow).alias("ema_slow"),
        ])
        
        # 计算 MACD
        df = df.with_columns([
            (pl.col("ema_fast") - pl.col("ema_slow")).alias("dif"),
        ])
        
        df = df.with_columns([
            pl.col("dif").ewm_mean(span=signal).alias("dea"),
        ])
        
        df = df.with_columns([
            (pl.col("dif") - pl.col("dea")).alias("macd"),
        ])
        
        # 计算得分
        latest = df.tail(2)
        if len(latest) < 2:
            return data.with_columns([pl.lit(50).alias("factor_macd")])
        
        macd_today = latest["macd"].tail(1).item()
        macd_yest = latest["macd"].head(1).item()
        dif_today = latest["dif"].tail(1).item()
        
        # 金叉信号
        if macd_today > 0 and macd_yest <= 0:
            score = 100  # 金叉
        elif macd_today > 0 and dif_today > 0:
            score = 80   # 多头区域
        elif macd_today > 0:
            score = 60   # MACD 为正
        elif macd_today < 0 and macd_yest >= 0:
            score = 20   # 死叉
        else:
            score = 40   # 空头区域
        
        return data.with_columns([
            pl.lit(score).alias("factor_macd")
        ])
```

### 3.3 量价因子

#### 3.3.1 量比因子

```python
# factors/volume_price/volume_ratio.py
class VolumeRatioFactor(BaseFactor):
    """量比因子"""
    
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(
            name="volume_ratio",
            category="volume_price",
            params=params or {"period": 5}
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算量比得分"""
        period = self.params["period"]
        
        df = data.sort("trade_date").with_columns([
            pl.col("volume").rolling_mean(period).alias("vol_ma"),
        ])
        
        latest = df.tail(1)
        volume = latest["volume"].item()
        vol_ma = latest["vol_ma"].item()
        
        if vol_ma == 0:
            ratio = 1
        else:
            ratio = volume / vol_ma
        
        # 量比评分
        if ratio > 3:
            score = 100  # 极度放量
        elif ratio > 2:
            score = 80   # 明显放量
        elif ratio > 1.5:
            score = 60   # 温和放量
        elif ratio > 0.8:
            score = 50   # 正常
        else:
            score = 30   # 缩量
        
        return data.with_columns([
            pl.lit(ratio).alias("volume_ratio"),
            pl.lit(score).alias("factor_volume_ratio"),
        ])
```

### 3.4 基本面因子

#### 3.4.1 PE/PB 估值因子

```python
# factors/fundamental/pe_pb.py
class PEPBFactor(BaseFactor):
    """估值因子"""
    
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(
            name="pe_pb",
            category="fundamental",
            params=params or {}
        )
    
    def calculate(self, data: pl.DataFrame, fundamental_data: pl.DataFrame) -> pl.DataFrame:
        """计算估值得分"""
        # 关联基本面数据
        df = data.join(fundamental_data, on="code", how="left")
        
        pe = df.select("pe_ttm").tail(1).item()
        pb = df.select("pb").tail(1).item()
        
        # PE 评分 (越低越好，但排除负值)
        if pe is None or pe <= 0:
            pe_score = 50
        elif pe < 15:
            pe_score = 100
        elif pe < 25:
            pe_score = 80
        elif pe < 40:
            pe_score = 60
        else:
            pe_score = 40
        
        # PB 评分 (越低越好)
        if pb is None or pb <= 0:
            pb_score = 50
        elif pb < 1:
            pb_score = 100
        elif pb < 2:
            pb_score = 80
        elif pb < 3:
            pb_score = 60
        else:
            pb_score = 40
        
        score = (pe_score + pb_score) / 2
        
        return data.with_columns([
            pl.lit(score).alias("factor_pe_pb"),
        ])
```

### 3.5 资金流向因子

#### 3.5.1 北向资金因子

```python
# factors/capital_flow/north_money.py
class NorthMoneyFactor(BaseFactor):
    """北向资金因子"""
    
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(
            name="north_money",
            category="capital_flow",
            params=params or {"period": 5}
        )
    
    def calculate(self, data: pl.DataFrame, north_money_data: pl.DataFrame) -> pl.DataFrame:
        """计算北向资金得分"""
        period = self.params["period"]
        
        # 计算近期北向资金净流入
        recent_flow = north_money_data.tail(period)["net_inflow"].sum()
        
        # 评分
        if recent_flow > 1e9:  # 10亿以上
            score = 100
        elif recent_flow > 5e8:  # 5亿以上
            score = 80
        elif recent_flow > 0:
            score = 60
        elif recent_flow > -5e8:
            score = 40
        else:
            score = 20
        
        return data.with_columns([
            pl.lit(recent_flow).alias("north_money_flow"),
            pl.lit(score).alias("factor_north_money"),
        ])
```

---

## 四、策略引擎

### 4.1 策略配置格式

```yaml
# config/strategies/trend_following.yaml
strategy:
  name: "趋势跟踪策略"
  description: "捕捉中期趋势，顺势而为"
  version: "1.0"
  
  # 因子配置
  factors:
    - name: ma_trend
      category: technical
      weight: 0.25
      threshold: 60
      params:
        short: 5
        mid: 10
        long: 20
    
    - name: macd
      category: technical
      weight: 0.20
      threshold: 50
      params:
        fast: 12
        slow: 26
        signal: 9
    
    - name: volume_ratio
      category: volume_price
      weight: 0.15
      threshold: 50
      params:
        period: 5
    
    - name: momentum_10d
      category: technical
      weight: 0.20
      threshold: 5
    
    - name: north_money
      category: capital_flow
      weight: 0.20
      threshold: 0
      params:
        period: 5
  
  # 筛选条件
  filters:
    - type: market_cap
      min: 30      # 亿
      max: 500     # 亿
    
    - type: price
      min: 3       # 元
      max: 100     # 元
    
    - type: change_pct
      min: -10     # %
      max: 10      # %
  
  # 输出配置
  output:
    top_n: 20
    min_score: 70
    formats: ["json", "html", "txt"]
```

### 4.2 策略引擎实现

```python
# core/strategy_engine.py
import yaml
import polars as pl
from pathlib import Path
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class StrategyEngine:
    """策略引擎"""
    
    def __init__(self, strategy_config: str, factor_engine):
        self.config = self._load_config(strategy_config)
        self.factor_engine = factor_engine
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self, config_path: str) -> dict:
        """加载策略配置"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def calculate_factor_scores(self, stock_data: pl.DataFrame) -> pl.DataFrame:
        """计算所有因子得分"""
        df = stock_data.clone()
        
        for factor_config in self.config["strategy"]["factors"]:
            factor_name = factor_config["name"]
            category = factor_config["category"]
            params = factor_config.get("params", {})
            
            # 获取因子实例
            factor = self.factor_engine.get_factor(factor_name, category, params)
            
            # 计算因子得分
            df = factor.calculate(df)
        
        return df
    
    def calculate_weighted_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算加权综合得分"""
        factors = self.config["strategy"]["factors"]
        
        # 构建加权得分表达式
        score_expr = pl.lit(0.0)
        for factor_config in factors:
            factor_name = factor_config["name"]
            weight = factor_config["weight"]
            score_expr = score_expr + pl.col(f"factor_{factor_name}") * weight
        
        return df.with_columns([
            score_expr.alias("strategy_score")
        ])
    
    def apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用筛选条件"""
        filters = self.config["strategy"].get("filters", [])
        
        for f in filters:
            filter_type = f["type"]
            
            if filter_type == "market_cap":
                # 需要关联市值数据
                pass
            elif filter_type == "price":
                df = df.filter(
                    (pl.col("close") >= f["min"]) & 
                    (pl.col("close") <= f["max"])
                )
            elif filter_type == "change_pct":
                df = df.filter(
                    (pl.col("change_pct") >= f["min"]) & 
                    (pl.col("change_pct") <= f["max"])
                )
        
        return df
    
    def select_stocks(self, stock_data: pl.DataFrame) -> pl.DataFrame:
        """执行选股"""
        self.logger.info(f"开始执行策略: {self.config['strategy']['name']}")
        
        # 1. 计算因子得分
        df = self.calculate_factor_scores(stock_data)
        
        # 2. 计算综合得分
        df = self.calculate_weighted_score(df)
        
        # 3. 应用筛选条件
        df = self.apply_filters(df)
        
        # 4. 过滤低分股票
        min_score = self.config["strategy"]["output"]["min_score"]
        df = df.filter(pl.col("strategy_score") >= min_score)
        
        # 5. 排序取前 N
        top_n = self.config["strategy"]["output"]["top_n"]
        df = df.sort("strategy_score", descending=True).head(top_n)
        
        self.logger.info(f"选出 {len(df)} 只股票")
        
        return df
```

---

## 五、因子计算引擎

```python
# core/factor_engine.py
import importlib
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
import logging

logger = logging.getLogger(__name__)

class FactorEngine:
    """因子计算引擎"""
    
    def __init__(self, config_dir: str = "config/factors"):
        self.config_dir = Path(config_dir)
        self.factor_registry = {}
        self._load_factor_configs()
    
    def _load_factor_configs(self):
        """加载所有因子配置"""
        for config_file in self.config_dir.glob("*.yaml"):
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                for factor_config in config.get("factors", []):
                    self.factor_registry[factor_config["name"]] = factor_config
    
    def get_factor(self, name: str, category: str, params: Dict[str, Any] = None):
        """获取因子实例"""
        # 动态导入因子模块
        module_path = f"factors.{category}.{name}"
        try:
            module = importlib.import_module(module_path)
            factor_class = getattr(module, f"{name.capitalize()}Factor")
            return factor_class(params=params)
        except (ImportError, AttributeError) as e:
            logger.warning(f"因子 {name} 未找到，使用默认实现: {e}")
            return self._get_default_factor(name, category, params)
    
    def _get_default_factor(self, name: str, category: str, params: Dict[str, Any]):
        """获取默认因子实现"""
        from core.factor_library import BaseFactor
        
        class DefaultFactor(BaseFactor):
            def calculate(self, data):
                return data.with_columns([
                    pl.lit(50).alias(f"factor_{name}")
                ])
        
        return DefaultFactor(name=name, category=category, params=params)
    
    def list_factors(self) -> Dict[str, dict]:
        """列出所有可用因子"""
        return self.factor_registry.copy()
    
    def get_factor_info(self, name: str) -> Optional[dict]:
        """获取因子信息"""
        return self.factor_registry.get(name)
```

---

## 六、使用示例

### 6.1 运行策略

```python
# scripts/run_strategy.py
from core.factor_engine import FactorEngine
from core.strategy_engine import StrategyEngine
import polars as pl

# 初始化引擎
factor_engine = FactorEngine(config_dir="config/factors")
strategy_engine = StrategyEngine(
    strategy_config="config/strategies/trend_following.yaml",
    factor_engine=factor_engine
)

# 加载股票数据
kline_dir = "data/kline/*.parquet"
stock_data = pl.read_parquet(kline_dir)

# 执行选股
selected_stocks = strategy_engine.select_stocks(stock_data)

# 输出结果
print(selected_stocks.select([
    "code", "name", "close", "strategy_score"
]))
```

### 6.2 回测策略

```python
# scripts/backtest_strategy.py
from core.backtest import BacktestEngine
from core.strategy_engine import StrategyEngine

# 初始化
strategy_engine = StrategyEngine("config/strategies/trend_following.yaml")
backtest_engine = BacktestEngine(strategy_engine)

# 执行回测
result = backtest_engine.run(
    start_date="2025-01-01",
    end_date="2026-03-26",
    initial_capital=1000000
)

# 输出回测结果
print(f"总收益率: {result['total_return']:.2%}")
print(f"年化收益: {result['annual_return']:.2%}")
print(f"最大回撤: {result['max_drawdown']:.2%}")
print(f"夏普比率: {result['sharpe_ratio']:.2f}")
```

---

## 七、实施计划

### 7.1 第一阶段：核心框架 (1-2天)

| 任务 | 说明 |
|------|------|
| 创建因子基类 | `core/factor_library.py` |
| 实现因子引擎 | `core/factor_engine.py` |
| 实现策略引擎 | `core/strategy_engine.py` |
| 创建配置目录结构 | `config/factors/`, `config/strategies/` |

### 7.2 第二阶段：因子实现 (2-3天)

| 任务 | 说明 |
|------|------|
| 技术指标因子 | MA、MACD、RSI、KDJ、布林带 |
| 量价因子 | 量比、换手率、CVD |
| 基本面因子 | PE/PB、ROE、成长因子 |
| 资金流向因子 | 北向资金、主力资金 |

### 7.3 第三阶段：策略配置 (1天)

| 任务 | 说明 |
|------|------|
| 趋势跟踪策略 | 配置文件 + 验证 |
| 反转策略 | 配置文件 + 验证 |
| 动量策略 | 配置文件 + 验证 |

### 7.4 第四阶段：集成测试 (1天)

| 任务 | 说明 |
|------|------|
| 单元测试 | 各因子测试用例 |
| 集成测试 | 策略执行测试 |
| 性能测试 | 大规模数据测试 |

---

## 八、后续扩展

### 8.1 待添加因子
- 情绪因子（舆情分析）
- 龙虎榜因子
- 机构持仓因子
- 行业轮动因子

### 8.2 待添加策略
- 行业轮动策略
- 事件驱动策略
- 多因子量化策略

### 8.3 功能增强
- Web 界面配置
- 实时策略监控
- 自动调参优化
