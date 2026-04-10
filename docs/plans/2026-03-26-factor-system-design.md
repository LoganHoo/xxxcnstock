# 指标与因子体系设计文档

## 一、概述

### 1.1 目标
建立可配置的多策略选股系统，支持通过配置文件灵活组合多个因子形成选股策略。

### 1.2 设计原则
- **模块化**: 每个因子独立实现，便于维护和扩展
- **可配置**: 通过 YAML 配置文件管理因子和策略
- **可扩展**: 支持新增因子类别和策略类型
- **高性能**: 使用 Polars + DuckDB 保证计算效率
- **多流程**: 支持三种并行选股流程，通过对比分析选择最优方案

### 1.3 因子类别
| 类别 | 说明 | 示例因子 |
|------|------|----------|
| 技术指标因子 | 基于价格和成交量的技术分析 | MA、MACD、RSI、KDJ、布林带、CCI、WR、ATR、DMI、EMV、ASI、ROC、PSY、MTM |
| 量价因子 | 分析成交量与价格关系 | 量比、换手率、MFI、OBV、VR、VOSC、WVAD、VMA、**主力共振** |
| 市场情绪因子 | 分析市场整体情绪状态 | 市场温度、市场情绪、市场趋势、市场广度、涨跌停情绪、领涨股状态、筹码峰值 |

### 1.4 过滤器类别
| 类别 | 说明 | 示例过滤器 |
|------|------|----------|
| 流动性过滤 | 排除低流动性股票 | 量比、换手率、连续低换手、成交量稳定性 |
| 估值过滤 | 排除估值异常股票 | PE/PB、价格区间、流通市值 |
| 市场过滤 | 市值和价格过滤 | 总市值、股价、停牌 |
| 技术过滤 | 技术形态过滤 | MA位置、MACD金叉死叉、趋势 |
| 模式过滤 | 特殊形态过滤 | 涨停陷阱、涨停后回调、反弹信号 |
| 基本面过滤 | 排除问题股 | 业绩、违规、市场崩盘 |
| 风控过滤 | 风险股票过滤 | ST股票、次新股、退市风险 |

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

### 3.4 市场情绪因子

#### 3.4.1 涨跌停情绪因子

```python
# factors/market/emotion_factors.py
class LimitUpScoreFactor(BaseFactor):
    """涨停家数 - 跌停家数 + 连板高度"""
```

#### 3.4.2 领涨股状态因子

```python
# factors/market/emotion_factors.py
class PioneerStatusFactor(BaseFactor):
    """核心领涨个股实时涨跌幅"""
```

#### 3.4.3 市场温度计

```python
# factors/market/market_temperature.py
class MarketTemperatureFactor(BaseFactor):
    """市场温度指标"""
```

#### 3.4.4 市场情绪综合

```python
# factors/market/market_sentiment.py
class MarketSentimentFactor(BaseFactor):
    """市场情绪指标"""
```

#### 3.4.5 市场趋势

```python
# factors/market/market_trend.py
class MarketTrendFactor(BaseFactor):
    """大盘指数趋势强度"""
```

#### 3.4.6 市场广度

```python
# factors/market/market_breadth.py
class MarketBreadthFactor(BaseFactor):
    """市场涨跌家数对比"""
```

#### 3.4.7 筹码峰值

```python
# factors/market/cost_peak.py
class CostPeakFactor(BaseFactor):
    """筹码分布最大密集峰位"""
```

### 3.5 主力痕迹共振因子

#### 3.5.1 主力共振因子

```python
# factors/volume_price/mainforce_resonance.py
class MainForceResonanceFactor(BaseFactor):
    """主力痕迹共振强度评分(0-100)

    综合评分 = (S1涨停质量 + S2缺口强度 + S3连阳强度 + S4放量强度)

    各子因子最大分值:
    - mf_s1_limit_up (涨停质量): 25分
    - mf_s2_gap (缺口强度): 25分
    - mf_s3_consecutive (连阳强度): 25分
    - mf_s4_volume (放量强度): 25分
    """
```

### 3.6 资金流向因子

#### 3.6.1 北向资金因子

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

### 7.2 第二阶段：因子实现 (已完成)

| 类别 | 已实现因子 |
|------|-----------|
| 技术指标因子 | MA、MACD、RSI、KDJ、布林带、CCI、WR、ATR、DMI、EMV、ASI、ROC、PSY、MTR、MA5Bias、MATrend |
| 量价因子 | 量比、换手率、MFI、OBV、VR、VOSC、WVAD、VMA、**主力共振** |
| 市场情绪因子 | 市场温度、市场情绪、市场趋势、市场广度、涨跌停情绪、领涨股状态、筹码峰值 |

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

---

## 附录：因子完整列表

### A.1 技术指标因子 (16个)

| 因子名称 | 类名 | 描述 |
|----------|------|------|
| ma_trend | MATrendFactor | 均线趋势因子，多头排列评分 |
| macd | MacdFactor | MACD金叉死叉因子 |
| rsi | RsiFactor | RSI超买超卖因子 |
| kdj | KdjFactor | KDJ随机指标因子 |
| bollinger | BollingerFactor | 布林带突破因子 |
| cci | CciFactor | CCI顺势指标因子 |
| wr | WrFactor |威廉指标超买超卖因子 |
| atr | AtrFactor | 真实波动幅度均值 |
| dmi | DmiFactor | 趋向指标动向因子 |
| emv | EmvFactor | 简易波动指标 |
| asi | AsiFactor | 振动升降指标 |
| roc | RocFactor | 变动率指标 |
| psy | PsyFactor | 心理线指标 |
| mtm | MtmFactor | 动量指标 |
| ma5_bias | MA5BiasFactor | 5日均线偏离度 |
| atr | AtrFactor | 真实波幅均值 |

### A.2 量价因子 (10个)

| 因子名称 | 类名 | 描述 |
|----------|------|------|
| volume_ratio | VolumeRatioFactor | 量比因子 |
| turnover | TurnoverFactor | 换手率因子 |
| mfi | MfiFactor | 资金流量指标 |
| obv | ObvFactor | 能量潮指标 |
| vr | VrFactor | 成交量变异率 |
| vosc | VoscFactor | 成交量振荡器 |
| wvad | WvadFactor | 威廉变异离散量 |
| vma | VmaFactor | 成交量移动平均 |
| v_ratio | VRatio10Factor | 早盘量能比 |
| **mainforce_resonance** | MainForceResonanceFactor | **主力痕迹共振因子(0-100)** |

### A.3 市场情绪因子 (7个)

| 因子名称 | 类名 | 描述 |
|----------|------|------|
| market_temperature | MarketTemperatureFactor | 市场温度指标 |
| market_sentiment | MarketSentimentFactor | 市场情绪综合指标 |
| market_trend | MarketTrendFactor | 大盘指数趋势强度 |
| market_breadth | MarketBreadthFactor | 市场涨跌家数对比 |
| limit_up_score | LimitUpScoreFactor | 涨停-跌停+连板高度 |
| pioneer_status | PioneerStatusFactor | 核心领涨股涨跌幅 |
| cost_peak | CostPeakFactor | 筹码分布峰值位置 |

### 因子使用示例

```python
from core.factor_engine import FactorEngine
from factors.volume_price.mainforce_resonance import MainForceResonanceFactor

# 初始化引擎
engine = FactorEngine()

# 添加主力共振因子
engine.add_factor(MainForceResonanceFactor())

# 计算因子
result = engine.calculate(stock_data)

# 筛选高共振股票 (得分>80)
high_resonance = result.filter(pl.col("mainforce_resonance") > 80)
```

---

## 附录B：过滤器完整列表

### B.1 流动性过滤器 (4个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| volume_ratio_filter | VolumeRatioFilter | 量比过滤，排除极度缩量 |
| turnover_rate_filter | TurnoverRateFilter | 换手率过滤 |
| continuous_low_turnover_filter | ContinuousLowTurnoverFilter | 连续低换手过滤 |
| volume_stability_filter | VolumeStabilityFilter | 成交量稳定性过滤 |

### B.2 估值过滤器 (3个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| valuation_filter | ValuationFilter | PE/PB估值过滤 |
| price_range_filter | PriceRangeFilter | 价格区间过滤 |
| float_market_cap_filter | FloatMarketCapFilter | 流通市值过滤 |

### B.3 市场过滤器 (4个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| market_cap_filter | MarketCapFilter | 总市值过滤 |
| price_filter | PriceFilter | 股价过滤 |
| suspension_filter | SuspensionFilter | 停牌过滤 |

### B.4 技术过滤器 (4个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| ma_position_filter | MAPositionFilter | MA位置过滤 |
| macd_cross_filter | MACDCrossFilter | MACD金叉死叉 |
| monthly_ma_filter | MonthlyMAFilter | 月线MA过滤 |
| trend_filter | TrendFilter | 趋势过滤 |

### B.5 模式过滤器 (6个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| limit_up_trap_filter | LimitUpTrapFilter | 涨停陷阱过滤 |
| limit_up_after_filter | LimitUpAfterFilter | 涨停后回调过滤 |
| pullback_signal_filter | PullbackSignalFilter | 反弹信号过滤 |
| over_hyped_filter | OverHypedFilter | 过度炒作过滤 |
| institution_signal_filter | InstitutionSignalFilter | 机构信号过滤 |

### B.6 基本面过滤器 (3个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| performance_filter | PerformanceFilter | 业绩过滤 |
| illegal_filter | IllegalFilter | 违规过滤 |
| market_crash_filter | MarketCrashFilter | 市场崩盘过滤 |

### B.7 风控过滤器 (3个)

| 过滤器名称 | 类名 | 描述 |
|------------|------|------|
| st_filter | STFilter | ST股票过滤 |
| new_stock_filter | NewStockFilter | 次新股过滤 |
| delisting_filter | DelistingFilter | 退市风险过滤 |

---

## 附录C：多流程选股系统

### C.1 三种选股流程

系统支持三种并行选股流程，通过对比分析选择最优方案：

| 流程 | 名称 | 特点 | 适用场景 |
|------|------|------|----------|
| A | 过滤优先 (conservative) | 先过滤后评分，严谨稳健 | 市场不明朗时 |
| B | 因子优先 (balanced_factor) | 全量因子计算，均衡配置 | 正常市场环境 |
| C | 信号优先 (aggressive_signal) | 纯主力共振信号驱动 | 强势市场追涨 |

### C.2 流程架构

```
┌─────────────────────────────────────────────────────────┐
│                    多流程选股系统                         │
├─────────────────────────────────────────────────────────┤
│  流程A: 过滤优先                                          │
│    1. 读取K线数据                                         │
│    2. 过滤: 涨跌停/停牌/低流动性/科创创业                    │
│    3. 计算主力共振信号                                      │
│    4. 综合评分: 因子40% + 共振信号60%                       │
│    5. 选股: 取评分前30只                                   │
├─────────────────────────────────────────────────────────┤
│  流程B: 因子优先                                          │
│    1. 读取K线数据（不预过滤）                               │
│    2. 计算全量因子（技术指标+量价+市场情绪）                 │
│    3. 综合评分: 多因子加权平均                              │
│    4. 选股: 取评分前30只                                   │
├─────────────────────────────────────────────────────────┤
│  流程C: 信号优先                                          │
│    1. 读取K线数据                                         │
│    2. 计算主力共振信号（S+/A/B级）                          │
│    3. 过滤: 剔除不满足买入条件的                            │
│    4. 选股: S+全取 + A取前50% + B取前20%                    │
└─────────────────────────────────────────────────────────┘
```

### C.3 对比分析器

`flow_comparator.py` 追踪持仓表现，计算：

| 指标 | 说明 |
|------|------|
| total_picks | 总选股数 |
| closed_picks | 已平仓数 |
| win_count | 盈利次数 |
| loss_count | 亏损次数 |
| win_rate | 胜率 |
| avg_return | 平均收益 |
| max_return | 最大收益 |
| min_return | 最小收益 |

### C.4 使用方法

```bash
# 运行多流程选股
python scripts/pipeline/stock_pick_multi.py

# 运行对比分析
python scripts/pipeline/stock_pick_multi.py --compare

# 查看报告
cat data/reports/compare_YYYYMMDD_HHMMSS.md
```

---

## 附录D：过滤与因子组合指南

### D.1 组合原则

```
Filters (过滤) ──► Factors (评分) ──► 选股
   排除问题股         计算排名       取Top N
```

**Filters** 负责**过滤** - 排除不符合条件的股票（降低候选池）
**Factors** 负责**评分** - 对通过过滤的股票进行综合评分（排名）

### D.2 推荐组合顺序

```python
# 推荐顺序：先过滤后评分
engine.apply_filters(df, filter_names=[
    "suspension_filter",    # 1. 停牌先排除
    "st_filter",            # 2. ST股排除
    "liquidity_filter",     # 3. 流动性过滤
    "valuation_filter",     # 4. 估值过滤
    "limit_up_trap_filter", # 5. 涨停陷阱过滤
])
```

### D.3 预设模式

过滤器支持多种预设模式：

```yaml
# valuation_filter.yaml
presets:
  conservative:  # 保守 - 估值要求严格
    max_pe: 30
    max_pb: 3
  standard:      # 标准
    max_pe: 100
    max_pb: 10
  aggressive:    # 激进 - 估值宽松
    max_pe: 200
    max_pb: 20
```

```python
# 使用预设
engine = FilterEngine(preset="conservative")  # 保守
engine = FilterEngine(preset="aggressive")    # 激进
```

### D.4 因子权重调整

```python
# 在 factors/volume_price/mainforce_resonance.py 中调整
S1_weight = 0.25  # 涨停信号权重
S2_weight = 0.25  # 缺口信号权重
S3_weight = 0.25  # 连阳信号权重
S4_weight = 0.25  # 放量信号权重
```

### D.5 组合示例

```python
from filters.filter_engine import FilterEngine
from core.factor_engine import FactorEngine

# 步骤1: 过滤
filter_engine = FilterEngine(preset="standard")
filtered_stocks = filter_engine.apply_filters(all_stocks)

# 步骤2: 因子计算
factor_engine = FactorEngine()
scored_stocks = factor_engine.calculate_all_factors(filtered_stocks)

# 步骤3: 选股
top_stocks = scored_stocks.sort("综合评分", descending=True).head(30)
```

---

## 附录E：参数调优指南

### E.1 过滤器参数调优

| 参数 | 保守值 | 标准值 | 激进值 | 调整建议 |
|------|--------|--------|--------|----------|
| max_pe | 30 | 100 | 200 | 牛市调高，熊市调低 |
| max_pb | 3 | 10 | 20 | 成长股调高，价值股调低 |
| min_volume | 5000万 | 1000万 | 500万 | 流动性要求 |
| min_turnover | 3% | 1% | 0.5% | 交易活跃度要求 |

### E.2 因子权重调优

| 因子 | 趋势市场 | 震荡市场 | 调整原则 |
|------|----------|----------|----------|
| ma_trend | 0.30 | 0.15 | 趋势明确时加重 |
| macd | 0.20 | 0.25 | 震荡时靠摆动指标 |
| volume_ratio | 0.15 | 0.20 | 放量时加重 |
|主力共振 | 0.35 | 0.40 | 信号明确时加重 |

### E.3 流程选择建议

| 市场环境 | 推荐流程 | 原因 |
|----------|----------|------|
| 强势上涨 | C (信号优先) | 抓住主升浪 |
| 震荡整理 | B (因子优先) | 均衡配置 |
| 方向不明 | A (过滤优先) | 控制风险 |
| 弱势下跌 | A (过滤优先) | 严格风控 |

### E.4 调优周期

- **短期调整 (1-2周)**: 根据市场温度调整过滤参数
- **中期调整 (1个月)**: 根据因子表现调整权重
- **长期跟踪 (季度)**: 评估流程表现，淘汰低效流程
