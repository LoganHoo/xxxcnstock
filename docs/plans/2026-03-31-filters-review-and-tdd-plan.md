# Filters Review And TDD Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 以业务功能为主完成 `filters/` 目录逐文件评审、核心路径 TDD 测试补齐、引擎级集成验收，并修复发现的缺陷。

**Architecture:** 先审查 `BaseFilter`、注册表和 `FilterEngine` 的基础契约，再围绕股票属性、市场、基本面、流动性、技术、估值、形态 6 类过滤器建立最小但有效的行为测试。测试分为“核心过滤规则单测”和“FilterEngine 组合执行验收”两层，避免追求边角穷举但保证业务主路径可信。

**Tech Stack:** Python 3.11+, pytest, Polars, 项目内 FilterRegistry / FilterEngine / filter_config_loader

---

### Task 1: 建立过滤器审查基线

**Files:**
- Modify: `tests/test_filter.py`
- Test: `tests/test_filter.py`

**Step 1: 写失败测试**

在 `tests/test_filter.py` 中补充针对 `FilterEngine` 的高价值断言：
- 初始化后能加载核心过滤器
- `enabled_only=True` 时只返回启用过滤器
- `enable_filter/disable_filter` 会影响执行结果

**Step 2: 运行测试确认失败**

Run: `pytest tests/test_filter.py -q`
Expected: FAIL，暴露现有断言不足或行为不一致

**Step 3: 最小实现/修复**

仅修复 `filters/filter_engine.py` 中与过滤器装载、筛选、启停、查询相关的问题。

**Step 4: 运行测试确认通过**

Run: `pytest tests/test_filter.py -q`
Expected: PASS

### Task 2: 补股票属性与市场过滤器核心测试

**Files:**
- Create: `tests/test_filter_stock_market.py`
- Modify: `filters/stock_filter.py`
- Modify: `filters/market_filter.py`
- Test: `tests/test_filter_stock_market.py`

**Step 1: 写失败测试**

覆盖以下业务行为：
- `STFilter` 会剔除 ST/*ST
- `NewStockFilter` 在缺列时安全退化，有 `list_date` 时剔除新股
- `DelistingFilter` 会剔除退市风险名称
- `MarketCapFilter` 支持 `market_cap` / `total_mv`
- `SuspensionFilter` 支持 `trade_status` 和 `volume` 回退
- `PriceFilter` / `VolumeFilter` 能按阈值过滤

**Step 2: 运行测试确认失败**

Run: `pytest tests/test_filter_stock_market.py -q`
Expected: FAIL

**Step 3: 最小实现/修复**

只修改 `stock_filter.py`、`market_filter.py` 中被失败测试证实的问题。

**Step 4: 运行测试确认通过**

Run: `pytest tests/test_filter_stock_market.py -q`
Expected: PASS

### Task 3: 补基本面与流动性过滤器核心测试

**Files:**
- Create: `tests/test_filter_fundamental_liquidity.py`
- Modify: `filters/fundamental_filter.py`
- Modify: `filters/liquidity_filter.py`
- Test: `tests/test_filter_fundamental_liquidity.py`

**Step 1: 写失败测试**

覆盖以下业务行为：
- `IllegalFilter` 对 `risk_flag` / `announcement` 任一列生效
- `PerformanceFilter` 先过滤亏损，再结合 `profit_yoy`
- `MarketCrashFilter` 在指数暴跌时返回空表
- `VolumeRatioFilter`、`TurnoverRateFilter` 按范围过滤
- `VolumeStabilityFilter` 与 `ContinuousLowTurnoverFilter` 在时序数据上做整表剔除

**Step 2: 运行测试确认失败**

Run: `pytest tests/test_filter_fundamental_liquidity.py -q`
Expected: FAIL

**Step 3: 最小实现/修复**

仅修复 `fundamental_filter.py`、`liquidity_filter.py` 中的真实问题。

**Step 4: 运行测试确认通过**

Run: `pytest tests/test_filter_fundamental_liquidity.py -q`
Expected: PASS

### Task 4: 补技术、估值、形态过滤器核心测试

**Files:**
- Create: `tests/test_filter_technical_pattern_valuation.py`
- Modify: `filters/technical_filter.py`
- Modify: `filters/valuation_filter.py`
- Modify: `filters/pattern_filter.py`
- Test: `tests/test_filter_technical_pattern_valuation.py`

**Step 1: 写失败测试**

覆盖以下业务行为：
- `TrendFilter` 支持直接均线列与 `close` 回退计算
- `MaPositionFilter` 支持 `require_all` 与任意均线满足
- `MonthlyMaFilter`、`MacdCrossFilter` 的缺列退化与过滤结果
- `FloatMarketCapFilter`、`PriceRangeFilter`、`ValuationFilter` 的核心阈值行为
- `LimitUpTrapFilter`、`OverHypedFilter`、`PullbackSignalFilter`、`InstitutionSignalFilter`、`LimitUpAfterFilter` 的核心规则

**Step 2: 运行测试确认失败**

Run: `pytest tests/test_filter_technical_pattern_valuation.py -q`
Expected: FAIL

**Step 3: 最小实现/修复**

只调整与失败测试直接相关的实现。

**Step 4: 运行测试确认通过**

Run: `pytest tests/test_filter_technical_pattern_valuation.py -q`
Expected: PASS

### Task 5: 做引擎级组合验收

**Files:**
- Create: `tests/test_filter_engine_acceptance.py`
- Modify: `filters/filter_engine.py`
- Test: `tests/test_filter_engine_acceptance.py`

**Step 1: 写失败测试**

用一份综合业务数据验证：
- `apply_filters()` 会按配置执行并缩减样本
- `filter_names` 只执行指定过滤器
- `get_filter_stats()` 返回 before/after/removed
- 异常过滤器不会中断全链路

**Step 2: 运行测试确认失败**

Run: `pytest tests/test_filter_engine_acceptance.py -q`
Expected: FAIL

**Step 3: 最小实现/修复**

只修改 `filter_engine.py` 中组合执行、错误隔离、统计信息相关逻辑。

**Step 4: 运行测试确认通过**

Run: `pytest tests/test_filter_engine_acceptance.py -q`
Expected: PASS

### Task 6: 全量验证与交付审查

**Files:**
- Modify: `说明文档.md`

**Step 1: 运行过滤器相关测试**

Run: `pytest tests/test_filter.py tests/test_filter_stock_market.py tests/test_filter_fundamental_liquidity.py tests/test_filter_technical_pattern_valuation.py tests/test_filter_engine_acceptance.py -q`
Expected: PASS

**Step 2: 运行全量测试**

Run: `pytest -q`
Expected: PASS

**Step 3: 运行代码检查**

Run: `python -m ruff check filters tests`
Expected: PASS

**Step 4: 更新说明文档**

同步记录：
- filters 评审结果
- 新增测试覆盖范围
- 已修复问题与剩余技术债

**Step 5: 审查**

在交付前再次复核每个文件的职责、主要风险和测试证据。
