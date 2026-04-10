# Factors Review And TDD Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 对 `factors/` 目录做逐文件评审，并以业务功能为主完成核心因子测试、`FactorEngine` 集成验收和缺陷修复。

**Architecture:** 测试按 `market`、`technical`、`volume_price` 三类组织，每类仅覆盖核心公式行为、缺列退化、边界稳定性和标准输出列。完成单测后，再通过 `FactorEngine` 做配置加载与组合计算验收，最终做全量验证与逐文件 review。

**Tech Stack:** Python 3.11+, pytest, Polars, FactorRegistry, FactorEngine

---

### Task 1: 补市场因子核心测试

**Files:**
- Create: `tests/test_factor_market.py`
- Modify: `factors/market/cost_peak.py`
- Modify: `factors/market/emotion_factors.py`
- Modify: `factors/market/market_breadth.py`
- Modify: `factors/market/market_sentiment.py`
- Modify: `factors/market/market_temperature.py`
- Modify: `factors/market/market_trend.py`
- Test: `tests/test_factor_market.py`

**Step 1: Write the failing test**

覆盖以下行为：
- `cost_peak` 生成 `factor_cost_peak` 且窗口不足时不崩溃
- `market_sentiment` 在成交量/价格变化存在时生成情绪列，空值安全填充
- `market_trend`、`market_breadth`、`market_temperature` 对标准行情输入返回稳定列
- `emotion_factors` 的核心导出因子可被注册并计算

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_factor_market.py -q`
Expected: FAIL

**Step 3: Write minimal implementation**

只修复被测试证实的问题：
- 缺列异常
- 滚动窗口不足
- 输出列不一致
- 注册或导出失败

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_factor_market.py -q`
Expected: PASS

### Task 2: 补技术因子核心测试

**Files:**
- Create: `tests/test_factor_technical.py`
- Modify: `factors/technical/asi.py`
- Modify: `factors/technical/atr.py`
- Modify: `factors/technical/bollinger.py`
- Modify: `factors/technical/cci.py`
- Modify: `factors/technical/dmi.py`
- Modify: `factors/technical/emv.py`
- Modify: `factors/technical/kdj.py`
- Modify: `factors/technical/ma5_bias.py`
- Modify: `factors/technical/ma_trend.py`
- Modify: `factors/technical/macd.py`
- Modify: `factors/technical/mtm.py`
- Modify: `factors/technical/psy.py`
- Modify: `factors/technical/roc.py`
- Modify: `factors/technical/rsi.py`
- Modify: `factors/technical/wr.py`
- Test: `tests/test_factor_technical.py`

**Step 1: Write the failing test**

覆盖以下行为：
- `ma_trend`、`ma5_bias` 在趋势数据上生成标准列
- `macd`、`kdj`、`rsi`、`bollinger` 对窗口不足和正常输入都安全
- `atr`、`cci`、`dmi`、`wr`、`roc`、`mtm`、`psy`、`asi`、`emv` 在标准行情下生成 `factor_` 列

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_factor_technical.py -q`
Expected: FAIL

**Step 3: Write minimal implementation**

仅修复失败测试揭示的问题，不重写全部公式。

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_factor_technical.py -q`
Expected: PASS

### Task 3: 补量价因子核心测试

**Files:**
- Create: `tests/test_factor_volume_price.py`
- Modify: `factors/volume_price/mfi.py`
- Modify: `factors/volume_price/obv.py`
- Modify: `factors/volume_price/turnover.py`
- Modify: `factors/volume_price/v_ratio.py`
- Modify: `factors/volume_price/vma.py`
- Modify: `factors/volume_price/volume_ratio.py`
- Modify: `factors/volume_price/vosc.py`
- Modify: `factors/volume_price/vr.py`
- Modify: `factors/volume_price/wvad.py`
- Test: `tests/test_factor_volume_price.py`

**Step 1: Write the failing test**

覆盖以下行为：
- `volume_ratio`、`v_ratio`、`turnover` 生成正确因子列并处理零分母
- `obv`、`mfi`、`vma`、`vosc`、`vr`、`wvad` 在标准样本上可计算
- 缺列或窗口不足时不导致整链路崩溃

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_factor_volume_price.py -q`
Expected: FAIL

**Step 3: Write minimal implementation**

只修复经测试确认的问题。

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_factor_volume_price.py -q`
Expected: PASS

### Task 4: 补 FactorEngine 集成验收

**Files:**
- Create: `tests/test_factor_engine_acceptance.py`
- Modify: `core/factor_engine.py`
- Test: `tests/test_factor_engine_acceptance.py`

**Step 1: Write the failing test**

覆盖以下行为：
- `list_factors()` 可按分类筛选
- `get_factor_info()` 返回完整配置
- `calculate_factor()` 对已知因子生成目标列，对未知因子安全降级
- `calculate_all_factors()` 能串联 market/technical/volume_price 因子

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_factor_engine_acceptance.py -q`
Expected: FAIL

**Step 3: Write minimal implementation**

仅修复加载配置、模块发现、单因子/多因子组合计算中的真实问题。

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_factor_engine_acceptance.py -q`
Expected: PASS

### Task 5: 全量验证与逐文件评审

**Files:**
- Modify: `说明文档.md`

**Step 1: Run focused tests**

Run: `pytest tests/test_factor_market.py tests/test_factor_technical.py tests/test_factor_volume_price.py tests/test_factor_engine.py tests/test_factor_engine_acceptance.py tests/test_fund_behavior_system.py -q`
Expected: PASS

**Step 2: Run full tests**

Run: `pytest -q`
Expected: PASS

**Step 3: Run lint**

Run: `python -m ruff check factors tests core`
Expected: PASS

**Step 4: Update docs**

更新 `说明文档.md`：
- factors 评审结果
- 新增测试覆盖
- 修复问题与剩余技术债

**Step 5: Review**

逐文件输出：
- 职责
- 发现的问题
- 测试证据
- 是否存在剩余风险
