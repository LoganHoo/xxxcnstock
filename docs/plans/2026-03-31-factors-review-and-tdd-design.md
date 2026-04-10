# Factors Review And TDD Design

**目标**

对 `factors/` 目录做逐文件评审，以业务功能为主补齐核心测试，并通过 `FactorEngine` 集成验收确认市场因子、技术因子、量价因子的主路径可用、缺列安全、边界稳定。

**范围**

- `factors/market/`
  - `cost_peak.py`
  - `emotion_factors.py`
  - `market_breadth.py`
  - `market_sentiment.py`
  - `market_temperature.py`
  - `market_trend.py`
- `factors/technical/`
  - `asi.py`
  - `atr.py`
  - `bollinger.py`
  - `cci.py`
  - `dmi.py`
  - `emv.py`
  - `kdj.py`
  - `ma5_bias.py`
  - `ma_trend.py`
  - `macd.py`
  - `mtm.py`
  - `psy.py`
  - `roc.py`
  - `rsi.py`
  - `wr.py`
- `factors/volume_price/`
  - `mfi.py`
  - `obv.py`
  - `turnover.py`
  - `v_ratio.py`
  - `vma.py`
  - `volume_ratio.py`
  - `vosc.py`
  - `vr.py`
  - `wvad.py`
- 集成入口
  - `core/factor_engine.py`
  - `core/factor_library.py`

**设计原则**

- 先红后绿：每一类因子先写失败测试，再做最小修复
- 业务优先：每个文件只验证核心行为，不追求公式所有细枝末节
- 安全退化：缺列、窗口不足、零分母、空数据时不得直接炸链路
- 输出统一：所有因子都必须生成 `factor_<name>` 列，必要时附带中间列
- 集成兜底：`FactorEngine` 负责加载、注册、单因子计算、多因子组合计算和未知因子降级

**测试分层**

- 文件级单测
  - `market`：验证因子列生成、聚合行为、缺列回退、窗口不足
  - `technical`：验证趋势/震荡/超买超卖等代表性逻辑
  - `volume_price`：验证量比、换手、资金流向、均量/振荡器的代表性行为
- 引擎级集成验收
  - `list_factors()`、`get_factor_info()`、`calculate_factor()`、`calculate_all_factors()`
  - 分类筛选、配置加载、未知因子安全退化
  - 使用一份标准样本数据串联多类因子计算

**主要风险点**

- 某些因子直接读固定列，缺列时可能抛异常而不是安全返回
- 某些实现只看最后两行或最后一行，可能忽略多股票场景
- `FactorEngine._find_factor_module()` 与配置类别耦合，可能出现配置存在但模块未命中的问题
- 滚动窗口与除零计算可能在 `null` / `0` 情况下产生不稳定结果

**验收标准**

- 每一类因子至少有一组正式 pytest 用例覆盖核心业务行为
- `FactorEngine` 集成测试通过
- 全量 pytest 通过
- `python -m ruff check factors tests core` 通过
- 输出一份逐文件 review 结论，说明职责、风险、测试证据与剩余技术债
