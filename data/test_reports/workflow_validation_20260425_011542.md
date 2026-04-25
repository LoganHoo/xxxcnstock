# 工作流验证报告

验证时间: 2026-04-25 01:15:42

## 摘要

- 总检查项: 5
- 通过: 4 ✅
- 失败: 1 ❌
- 成功率: 80.0%

## 详细结果

### ❌ 数据新鲜度

- total_files: 5019
- sample_size: 100
- latest_count: 0
- freshness_rate: 0.0
- target_date: 2026-04-25

### ✅ 数据完整性

- checked: 50
- valid: 50
- invalid: 0

### ✅ 因子计算

- indicators_tested: ['EMA', 'MACD', 'RSI']
- total: 3
- passed: 3

### ✅ 选股策略

- configs: {'existing': ['config/strategies/champion.yaml', 'config/factors_config.yaml', 'config/filters_config.yaml'], 'missing': []}
- scripts: {'existing': []}

### ✅ 报告生成

- report_dirs: ['data/reports', 'data/test_reports']
- test_report_generated: True
- test_report_path: data/test_reports/workflow_validation_test.json

## 结论

⚠️ **部分检查项未通过，工作流基本可用但需关注**
