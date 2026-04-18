# 定时任务系统评估报告

## 执行摘要

经过深入代码分析，发现调度系统存在**严重的重复和配置不一致问题**。

## 关键发现

### 1. 配置不一致（严重问题）

**apscheduler_enhanced.py** 和 **apscheduler_backup.py** 都**硬编码了任务列表**，而不是从 `cron_tasks.yaml` 读取：

```python
# apscheduler_enhanced.py 第550行起
jobs = [
    {
        'id': 'collect_news_cctv_midnight',
        'script': 'scripts/pipeline/cctv_analyzer.py',
        'name': '新闻联播采集-主采集',
        'cron': '22 0 * * *',
        'args': ['--mode', 'full', '--fetch-only']
    },
    ...
]
```

**后果**：修改 `cron_tasks.yaml` 不会生效，调度器执行的是硬编码的任务列表！

### 2. 任务列表差异

对比 `cron_tasks.yaml` 和 `apscheduler_enhanced.py` 的任务：

| 任务 | cron_tasks.yaml | apscheduler_enhanced | 状态 |
|------|----------------|---------------------|------|
| data_freshness_check | ✅ 有 | ❌ 无 | **缺失** |
| data_fetch_retry2 | ✅ 有 | ❌ 无 | **缺失** |
| calculate_cvd | ✅ 有 | ❌ 无 | **缺失** |
| review_report | ✅ 有 | ❌ 无 | **缺失** |
| review_brief | ✅ 有 | ❌ 无 | **缺失** |
| update_tracking | ✅ 有 | ❌ 无 | **缺失** |
| precompute | ✅ 有 | ❌ 无 | **缺失** |
| night_analysis | ✅ 有 | ❌ 无 | **缺失** |
| manual_verification | ✅ 有 | ❌ 无 | **缺失** |
| scheduled_tasks | ❌ 无 | ✅ 有 | **多余** |

**结论**：当前生产环境可能只执行了部分任务！

### 3. 调度脚本功能对比

| 脚本 | 功能 | 状态 | 建议 |
|------|------|------|------|
| `apscheduler_enhanced.py` | 主调度器（在用） | ⚠️ 硬编码任务 | **需要修复** |
| `apscheduler_backup.py` | 备份调度器 | ⚠️ 硬编码任务 | 可归档 |
| `backup_scheduler.py` | Cron备用方案 | ⚠️ 硬编码任务 | 可归档 |
| `cron_task_manager.py` | 从YAML读取配置 | ✅ 正确 | **应该使用** |
| `run_scheduler.py` | 启动脚本 | ❓ 调用服务层 | 需评估 |
| `run_daily_scheduler.py` | 每日调度器 | ❓ 调用服务层 | 需评估 |

## 建议方案

### 方案A：最小修复（推荐立即执行）

1. **修复 `apscheduler_enhanced.py`**：改为从 `cron_tasks.yaml` 读取任务
2. **归档其他调度脚本**：避免混淆
3. **验证所有任务执行**：确保没有遗漏

### 方案B：重构整合（长期优化）

1. 创建统一的调度器模块
2. 所有调度器共用配置读取逻辑
3. 单一入口，备份方案通过配置切换

## 风险评估

| 风险 | 等级 | 说明 |
|------|------|------|
| 任务遗漏 | 🔴 高 | 当前可能只执行了约60%的任务 |
| 配置失效 | 🔴 高 | 修改YAML不生效 |
| 维护困难 | 🟡 中 | 多个调度器代码重复 |

## 立即行动建议

1. **检查生产环境**：确认当前实际执行的任务列表
2. **修复配置读取**：优先修复 `apscheduler_enhanced.py`
3. **补执行遗漏任务**：手动执行可能遗漏的任务
