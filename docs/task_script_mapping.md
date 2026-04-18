# 定时任务与脚本映射表

> 本文档由 `config/cron_tasks.yaml` 自动生成，是唯一的任务配置来源

## 总览

- **总任务数**: 37个
- **启用任务**: 37个
- **配置文件**: `config/cron_tasks.yaml`
- **调度器**: `scripts/apscheduler_enhanced.py` (从配置文件读取)

---

## 任务列表

### 一、盘后下午任务组 (14个) - 16:00-19:00

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 超时 | 依赖 |
|------|----------|----------|----------|------|------|
| 1 | data_fetch | 16:00 | scripts/pipeline/data_collect.py | 3600s | - |
| 2 | data_fetch_retry | 16:30 | scripts/pipeline/data_collect.py --retry | 3600s | data_fetch |
| 3 | data_fetch_retry2 | 17:30 | scripts/pipeline/data_collect.py --retry | 3600s | data_fetch_retry |
| 4 | data_freshness_check | 16:50 | scripts/pipeline/check_data_freshness.py --threshold 0.85 | 300s | data_fetch |
| 5 | data_quality_check | 17:00 | scripts/pipeline/data_audit.py | 600s | data_freshness_check |
| 6 | calculate_cvd | 17:15 | scripts/calculate_cvd.py | 600s | data_quality_check |
| 7 | market_review | 17:30 | scripts/pipeline/daily_review.py | 900s | data_quality_check |
| 8 | picks_review | 17:45 | scripts/pipeline/stock_pick.py | 600s | market_review |
| 9 | daily_selection_review | 18:10 | scripts/pipeline/daily_stock_selection_review.py --all | 1800s | picks_review |
| 10 | review_report | 18:00 | scripts/send_review_report.py | 600s | market_review |
| 11 | review_brief | 19:00 | scripts/pipeline/morning_push.py | 300s | review_report |
| 12 | update_tracking | 19:30 | scripts/pipeline/update_tracking.py | 600s | review_report |
| 13 | update_index_data | 16:05 | scripts/pipeline/fetch_index.py | 600s | data_fetch |
| 14 | evening_monitoring_summary | 18:05 | scripts/monitoring_dashboard.py --email | 60s | - |

### 二、盘后夜间任务组 (3个) - 20:00-21:00

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 超时 | 依赖 |
|------|----------|----------|----------|------|------|
| 15 | precompute | 20:00 | scripts/pipeline/precompute.py | 1200s | data_quality_check |
| 16 | night_analysis | 20:30 | scripts/pipeline/night_picks.py | 900s | precompute |
| 17 | manual_verification | 20:30 | scripts/manual_verification.py | 600s | data_quality_check |

### 三、盘前上午任务组 (14个) - 08:30-09:30

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 超时 | 依赖 |
|------|----------|----------|----------|------|------|
| 18 | morning_data | 08:30 | scripts/pipeline/morning_update.py | 600s | - |
| 19 | collect_macro | 08:32 | scripts/collect_macro_data.py | 120s | - |
| 20 | collect_oil_dollar | 08:34 | scripts/collect_oil_dollar.py | 120s | - |
| 21 | collect_commodities | 08:36 | scripts/collect_commodities.py | 120s | - |
| 22 | collect_sentiment | 08:38 | scripts/collect_sentiment.py | 120s | - |
| 23 | collect_news | 08:40 | scripts/collect_news.py | 120s | - |
| 24 | market_analysis | 08:42 | scripts/market_analysis.py | 120s | morning_data |
| 25 | morning_report | 08:45 | scripts/pipeline/send_morning_shao.py | 300s | collect_news |
| 26 | fund_behavior_resource_prepare | 09:15 | scripts/pipeline/fund_behavior_resource_guardian.py --phase prepare | 420s | morning_data |
| 27 | fund_behavior_resource_validate | 09:22 | scripts/pipeline/fund_behavior_resource_guardian.py --phase validate | 120s | fund_behavior_resource_prepare |
| 28 | fund_behavior_guardian_check | 09:24 | scripts/core_task_guardian.py check | 60s | fund_behavior_resource_validate |
| 29 | fund_behavior_report | 09:26 | scripts/core_task_guardian.py full | 900s | fund_behavior_guardian_check |
| 30 | morning_monitoring_summary | 08:55 | scripts/monitoring_dashboard.py --email | 60s | - |
| 31 | data_integrity_check | 09:05 | scripts/data_integrity_check.py | 300s | fund_behavior_report |

### 四、每日凌晨任务组 (3个)

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 超时 | 依赖 |
|------|----------|----------|----------|------|------|
| 32 | collect_news_cctv_midnight | 00:22 | scripts/pipeline/cctv_analyzer.py --mode full --fetch-only | 300s | - |
| 33 | collect_news_cctv_morning | 06:22 | scripts/pipeline/cctv_analyzer.py --mode supplement --fetch-only | 300s | - |
| 34 | weekly_multi_period_update | 周六10:00 | scripts/pipeline/daily_stock_selection_review.py --update-multi-period --days-back 90 | 3600s | - |

### 五、系统维护任务组 (3个)

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 超时 | 依赖 |
|------|----------|----------|----------|------|------|
| 35 | scheduler_watchdog | 每5分钟 | scripts/scheduler_watchdog.sh | 60s | - |
| 36 | cache_cleanup | 03:00 | scripts/cache_monitor.py | 300s | - |
| 37 | generate_dashboard | 每10分钟 | scripts/monitoring_dashboard.py | 60s | - |

---

## 关键脚本说明

### 核心脚本
| 脚本 | 功能 | 被调用任务 |
|------|------|-----------|
| data_collect.py | 数据采集 | data_fetch, data_fetch_retry, data_fetch_retry2 |
| core_task_guardian.py | 09:26核心任务守护 | fund_behavior_guardian_check, fund_behavior_report |
| fund_behavior_resource_guardian.py | 资源保障 | fund_behavior_resource_prepare, fund_behavior_resource_validate |

### 报告脚本
| 脚本 | 功能 | 被调用任务 |
|------|------|-----------|
| send_review_report.py | 复盘报告 | review_report |
| send_morning_shao.py | 晨前哨报 | morning_report |
| morning_push.py | 复盘快报 | review_brief |

### 监控脚本
| 脚本 | 功能 | 被调用任务 |
|------|------|-----------|
| monitoring_dashboard.py | 监控面板 | morning_monitoring_summary, evening_monitoring_summary, generate_dashboard |
| cache_monitor.py | 缓存清理 | cache_cleanup |
| data_integrity_check.py | 数据完整性 | data_integrity_check |

---

## 配置规范

### 添加新任务
在 `config/cron_tasks.yaml` 的 `tasks` 列表中添加：

```yaml
- name: "task_name"           # 唯一标识
  description: "任务描述"      # 说明
  schedule: "0 16 * * 1-5"    # cron表达式
  script: "scripts/xxx.py"    # 脚本路径
  enabled: true               # 是否启用
  day_type: "weekday"         # weekday/daily/any
  timeout: 600                # 超时时间(秒)
  depends_on: "other_task"    # 依赖任务(可选)
```

### 修改任务
直接编辑 `config/cron_tasks.yaml`，调度器会自动加载最新配置。

---

## 注意事项

1. **配置唯一来源**: 所有任务配置必须在 `cron_tasks.yaml` 中定义
2. **脚本路径**: 相对于项目根目录的相对路径
3. **依赖关系**: `depends_on` 仅用于文档说明，实际执行顺序由cron时间控制
4. **超时设置**: 根据任务实际执行时间合理设置，避免过长或过短
