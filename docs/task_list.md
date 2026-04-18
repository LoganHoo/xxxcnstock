# 定时任务清单

> 生成时间: 2026-04-17
> 配置文件: `config/cron_tasks.yaml`

## 总览

- **总任务数**: 37个
- **配置文件**: `config/cron_tasks.yaml`
- **调度器**: `scripts/apscheduler_enhanced.py`

---

## 一、盘后下午任务 (14个) - 16:00-19:00

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 参数 | 超时 |
|------|----------|----------|----------|------|------|
| 1 | data_fetch | 16:00 | scripts/pipeline/data_collect.py | - | 3600s |
| 2 | data_fetch_retry | 16:30 | scripts/pipeline/data_collect.py | --retry | 3600s |
| 3 | data_fetch_retry2 | 17:35 | scripts/pipeline/data_collect.py | --retry | 3600s |
| 4 | data_freshness_check | 16:50 | scripts/pipeline/check_data_freshness.py | --threshold 0.85 | 300s |
| 5 | data_quality_check | 17:00 | scripts/pipeline/data_audit.py | - | 600s |
| 6 | calculate_cvd | 17:15 | scripts/calculate_cvd.py | - | 600s |
| 7 | market_review | 17:30 | scripts/pipeline/daily_review.py | - | 900s |
| 8 | picks_review | 17:45 | scripts/pipeline/stock_pick.py | - | 600s |
| 9 | daily_selection_review | 18:10 | scripts/pipeline/daily_stock_selection_review.py | --all | 1800s |
| 10 | review_report | 18:00 | scripts/send_review_report.py | - | 600s |
| 11 | review_brief | 19:00 | scripts/pipeline/morning_push.py | - | 300s |
| 12 | update_tracking | 19:30 | scripts/pipeline/update_tracking.py | - | 600s |
| 13 | update_index_data | 16:05 | scripts/pipeline/fetch_index.py | - | 600s |
| 14 | evening_monitoring_summary | 18:05 | scripts/monitoring_dashboard.py | --email | 60s |

---

## 二、盘后夜间任务 (3个) - 20:00-21:00

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 参数 | 超时 |
|------|----------|----------|----------|------|------|
| 15 | precompute | 20:00 | scripts/pipeline/precompute.py | - | 1200s |
| 16 | night_analysis | 20:35 | scripts/pipeline/night_picks.py | - | 900s |
| 17 | manual_verification | 20:30 | scripts/manual_verification.py | - | 600s |

---

## 三、盘前上午任务 (14个) - 08:30-09:30

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 参数 | 超时 |
|------|----------|----------|----------|------|------|
| 18 | morning_data | 08:30 | scripts/pipeline/morning_update.py | - | 600s |
| 19 | collect_macro | 08:32 | scripts/collect_macro_data.py | - | 120s |
| 20 | collect_oil_dollar | 08:34 | scripts/collect_oil_dollar.py | - | 120s |
| 21 | collect_commodities | 08:36 | scripts/collect_commodities.py | - | 120s |
| 22 | collect_sentiment | 08:38 | scripts/collect_sentiment.py | - | 120s |
| 23 | collect_news | 08:40 | scripts/collect_news.py | - | 120s |
| 24 | market_analysis | 08:42 | scripts/market_analysis.py | - | 120s |
| 25 | morning_report | 08:45 | scripts/pipeline/send_morning_shao.py | - | 300s |
| 26 | fund_behavior_resource_prepare | 09:15 | scripts/pipeline/fund_behavior_resource_guardian.py | --phase prepare | 420s |
| 27 | fund_behavior_resource_validate | 09:22 | scripts/pipeline/fund_behavior_resource_guardian.py | --phase validate | 120s |
| 28 | fund_behavior_guardian_check | 09:24 | scripts/core_task_guardian.py | check | 60s |
| 29 | fund_behavior_report | 09:26 | scripts/core_task_guardian.py | full | 900s |
| 30 | morning_monitoring_summary | 08:55 | scripts/monitoring_dashboard.py | --email | 60s |
| 31 | data_integrity_check | 09:05 | scripts/data_integrity_check.py | - | 300s |

---

## 四、每日凌晨任务 (3个)

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 参数 | 超时 |
|------|----------|----------|----------|------|------|
| 32 | collect_news_cctv_midnight | 00:22 | scripts/pipeline/cctv_analyzer.py | --mode full --fetch-only | 300s |
| 33 | collect_news_cctv_morning | 06:22 | scripts/pipeline/cctv_analyzer.py | --mode supplement --fetch-only | 300s |
| 34 | weekly_multi_period_update | 周六10:00 | scripts/pipeline/daily_stock_selection_review.py | --update-multi-period --days-back 90 | 3600s |

---

## 五、系统维护任务 (3个)

| 序号 | 任务名称 | 执行时间 | 脚本路径 | 参数 | 超时 |
|------|----------|----------|----------|------|------|
| 35 | scheduler_watchdog | 每5分钟 | scripts/scheduler_watchdog.sh | - | 60s |
| 36 | cache_cleanup | 03:00 | scripts/cache_monitor.py | - | 300s |
| 37 | generate_dashboard | 每10分钟 | scripts/monitoring_dashboard.py | - | 60s |

---

## 脚本索引

### 按脚本分组

| 脚本 | 任务数 | 任务列表 |
|------|--------|----------|
| scripts/pipeline/data_collect.py | 3 | data_fetch, data_fetch_retry, data_fetch_retry2 |
| scripts/monitoring_dashboard.py | 3 | generate_dashboard, morning_monitoring_summary, evening_monitoring_summary |
| scripts/pipeline/fund_behavior_resource_guardian.py | 2 | fund_behavior_resource_prepare, fund_behavior_resource_validate |
| scripts/core_task_guardian.py | 2 | fund_behavior_guardian_check, fund_behavior_report |
| scripts/pipeline/cctv_analyzer.py | 2 | collect_news_cctv_midnight, collect_news_cctv_morning |
| scripts/pipeline/daily_stock_selection_review.py | 2 | daily_selection_review, weekly_multi_period_update |
| scripts/pipeline/check_data_freshness.py | 1 | data_freshness_check |
| scripts/pipeline/data_audit.py | 1 | data_quality_check |
| scripts/calculate_cvd.py | 1 | calculate_cvd |
| scripts/pipeline/daily_review.py | 1 | market_review |
| scripts/pipeline/stock_pick.py | 1 | picks_review |
| scripts/send_review_report.py | 1 | review_report |
| scripts/pipeline/morning_push.py | 1 | review_brief |
| scripts/pipeline/precompute.py | 1 | precompute |
| scripts/pipeline/night_picks.py | 1 | night_analysis |
| scripts/pipeline/morning_update.py | 1 | morning_data |
| scripts/collect_macro_data.py | 1 | collect_macro |
| scripts/collect_oil_dollar.py | 1 | collect_oil_dollar |
| scripts/collect_commodities.py | 1 | collect_commodities |
| scripts/collect_sentiment.py | 1 | collect_sentiment |
| scripts/collect_news.py | 1 | collect_news |
| scripts/market_analysis.py | 1 | market_analysis |
| scripts/pipeline/send_morning_shao.py | 1 | morning_report |
| scripts/pipeline/fetch_index.py | 1 | update_index_data |
| scripts/scheduler_watchdog.sh | 1 | scheduler_watchdog |
| scripts/pipeline/update_tracking.py | 1 | update_tracking |
| scripts/manual_verification.py | 1 | manual_verification |
| scripts/cache_monitor.py | 1 | cache_cleanup |
| scripts/data_integrity_check.py | 1 | data_integrity_check |

---

## 任务分组

| 分组 | 任务数 | 说明 |
|------|--------|------|
| pre_market_morning | 14 | 盘前上午（08:30-09:30）|
| post_market_afternoon | 14 | 盘后下午（16:00-19:00）|
| post_market_evening | 3 | 盘后夜间（20:00-21:00）|
| resource_guarantee | 3 | 09:26核心任务资源保障链 |
| daily | 3 | 每日凌晨任务 |
| system_maintenance | 3 | 系统维护和监控 |
