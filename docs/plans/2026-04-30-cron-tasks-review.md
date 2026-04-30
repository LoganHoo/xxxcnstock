# Cron Tasks Review

## 当前可执行任务

- 当前 `tasks` 执行面共 `44` 个任务，仍由 `scripts/scheduler.py` 直接消费
- 盘前与盘后系统检查：`system_health_check`、`system_health_check_post`
- 数据采集主链路：`data_fetch`、`data_fetch_retry`、`data_audit_unified`、`calculate_cvd`
- 涨停系统链路：`limitup_data_collect`、`fund_flow_collect`、`dragon_tiger_fetch`、`dragon_tiger_collect`、`afternoon_limit_up`、`limitup_daily_review`
- 复盘与报告链路：`market_review`、`picks_review`、`daily_selection_review`、`drawdown_analysis`、`review_report`、`review_brief`
- 晚间分析链路：`precompute`、`night_analysis`、`manual_verification`
- 晨间准备链路：`morning_data`、`collect_macro`、`collect_oil_dollar`、`collect_commodities`、`collect_sentiment`、`collect_news`、`prediction_correction_check`、`market_analysis`、`morning_report`
- 核心决策链路：`fund_behavior_resource_prepare`、`fund_behavior_resource_validate`、`fund_behavior_guardian_check`、`fund_behavior_report`、`morning_limit_up`
- 每日/周末补充链路：`collect_news_cctv_midnight`、`collect_news_cctv_morning`、`analyze_cctv_news`、`update_index_data`、`update_tracking`、`cache_cleanup`、`weekly_multi_period_update`、`weekly_champion_report`、`data_integrity_check`

## 规划态任务

- `planned_tasks` 当前共 `2` 个任务：`kline_pre_market`、`kline_post_market`
- 两个任务均已显式标记 `enabled: false`
- 两个任务均已显式标记 `status: planned`
- 进入规划态的原因一致：当前仓库不存在 `scripts/pipeline/kline_collect.py`，不应继续留在执行面

## 高风险问题

- `groups` 仍包含阶段叙事与未消费元数据，容易让维护者误以为分组会直接驱动调度
- `priority`、`optional` 等字段目前仍是评审性元数据，`scripts/scheduler.py` 并不消费
- 评审文档与主配置需要持续同步；若后续有人把规划态任务重新塞回 `tasks`，会直接破坏当前执行契约

## 已修复问题

- 启用任务脚本存在性已通过测试锁定，不再允许 `enabled: true` 指向缺失脚本
- 两个未接线 K 线任务已从 `tasks` 执行面移出，避免调度器加载后形成假可执行状态
- `planned_tasks` 结构已建立，规划态任务与执行态任务有了物理隔离
- 新增 `tests/unit/test_cron_tasks_config.py`，覆盖脚本存在性、依赖闭合、规划态边界与执行契约
