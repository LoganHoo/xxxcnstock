# 任务分组详细说明（含脚本对应关系）

## 总览

| 分组 | 任务数 | 执行时段 | 核心目标 |
|------|--------|----------|----------|
| pre_market_morning | 14 | 08:30-09:30 | 开盘前数据准备与核心决策 |
| post_market_afternoon | 13 | 16:00-19:00 | 收盘数据采集与复盘分析 |
| post_market_evening | 3 | 20:00-21:00 | 晚间分析与预计算 |
| resource_guarantee | 3 | 09:15-09:26 | 09:26核心任务资源保障链 |
| daily | 3 | 00:00-10:00 | 每日凌晨任务 |
| system_maintenance | 3 | 持续 | 系统维护和监控 |

---

## 一、pre_market_morning (盘前上午)

**时段**: 08:30-09:30  
**核心目标**: 开盘前完成所有数据准备，确保09:26核心决策任务顺利执行

### 任务列表

| 序号 | 任务名称 | 时间 | 脚本路径 | 功能 | 实现目标 |
|------|----------|------|----------|------|----------|
| 1 | morning_data | 08:30 | scripts/pipeline/morning_update.py | 更新隔夜外盘数据 | 获取美股、亚洲股市收盘数据 |
| 2 | collect_macro | 08:32 | scripts/collect_macro_data.py | 采集宏观数据 | 美元指数、美债、人民币汇率 |
| 3 | collect_oil_dollar | 08:34 | scripts/collect_oil_dollar.py | 采集石油美元 | 原油价格、石油美元动态 |
| 4 | collect_commodities | 08:36 | scripts/collect_commodities.py | 采集大宗商品 | 黄金、铜、碳酸锂价格 |
| 5 | collect_sentiment | 08:38 | scripts/collect_sentiment.py | 采集情绪数据 | 炸板率、恐慌贪婪指数 |
| 6 | collect_news | 08:40 | scripts/collect_news.py | 采集新闻数据 | 国内外重要财经新闻 |
| 7 | market_analysis | 08:42 | scripts/market_analysis.py | 大盘关键位分析 | 计算大盘关键位和CVD指标 |
| 8 | morning_report | 08:45 | scripts/pipeline/send_morning_shao.py | 推送晨前哨报 | 外盘+大盘+宏观+AI建议 |
| 9 | fund_behavior_resource_prepare | 09:15 | scripts/pipeline/fund_behavior_resource_guardian.py --phase prepare | 数据准备 | 预热+锁定资源 |
| 10 | fund_behavior_resource_validate | 09:22 | scripts/pipeline/fund_behavior_resource_guardian.py --phase validate | 资源验证 | 检查+快速通道 |
| 11 | fund_behavior_guardian_check | 09:24 | scripts/core_task_guardian.py check | 前置检查 | 最终数据就绪检查 |
| 12 | fund_behavior_report | 09:26 | scripts/core_task_guardian.py full | 核心决策 | 资金行为学策略执行与报告 |
| 13 | morning_monitoring_summary | 08:55 | scripts/monitoring_dashboard.py --email | 监控摘要 | 发送晨间任务监控摘要 |
| 14 | data_integrity_check | 09:05 | scripts/data_integrity_check.py | 数据完整性 | 验证核心数据表记录数 |

### 关键依赖链
```
morning_data → market_analysis
            → fund_behavior_resource_prepare → fund_behavior_resource_validate 
            → fund_behavior_guardian_check → fund_behavior_report (09:26核心)
```

---

## 二、post_market_afternoon (盘后下午)

**时段**: 16:00-19:00  
**核心目标**: 收盘后立即采集数据，完成质检后生成复盘报告

### 任务列表

| 序号 | 任务名称 | 时间 | 脚本路径 | 功能 | 实现目标 |
|------|----------|------|----------|------|----------|
| 1 | data_fetch | 16:00 | scripts/pipeline/data_collect.py | 数据采集 | 采集A股所有行情数据（日线） |
| 2 | data_fetch_retry | 16:30 | scripts/pipeline/data_collect.py --retry | 断点续传 | 补充未完成的数据 |
| 3 | data_fetch_retry2 | 17:35 | scripts/pipeline/data_collect.py --retry | 第二轮续传 | 最终数据补充 |
| 4 | data_audit_unified | 16:50 | scripts/pipeline/data_audit_unified.py --threshold 0.85 | 统一数据审计 | 新鲜度+完整性+质量检查 |
| 5 | calculate_cvd | 17:15 | scripts/calculate_cvd.py | CVD计算 | 计算60日累积成交量差指标 |
| 6 | market_review | 17:30 | scripts/pipeline/daily_review.py | 复盘分析 | 涨跌停家数、板块强度 |
| 7 | picks_review | 17:45 | scripts/pipeline/stock_pick.py | 选股复盘 | 验证昨日推荐股票表现 |
| 8 | daily_selection_review | 18:10 | scripts/pipeline/daily_stock_selection_review.py --all | 每日选股复盘 | 更新选股多周期表现 |
| 9 | review_report | 18:00 | scripts/send_review_report.py | 复盘报告 | 大盘+推荐追踪+AI复盘 |
| 10 | review_brief | 19:00 | scripts/pipeline/morning_push.py | 复盘快报 | 热点+资金流向+质检摘要 |
| 11 | update_tracking | 19:30 | scripts/pipeline/update_tracking.py | 跟踪更新 | 更新推荐股票跟踪数据 |
| 12 | update_index_data | 16:05 | scripts/pipeline/fetch_index.py | 大盘指数 | 更新上证、深证、创业板指数 |
| 13 | evening_monitoring_summary | 18:05 | scripts/monitoring_dashboard.py --email | 监控摘要 | 发送盘后任务监控摘要 |

### 关键依赖链
```
data_fetch → data_audit_unified → market_review → picks_review → daily_selection_review
                                        ↓
                                    review_report → review_brief
                                        ↓
                                    update_tracking
```

---

## 三、post_market_evening (盘后夜间)

**时段**: 20:00-21:00  
**核心目标**: 晚间深度分析，为次日交易做准备

### 任务列表

| 序号 | 任务名称 | 时间 | 脚本路径 | 功能 | 实现目标 |
|------|----------|------|----------|------|----------|
| 1 | precompute | 20:00 | scripts/pipeline/precompute.py | 预计算 | 计算技术指标评分 |
| 2 | night_analysis | 20:35 | scripts/pipeline/night_picks.py | 晚间分析 | 结合消息面的选股推荐 |
| 3 | manual_verification | 20:30 | scripts/manual_verification.py | 人工验证 | 随机抽检+第三方验证 |

### 关键依赖链
```
data_audit_unified → precompute → night_analysis
```

---

## 四、resource_guarantee (资源保障链)

**时段**: 09:15-09:24  
**核心目标**: 确保09:26核心任务有足够资源和数据准备

### 任务列表

| 序号 | 任务名称 | 时间 | 脚本路径 | 功能 | 实现目标 |
|------|----------|------|----------|------|----------|
| 1 | fund_behavior_resource_prepare | 09:15 | scripts/pipeline/fund_behavior_resource_guardian.py --phase prepare | 数据准备 | 预热缓存+锁定资源 |
| 2 | fund_behavior_resource_validate | 09:22 | scripts/pipeline/fund_behavior_resource_guardian.py --phase validate | 资源验证 | 检查就绪+快速通道 |
| 3 | fund_behavior_guardian_check | 09:24 | scripts/core_task_guardian.py check | 前置检查 | 最终数据就绪确认 |

### 保障机制
- **容错时间**: 4分钟（从09:15到09:26）
- **熔断机制**: 连续失败3次触发熔断
- **兜底脚本**: fund_behavior_fallback.py

---

## 五、daily (每日凌晨)

**时段**: 00:00-10:00（非交易时段）  
**核心目标**: 采集新闻数据，周末批量更新

### 任务列表

| 序号 | 任务名称 | 时间 | 脚本路径 | 功能 | 实现目标 |
|------|----------|------|----------|------|----------|
| 1 | collect_news_cctv_midnight | 00:22 | scripts/pipeline/cctv_analyzer.py --mode full --fetch-only | 新闻采集 | 采集昨天和今天新闻联播 |
| 2 | collect_news_cctv_morning | 06:22 | scripts/pipeline/cctv_analyzer.py --mode supplement --fetch-only | 新闻补采 | 补采7天内缺失的新闻 |
| 3 | weekly_multi_period_update | 周六10:00 | scripts/pipeline/daily_stock_selection_review.py --update-multi-period --days-back 90 | 周末任务 | 批量更新90天多周期表现 |

---

## 六、system_maintenance (系统维护)

**时段**: 持续运行  
**核心目标**: 保障系统稳定运行，监控任务状态

### 任务列表

| 序号 | 任务名称 | 时间 | 脚本路径 | 功能 | 实现目标 |
|------|----------|------|----------|------|----------|
| 1 | scheduler_watchdog | 每5分钟 | scripts/scheduler_watchdog.sh | 心跳监控 | 监控调度器健康状态 |
| 2 | cache_cleanup | 03:00 | scripts/cache_monitor.py | 缓存清理 | 清理损坏缓存和过期checkpoint |
| 3 | generate_dashboard | 每10分钟 | scripts/monitoring_dashboard.py | 监控面板 | 生成任务监控面板HTML |

---

## 脚本索引

### 按脚本分组

| 脚本路径 | 被调用任务数 | 所属分组 | 任务列表 |
|----------|-------------|----------|----------|
| scripts/pipeline/data_collect.py | 3 | post_market_afternoon | data_fetch, data_fetch_retry, data_fetch_retry2 |
| scripts/monitoring_dashboard.py | 3 | pre_market_morning, post_market_afternoon, system_maintenance | generate_dashboard, morning_monitoring_summary, evening_monitoring_summary |
| scripts/pipeline/fund_behavior_resource_guardian.py | 2 | pre_market_morning, resource_guarantee | fund_behavior_resource_prepare, fund_behavior_resource_validate |
| scripts/core_task_guardian.py | 2 | pre_market_morning, resource_guarantee | fund_behavior_guardian_check, fund_behavior_report |
| scripts/pipeline/cctv_analyzer.py | 2 | daily | collect_news_cctv_midnight, collect_news_cctv_morning |
| scripts/pipeline/daily_stock_selection_review.py | 2 | post_market_afternoon, daily | daily_selection_review, weekly_multi_period_update |
| scripts/pipeline/data_audit_unified.py | 1 | post_market_afternoon | data_audit_unified |
| scripts/calculate_cvd.py | 1 | post_market_afternoon | calculate_cvd |
| scripts/pipeline/daily_review.py | 1 | post_market_afternoon | market_review |
| scripts/pipeline/stock_pick.py | 1 | post_market_afternoon | picks_review |
| scripts/send_review_report.py | 1 | post_market_afternoon | review_report |
| scripts/pipeline/morning_push.py | 1 | post_market_afternoon | review_brief |
| scripts/pipeline/precompute.py | 1 | post_market_evening | precompute |
| scripts/pipeline/night_picks.py | 1 | post_market_evening | night_analysis |
| scripts/pipeline/morning_update.py | 1 | pre_market_morning | morning_data |
| scripts/collect_macro_data.py | 1 | pre_market_morning | collect_macro |
| scripts/collect_oil_dollar.py | 1 | pre_market_morning | collect_oil_dollar |
| scripts/collect_commodities.py | 1 | pre_market_morning | collect_commodities |
| scripts/collect_sentiment.py | 1 | pre_market_morning | collect_sentiment |
| scripts/collect_news.py | 1 | pre_market_morning | collect_news |
| scripts/market_analysis.py | 1 | pre_market_morning | market_analysis |
| scripts/pipeline/send_morning_shao.py | 1 | pre_market_morning | morning_report |
| scripts/pipeline/fetch_index.py | 1 | post_market_afternoon | update_index_data |
| scripts/scheduler_watchdog.sh | 1 | system_maintenance | scheduler_watchdog |
| scripts/pipeline/update_tracking.py | 1 | post_market_afternoon | update_tracking |
| scripts/manual_verification.py | 1 | post_market_evening | manual_verification |
| scripts/cache_monitor.py | 1 | system_maintenance | cache_cleanup |
| scripts/data_integrity_check.py | 1 | pre_market_morning | data_integrity_check |

---

## 任务分组设计原则

### 1. 时间隔离
- **盘前上午**: 08:30-09:30，确保开盘前完成准备
- **盘后下午**: 16:00-19:00，收盘后立即处理数据
- **盘后夜间**: 20:00-21:00，深度分析时段

### 2. 依赖解耦
- 数据采集 → 数据审计 → 复盘分析 → 报告推送
- 每个阶段完成后才进入下一阶段

### 3. 核心任务保障
- 09:26任务有独立的3层保障链
- 4分钟容错时间，熔断+兜底机制

### 4. 监控全覆盖
- 每5分钟心跳检测
- 每10分钟生成监控面板
- 晨间/盘后发送监控摘要
