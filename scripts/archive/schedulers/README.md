# 归档的调度器脚本

## 归档原因

这些调度脚本功能重复或已整合，统一归档到此处。当前使用 `scripts/apscheduler_enhanced.py` 作为主调度器。

## 归档脚本说明

### 调度器实现（5个）
| 脚本 | 原功能 | 归档原因 |
|------|--------|----------|
| `apscheduler_backup.py` | APScheduler备份调度器 | 与 `apscheduler_enhanced.py` 功能重复，且硬编码任务列表 |
| `backup_scheduler.py` | Cron备用方案 | 使用croniter实现，与其他方案重复 |
| `cron_task_manager.py` | 从YAML生成cron任务 | 功能已整合到 `apscheduler_enhanced.py` |
| `run_daily_scheduler.py` | 每日调度器启动 | 调用服务层，与其他调度器重复 |
| `run_scheduler.py` | APScheduler启动 | 调用服务层，与其他调度器重复 |

### 调度相关工具脚本（8个）
| 脚本 | 原功能 | 归档原因 |
|------|--------|----------|
| `scheduled_tasks.py` | 综合定时任务 | 功能重复，已整合 |
| `scheduled_fetch_optimized.py` | 优化版定时采集 | 功能重复，已整合 |
| `schedule_monitor.py` | 调度监控 | 功能已整合到主调度器 |
| `validate_cron_config.py` | 配置验证 | 一次性使用，已验证完成 |
| `verify_scheduler_config.py` | 调度器配置验证 | 一次性使用，已验证完成 |
| `check_scripts_existence.py` | 脚本存在性检查 | 一次性使用，已检查完成 |
| `analyze_script_duplicates.py` | 重复脚本分析 | 一次性使用，已分析完成 |
| `monitor_0926_task_chain.py` | 09:26任务链监控 | 一次性使用，已监控完成 |
| `test_0926_task_chain.py` | 09:26任务链测试 | 一次性使用，已测试完成 |

## 当前方案

统一使用 `scripts/apscheduler_enhanced.py`：
- 从 `config/cron_tasks.yaml` 读取任务配置
- 支持任务状态持久化
- 支持自动重试
- 支持邮件通知

## 恢复方法

如需恢复使用，将相应脚本移回 `scripts/` 目录即可：
```bash
mv archive/schedulers/xxx.py scripts/
```
