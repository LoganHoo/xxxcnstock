# 计划任务调度系统重构 - 原子任务清单

> 遵循 GSD 规范：每任务 < 2 小时，产出物明确，可独立验证

---

## Phase 1: 项目初始化

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 1.1 | 创建目录结构 `services/data_service/scheduler/` | 目录+`__init__.py` | `ls` 确认存在 |
| 1.2 | 创建 `requirements_scheduler.txt` | 依赖文件 | `cat requirements_scheduler.txt` |
| 1.3 | 创建 `Dockerfile.scheduler` | Dockerfile | `docker build --help` 验证 |

---

## Phase 2: 核心组件

### 2.1 RedisLockManager

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 2.1.1 | 创建 `locks/redis_lock.py` 类框架 | 空类 | `python -c "from ... import RedisLockManager"` |
| 2.1.2 | 实现 `acquire()` 方法 | 锁获取逻辑 | UT: `acquire → exists` |
| 2.1.3 | 实现 `release()` 方法 | 锁释放逻辑 | UT: `release → not exists` |
| 2.1.4 | 实现 `is_locked()` 查询方法 | 状态查询 | UT: `is_locked()` |

### 2.2 TaskExecutor

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 2.2.1 | 创建 `tasks/executor.py` 类框架 | 空类 | `python -c "from ... import TaskExecutor"` |
| 2.2.2 | 实现 `execute()` 基本执行 | subprocess 调用 | `execute('echo test')` |
| 2.2.3 | 实现 `execute_with_lock()` 加锁执行 | 锁+执行 | 并发测试 |
| 2.2.4 | 实现超时控制 | Timeout 逻辑 | 超时 kill 验证 |

### 2.3 SchedulerService

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 2.3.1 | 创建 `scheduler_service.py` 类框架 | APScheduler 初始化 | `python -c "from ... import SchedulerService"` |
| 2.3.2 | 实现 `add_task()` 添加任务 | 任务注册 | `get_jobs()` 确认 |
| 2.3.3 | 实现 `remove_task()` 删除任务 | 任务移除 | `get_jobs()` 确认 |
| 2.3.4 | 实现 `start()` / `shutdown()` 生命周期 | 启动/停止 | 日志无异常 |

### 2.4 HealthCheckAPI

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 2.4.1 | 创建 `api/health.py` Flask 框架 | 空 API | `curl localhost:5000/health` |
| 2.4.2 | 实现 `/health` 端点 | 健康检查 | 返回 JSON |
| 2.4.3 | 实现 `/tasks` 端点 | 任务列表 | 返回任务数组 |
| 2.4.4 | 实现 `/tasks/{name}/status` 端点 | 单任务状态 | 返回状态 JSON |

---

## Phase 3: 配置与集成

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 3.1 | 创建 `config_loader.py` YAML 加载 | 配置解析 | `python -c "load_config()"` |
| 3.2 | 更新 `config/scheduler.yaml` 统一配置 | 完整配置 | YAML 语法验证 |
| 3.3 | 创建 `main.py` 入口整合所有组件 | 可执行入口 | `python main.py & curl /health` |
| 3.4 | 集成 `core/logger.py` 日志系统 | 日志输出 | 日志文件验证 |

---

## Phase 4: Docker 部署

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 4.1 | 创建 `docker-compose.scheduler.yml` | Compose 文件 | `docker compose config` |
| 4.2 | 构建镜像 `xcnstock-scheduler` | Docker 镜像 | `docker images` |
| 4.3 | 启动容器验证 `/health` | 运行容器 | `curl localhost:5000/health` |
| 4.4 | 验证定时任务触发 | cron 执行 | 等待或手动触发 |

---

## Phase 5: 迁移与文档

| # | 任务 | 产出物 | 验证方式 |
|---|------|--------|----------|
| 5.1 | 停止 macOS LaunchAgent | 已卸载 | `launchctl list` 无相关 |
| 5.2 | 更新 README 部署说明 | README 更新 | `git diff` |
| 5.3 | 创建 `docs/scheduler-deployment.md` | 部署文档 | `cat docs/scheduler-deployment.md` |

---

## 原子任务汇总 (26 项)

```
Phase 1:  3 任务
Phase 2: 15 任务
Phase 3:  4 任务
Phase 4:  4 任务
Phase 5:  3 任务
─────────────────
Total:   26 任务
```

每任务预计 30-90 分钟，全部可独立验证。
