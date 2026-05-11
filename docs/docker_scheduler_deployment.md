# XCNStock Scheduler Docker 部署指南

## 概述

XCNStock 调度服务基于 APScheduler + Redis 实现跨平台统一的任务调度，支持分布式锁、健康检查 API。

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                   Docker Container                       │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │
│  │ APScheduler │  │ TaskExecutor │  │ Flask API   │  │
│  │ (调度器)     │  │ (执行器)      │  │ /health    │  │
│  └─────────────┘  └──────────────┘  └─────────────┘  │
│         │                │                             │
│         └────────────────┼─────────────────────────────
│                          │
│              ┌───────────┴───────────┐
│              │    RedisLockManager   │
│              │    (Redis 分布式锁)    │
│              └───────────┬───────────┘
└──────────────────────────┼──────────────────────────────
                           │
                    ┌──────┴──────┐
                    │    Redis    │
                    └─────────────┘
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `Dockerfile.scheduler` | 调度器服务镜像 |
| `docker-compose.scheduler.yml` | Docker Compose 部署配置 |
| `requirements_scheduler.txt` | Python 依赖 |
| `services/data_service/scheduler/` | 调度服务源码 |
| `scripts/deploy_scheduler.sh` | 部署脚本 |

## 快速开始

### 1. 构建镜像

```bash
docker build -f Dockerfile.scheduler -t xcnstock-scheduler .
```

### 2. 启动服务

```bash
# 使用 docker-compose
docker-compose -f docker-compose.scheduler.yml up -d

# 或直接运行
docker run -d --name xcnstock_scheduler \
  -e REDIS_HOST=49.233.10.199 \
  -e REDIS_PORT=6379 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  xcnstock-scheduler
```

### 3. 验证部署

```bash
# 健康检查
curl http://localhost:5000/health

# 查看任务列表
curl http://localhost:5000/tasks

# 查看特定任务状态
curl http://localhost:5000/tasks/daily_data_collection
```

### 4. 停止服务

```bash
docker-compose -f docker-compose.scheduler.yml down
```

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/tasks` | GET | 任务列表 |
| `/tasks/<name>` | GET | 单个任务状态 |
| `/tasks/<name>/run` | POST | 手动触发任务 |
| `/lock/<key>` | GET | 查看锁状态 |

## 配置

配置文件：`config/scheduler.yaml`

```yaml
scheduler:
  timezone: "Asia/Shanghai"
  max_workers: 4

tasks:
  - name: "daily_data_collection"
    script: "scripts/pipeline/data_collect.py"
    schedule: "30 17 * * 1-5"  # 工作日 17:30
    timeout: 7200
    requires_lock: true
    lock_key: "data_collection"

lock:
  redis:
    host: "${REDIS_HOST:-localhost}"
    port: ${REDIS_PORT:-6379}
```

## 定时任务

| 任务 | 调度 | 说明 |
|------|------|------|
| daily_data_collection | 30 17 * * 1-5 | K线数据采集 |
| data_quality_check | 0 18 * * 1-5 | 数据质量检查 |
| system_monitor | */15 * * * * | 系统健康检查 |
| data_freshness_check | 0 */2 * * * | 数据新鲜度检查 |

## 故障排除

### Redis 连接失败

```
WARNING - Redis 连接失败: Error ...，将使用无锁模式
```

检查 Redis 服务是否可用：
```bash
redis-cli -h 49.233.10.199 -p 6379 ping
```

### 任务被跳过

如果看到 `Task xxx is already locked`，说明上一个任务仍在执行或锁未正确释放。

手动释放锁：
```bash
curl http://localhost:5000/lock/data_collection
```
