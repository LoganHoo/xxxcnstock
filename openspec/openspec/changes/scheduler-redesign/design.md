# 计划任务调度系统重构 - 设计文档

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Redis     │    │  Scheduler  │    │   App       │         │
│  │   (Lock)    │◄───│  Container  │───►│  Container  │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
│                           │                                     │
│                           ▼                                     │
│                    ┌─────────────┐                              │
│                    │  Scheduler  │                              │
│                    │   Worker    │                              │
│                    └─────────────┘                              │
│                           │                                     │
│         ┌─────────────────┼─────────────────┐                 │
│         ▼                 ▼                 ▼                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │data_collect │   │quality_check│   │system_monitor│          │
│  └─────────────┘   └─────────────┘   └─────────────┘           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. Scheduler Container

| 组件 | 技术 | 职责 |
|------|------|------|
| APScheduler | apscheduler | 任务调度引擎 |
| Redis Client | redis-py | 分布式锁 |
| HTTP Server | Flask | 健康检查接口 |

### 2. 任务执行器

```python
class TaskExecutor:
    """统一任务执行器"""
    def __init__(self, redis_client, task_config):
        self.redis = redis_client
        self.config = task_config

    async def execute_with_lock(self, task_name, script_path):
        lock_key = f"lock:{task_name}"
        lock = self.redis.lock(lock_key, timeout=7200)

        if lock.acquire(blocking=False):
            try:
                result = subprocess.run(
                    ["python", script_path],
                    capture_output=True,
                    timeout=self.config.timeout
                )
                return result
            finally:
                lock.release()
        else:
            raise TaskLockedException(f"{task_name} is already running")
```

### 3. 分布式锁

```python
# Redis Lock 键设计
lock:data_collection      # 数据采集锁 (TTL: 2h)
lock:data_quality_check    # 质检任务锁 (TTL: 30m)
lock:system_monitor        # 系统监控锁 (TTL: 5m)
```

## 配置文件

### scheduler.yaml

```yaml
scheduler:
  name: "XCNStock APScheduler"
  timezone: "Asia/Shanghai"
  max_workers: 4

tasks:
  - name: "daily_data_collection"
    script: "scripts/pipeline/data_collect.py"
    schedule: "30 17 * * 1-5"  # 工作日 17:30
    timeout: 7200
    requires_lock: true
    lock_key: "data_collection"

  - name: "data_quality_check"
    script: "scripts/pipeline/data_quality_check.py"
    schedule: "0 18 * * 1-5"
    timeout: 1800
    requires_lock: true
    lock_key: "data_quality_check"

  - name: "system_monitor"
    script: "scripts/monitor/system_check.py"
    schedule: "*/15 * * * *"
    timeout: 300
    requires_lock: false

lock:
  redis:
    host: "${REDIS_HOST}"
    port: ${REDIS_PORT}
  default_ttl: 7200
  auto_renew: true
```

## Docker 配置

### Dockerfile.scheduler

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY scripts/ ./scripts/
COPY config/ ./config/
COPY services/ ./services/
ENV TZ=Asia/Shanghai
CMD ["python", "-m", "scheduler"]
```

### docker-compose.scheduler.yml

```yaml
services:
  scheduler:
    build:
      context: .
      dockerfile: Dockerfile.scheduler
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    environment:
      - REDIS_HOST=49.233.10.199
      - REDIS_PORT=6379
      - TZ=Asia/Shanghai
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## 健康检查 API

```
GET /health
Response: {"status": "healthy", "running_tasks": [], "timestamp": "..."}

GET /tasks
Response: {"tasks": [{"name": "...", "last_run": "...", "next_run": "..."}]}

GET /tasks/{name}/status
Response: {"name": "...", "status": "running|completed|failed", "started_at": "..."}
```

## 部署流程

1. 构建镜像: `docker build -f Dockerfile.scheduler -t xcnstock-scheduler .`
2. 启动服务: `docker-compose -f docker-compose.scheduler.yml up -d`
3. 验证健康: `curl http://localhost:5000/health`
4. 查看日志: `docker logs -f xcnstock_scheduler_1`

## 迁移步骤

1. 停止 macOS LaunchAgent
2. 启动 Docker Scheduler 容器
3. 验证任务执行
4. 确认无重复执行
