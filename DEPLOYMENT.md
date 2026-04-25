# XCNStock 数据流水线优化 - 部署指南

## 概述

本文档描述了数据流水线性能优化后的部署流程。优化内容包括：

1. **并行数据采集引擎** - 使用 aiohttp 实现高并发采集
2. **增量数据处理** - 只采集缺失数据，减少重复工作
3. **Polars 性能优化** - 使用高性能 DataFrame 库
4. **多级缓存系统** - L1 内存缓存 + L2 Redis 缓存
5. **双调度器协调** - 分布式锁和任务状态管理
6. **流水线监控** - Prometheus 指标采集

## 系统要求

### 硬件要求

- **CPU**: 4 核以上
- **内存**: 8GB 以上（推荐 16GB）
- **磁盘**: 50GB 可用空间（SSD 推荐）
- **网络**: 稳定的互联网连接

### 软件要求

- Docker 20.10+
- Docker Compose 2.0+
- Python 3.11+

## 快速开始

### 1. 克隆代码

```bash
git clone <repository-url>
cd xcnstock
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 文件，填入实际配置
vim .env
```

关键配置项：

```bash
# 数据源
TUSHARE_TOKEN=your_tushare_token

# 数据库
MYSQL_PASSWORD=your_mysql_password
REDIS_PASSWORD=your_redis_password

# 性能优化
PARALLEL_MAX_CONCURRENT=50
CACHE_L1_MAXSIZE=1000
POLARS_MAX_THREADS=4

# 监控
PROMETHEUS_ENABLED=true
```

### 3. 启动服务

```bash
# 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f xcnstock-fetcher
```

### 4. 验证部署

```bash
# 检查 Redis 连接
docker-compose exec redis redis-cli ping

# 检查 MySQL 连接
docker-compose exec mysql mysql -u xcnstock -p -e "SELECT 1"

# 检查 Prometheus
curl http://localhost:9090/-/healthy
```

## 服务架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Docker Compose                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  xcnstock-   │    │    Redis     │    │    MySQL     │  │
│  │  fetcher     │◄──►│   (缓存)     │    │  (数据存储)   │  │
│  │  (主服务)    │    │              │    │              │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                                                    │
│         │              ┌──────────────┐                     │
│         └─────────────►│  Prometheus  │                     │
│                        │   (监控)     │                     │
│                        └──────────────┘                     │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐                      │
│  │    Kestra    │    │  Kestra DB   │                      │
│  │  (主调度器)   │◄──►│  (Postgres)  │                      │
│  └──────────────┘    └──────────────┘                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 配置说明

### 性能优化配置

编辑 `config/cron_tasks.yaml`：

```yaml
global:
  performance:
    # 并行数据采集
    parallel_fetch:
      enabled: true
      max_concurrent: 50        # 最大并发数
      batch_size: 100           # 每批处理数量
      calls_per_minute: 480     # 每分钟请求数限制
    
    # 增量处理
    incremental:
      enabled: true
      max_gap_days: 7           # 最大允许日期间隔
    
    # 缓存配置
    cache:
      l1_enabled: true
      l1_maxsize: 1000
      l2_enabled: true
```

### 双调度器配置

编辑 `config/dual_scheduler.yaml`：

```yaml
failover:
  enabled: true
  primary_scheduler: "kestra"
  threshold: 3
  auto_recover: true
  health_check_interval: 30

distributed_lock:
  default_ttl: 60
  auto_renew: true
  deadlock_check_interval: 60
```

## 监控和告警

### Prometheus 指标

访问 http://localhost:9090 查看 Prometheus 界面。

关键指标：

- `xcnstock_task_duration_seconds` - 任务执行时间
- `xcnstock_task_success_rate` - 任务成功率
- `xcnstock_cache_hit_ratio` - 缓存命中率
- `xcnstock_data_collection_duration_seconds` - 数据采集时间

### 日志查看

```bash
# 查看应用日志
docker-compose logs -f xcnstock-fetcher

# 查看 Redis 日志
docker-compose logs -f redis

# 查看 MySQL 日志
docker-compose logs -f mysql
```

## 故障排查

### 常见问题

#### 1. Redis 连接失败

```bash
# 检查 Redis 状态
docker-compose exec redis redis-cli ping

# 重启 Redis
docker-compose restart redis
```

#### 2. 任务执行超时

检查 `config/cron_tasks.yaml` 中的 `timeout` 配置，或增加并行度：

```yaml
parallel_fetch:
  max_concurrent: 100  # 增加并发数
```

#### 3. 内存不足

调整缓存配置：

```yaml
cache:
  l1_maxsize: 500  # 减少内存缓存大小
```

### 性能调优

#### 数据采集优化

1. **调整并发数**：根据 API 限制调整 `max_concurrent`
2. **调整批次大小**：根据内存情况调整 `batch_size`
3. **启用增量采集**：确保 `incremental.enabled: true`

#### 缓存优化

1. **调整 TTL**：根据数据更新频率调整缓存过期时间
2. **监控命中率**：通过 Prometheus 监控缓存效率

## 回滚方案

如需回滚到优化前版本：

```bash
# 停止服务
docker-compose down

# 切换到旧版本分支
git checkout <old-branch>

# 启动旧版本
docker-compose up -d
```

## 维护操作

### 数据备份

```bash
# 备份 MySQL
docker-compose exec mysql mysqldump -u root -p xcnstock > backup.sql

# 备份 Redis
docker-compose exec redis redis-cli BGSAVE
```

### 更新部署

```bash
# 拉取最新代码
git pull origin main

# 重建镜像
docker-compose build

# 滚动更新
docker-compose up -d
```

## 联系支持

如有问题，请提交 Issue 或联系开发团队。
