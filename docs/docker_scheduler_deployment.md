# XCNStock Scheduler Docker 部署指南

## 概述

本文档介绍如何使用 Docker 部署 XCNStock 任务调度器服务。

## 文件说明

| 文件 | 说明 |
|------|------|
| `Dockerfile.scheduler` | 调度器服务镜像构建文件 |
| `docker-compose.scheduler.yml` | Docker Compose 部署配置 |
| `scripts/deploy_scheduler.sh` | 部署脚本 |

## 快速开始

### 1. 构建镜像

```bash
./scripts/deploy_scheduler.sh build
```

### 2. 启动服务

```bash
# 基础模式
./scripts/deploy_scheduler.sh start

# 带Redis支持（推荐生产环境）
./scripts/deploy_scheduler.sh start-redis
```

### 3. 查看状态

```bash
# 查看容器状态
./scripts/deploy_scheduler.sh status

# 查看调度器任务状态
./scripts/deploy_scheduler.sh scheduler

# 查看日志
./scripts/deploy_scheduler.sh logs
```

### 4. 停止服务

```bash
./scripts/deploy_scheduler.sh stop
```

## 手动部署

### 构建镜像

```bash
docker-compose -f docker-compose.scheduler.yml build
```

### 启动服务

```bash
# 基础模式
docker-compose -f docker-compose.scheduler.yml up -d

# 带Redis支持
docker-compose -f docker-compose.scheduler.yml --profile with-redis up -d

# 带监控面板
docker-compose -f docker-compose.scheduler.yml --profile with-monitor up -d
```

### 查看日志

```bash
docker-compose -f docker-compose.scheduler.yml logs -f
```

### 停止服务

```bash
docker-compose -f docker-compose.scheduler.yml down
```

## 配置说明

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `TZ` | Asia/Shanghai | 时区设置 |
| `PYTHONPATH` | /app | Python路径 |
| `SCHEDULER_CONFIG_PATH` | /app/config/cron_tasks.yaml | 任务配置文件 |
| `SCHEDULER_STATE_PATH` | /app/data/scheduler_state.json | 状态文件路径 |
| `LOG_LEVEL` | INFO | 日志级别 |
| `REDIS_HOST` | - | Redis主机（可选） |
| `REDIS_PORT` | 6379 | Redis端口（可选） |

### 数据卷

| 宿主机路径 | 容器路径 | 说明 |
|------------|----------|------|
| `./data` | `/app/data` | 数据文件 |
| `./logs` | `/app/logs` | 日志文件 |
| `./config` | `/app/config` | 配置文件（只读） |
| `./scripts` | `/app/scripts` | 脚本文件（只读） |
| `./core` | `/app/core` | 核心代码（只读） |
| `./services` | `/app/services` | 服务代码（只读） |

## 服务组件

### scheduler
- **说明**: 任务调度器主服务
- **镜像**: xcnstock/scheduler:latest
- **依赖**: 无（可选Redis）
- **端口**: 无

### redis（可选）
- **说明**: Redis服务，用于分布式锁
- **镜像**: redis:7-alpine
- **端口**: 127.0.0.1:6379
- **启用**: `--profile with-redis`

### scheduler-monitor（可选）
- **说明**: 简单的状态监控面板
- **启用**: `--profile with-monitor`

## 健康检查

调度器服务配置了健康检查：
- **检查间隔**: 30秒
- **超时时间**: 10秒
- **重试次数**: 3次
- **启动延迟**: 5秒

查看健康状态：
```bash
docker-compose -f docker-compose.scheduler.yml ps
```

## 资源限制

| 资源 | 限制 | 预留 |
|------|------|------|
| CPU | 2.0 | 0.5 |
| 内存 | 2G | 512M |

## 日志管理

日志配置：
- **驱动**: json-file
- **最大大小**: 100MB
- **最大文件数**: 5

查看日志：
```bash
# 实时日志
docker-compose -f docker-compose.scheduler.yml logs -f

# 最近100行
docker-compose -f docker-compose.scheduler.yml logs --tail=100
```

## 更新部署

```bash
# 使用脚本
./scripts/deploy_scheduler.sh update

# 手动更新
docker-compose -f docker-compose.scheduler.yml down
docker-compose -f docker-compose.scheduler.yml build --no-cache
docker-compose -f docker-compose.scheduler.yml up -d
```

## 故障排查

### 服务无法启动

1. 检查Docker和Docker Compose版本
```bash
docker --version
docker-compose --version
```

2. 检查端口占用
```bash
# 检查6379端口（Redis）
lsof -i :6379
```

3. 查看详细日志
```bash
docker-compose -f docker-compose.scheduler.yml logs
```

### 任务不执行

1. 检查配置文件
```bash
cat config/cron_tasks.yaml
```

2. 检查状态文件
```bash
cat data/scheduler_state.json
```

3. 检查日志
```bash
docker-compose -f docker-compose.scheduler.yml logs -f scheduler
```

### 数据丢失

数据持久化在 `./data` 目录，确保：
1. 不要删除 `./data` 目录
2. 定期备份 `./data` 和 `./logs`

## 安全建议

1. **生产环境**: 启用Redis分布式锁
2. **网络隔离**: 使用Docker网络隔离
3. **资源限制**: 设置CPU和内存限制
4. **日志审计**: 定期检查日志文件
5. **数据备份**: 定期备份数据目录

## 备份与恢复

### 备份

```bash
# 备份数据和配置
tar -czf backup-$(date +%Y%m%d).tar.gz data/ logs/ config/
```

### 恢复

```bash
# 停止服务
docker-compose -f docker-compose.scheduler.yml down

# 恢复数据
tar -xzf backup-20240101.tar.gz

# 启动服务
docker-compose -f docker-compose.scheduler.yml up -d
```

## 多环境部署

### 开发环境

```bash
# 使用代码挂载模式（实时更新）
docker-compose -f docker-compose.scheduler.yml up -d
```

### 生产环境

```bash
# 使用Redis和多实例
./scripts/deploy_scheduler.sh start-redis

# 或者手动
docker-compose -f docker-compose.scheduler.yml --profile with-redis up -d --scale scheduler=2
```

## 监控与告警

### 查看任务状态

```bash
./scripts/deploy_scheduler.sh scheduler
```

### 健康检查

```bash
./scripts/deploy_scheduler.sh health
```

### 集成外部监控

可以配置外部监控工具（如Prometheus）监控以下指标：
- 容器健康状态
- 任务执行成功率
- 数据文件更新时间和大小

## 相关文档

- [任务调度器指南](./task_scheduler_guide.md)
- [定时任务配置](../config/cron_tasks.yaml)
- [项目架构分析](./project_architecture_analysis.md)
