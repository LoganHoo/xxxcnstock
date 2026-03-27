# Docker 定时任务服务配置

## 概述

使用 Docker 容器运行定时任务，每日自动采集 A 股历史数据。

## 架构说明

### 方案一：单次执行容器
- 使用 `Dockerfile` 和 `docker-compose.yml`
- 容器启动后执行一次数据采集
- 适合手动触发或外部调度器调用

### 方案二：定时任务容器（推荐）
- 使用 `Dockerfile.cron` 和 `docker-compose.cron.yml`
- 容器内置 cron 服务
- 自动按计划执行数据采集
- 适合长期运行的自动化场景

---

## 快速开始

### 方案一：手动执行（单次）

#### 1. 构建镜像
```bash
docker-compose build
```

#### 2. 运行数据采集
```bash
docker-compose up
```

#### 3. 后台运行
```bash
docker-compose up -d
```

#### 4. 查看日志
```bash
docker-compose logs -f
```

#### 5. 停止服务
```bash
docker-compose down
```

---

### 方案二：定时任务服务（推荐）

#### 1. 构建定时任务镜像
```bash
docker-compose -f docker-compose.cron.yml build
```

#### 2. 启动定时任务服务
```bash
docker-compose -f docker-compose.cron.yml up -d
```

#### 3. 查看服务状态
```bash
docker-compose -f docker-compose.cron.yml ps
```

#### 4. 查看日志
```bash
# 查看容器日志
docker-compose -f docker-compose.cron.yml logs -f

# 查看定时任务日志
tail -f logs/cron.log
```

#### 5. 停止服务
```bash
docker-compose -f docker-compose.cron.yml down
```

---

## 定时任务配置

### 默认配置
- **执行时间**: 每个工作日下午 16:00（周一至周五）
- **时区**: Asia/Shanghai
- **采集范围**: 最近 3 年历史数据
- **速率限制**: 5 请求/秒

### 修改执行时间

编辑 `Dockerfile.cron` 中的 cron 配置：

```dockerfile
# 每日下午16:00执行
RUN echo "0 16 * * 1-5 cd /app && /usr/local/bin/python scripts/scheduled_fetch.py >> /app/logs/cron.log 2>&1" | crontab -
```

#### Cron 表达式说明
```
┌───────────── 分钟 (0 - 59)
│ ┌───────────── 小时 (0 - 23)
│ │ ┌───────────── 日 (1 - 31)
│ │ │ ┌───────────── 月 (1 - 12)
│ │ │ │ ┌───────────── 星期 (0 - 6, 0=周日)
│ │ │ │ │
* * * * *
```

#### 常用配置示例

```bash
# 每日下午16:00执行（工作日）
0 16 * * 1-5

# 每日凌晨2:00执行
0 2 * * *

# 每小时执行一次
0 * * * *

# 每6小时执行一次
0 */6 * * *

# 每周一上午9:00执行
0 9 * * 1

# 每月1日凌晨3:00执行
0 3 1 * *
```

---

## 数据持久化

### 挂载目录
```yaml
volumes:
  - ./data:/app/data        # 数据存储目录
  - ./logs:/app/logs        # 日志目录
  - ./config:/app/config:ro # 配置文件（只读）
```

### 数据位置
- **历史数据**: `./data/kline/*.parquet`
- **股票列表**: `./data/stock_list.parquet`
- **采集日志**: `./logs/scheduled_fetch.log`
- **Cron日志**: `./logs/cron.log`

---

## 环境变量配置

### 在 docker-compose.yml 中配置
```yaml
environment:
  - TZ=Asia/Shanghai           # 时区
  - PYTHONUNBUFFERED=1         # Python输出不缓冲
  - FETCH_DAYS=1095            # 采集天数（可选）
  - RATE_LIMIT=5.0             # 速率限制（可选）
```

### 创建 .env 文件
```bash
# .env
TZ=Asia/Shanghai
PYTHONUNBUFFERED=1
FETCH_DAYS=1095
RATE_LIMIT=5.0
```

---

## 日志管理

### 日志轮转配置
```yaml
logging:
  driver: "json-file"
  options:
    max-size: "10m"    # 单个日志文件最大10MB
    max-file: "3"      # 保留最近3个日志文件
```

### 查看日志
```bash
# 查看容器日志
docker logs xcnstock-cron

# 实时查看日志
docker logs -f xcnstock-cron

# 查看最近100行日志
docker logs --tail 100 xcnstock-cron

# 查看定时任务日志
tail -f logs/cron.log

# 查看采集日志
tail -f logs/scheduled_fetch.log
```

---

## 监控与告警

### 健康检查
```yaml
healthcheck:
  test: ["CMD", "test", "-f", "/app/logs/cron.log"]
  interval: 1m
  timeout: 10s
  retries: 3
  start_period: 40s
```

### 容器状态监控
```bash
# 查看容器状态
docker ps

# 查看容器资源使用
docker stats xcnstock-cron

# 查看容器详细信息
docker inspect xcnstock-cron
```

---

## 故障排查

### 1. 容器无法启动
```bash
# 查看容器日志
docker-compose logs xcnstock-cron

# 检查配置文件
ls -la config/

# 检查权限
ls -la data/ logs/
```

### 2. 定时任务未执行
```bash
# 进入容器
docker exec -it xcnstock-cron bash

# 检查 cron 服务
service cron status

# 查看 cron 日志
cat /var/log/syslog | grep CRON

# 手动测试脚本
python scripts/scheduled_fetch.py
```

### 3. 数据采集失败
```bash
# 查看采集日志
tail -f logs/scheduled_fetch.log

# 检查网络连接
docker exec -it xcnstock-cron ping -c 3 web.ifzq.gtimg.cn

# 检查磁盘空间
df -h
```

---

## 高级配置

### 多阶段构建优化
```dockerfile
# 构建阶段
FROM python:3.11-slim as builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 运行阶段
FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY scripts/ ./scripts/
COPY config/ ./config/
CMD ["python", "scripts/scheduled_fetch.py"]
```

### 使用外部调度器
```bash
# Kubernetes CronJob
kubectl create cronjob xcnstock-fetch \
  --image=xcnstock-fetcher:latest \
  --schedule="0 16 * * 1-5" \
  -- python scripts/scheduled_fetch.py

# 使用 watchtower 自动更新
docker run -d \
  --name watchtower \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower xcnstock-cron
```

---

## 生产环境建议

1. **资源限制**
```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 2G
    reservations:
      cpus: '1'
      memory: 1G
```

2. **重启策略**
```yaml
restart: unless-stopped
```

3. **日志管理**
- 定期清理旧日志
- 使用日志收集系统（如 ELK）
- 设置日志告警

4. **监控告警**
- 监控容器运行状态
- 监控数据采集成功率
- 监控磁盘空间使用

5. **备份策略**
- 定期备份 parquet 数据文件
- 备份配置文件
- 记录采集历史

---

## 完整示例

### 启动定时任务服务
```bash
# 1. 构建镜像
docker-compose -f docker-compose.cron.yml build

# 2. 启动服务
docker-compose -f docker-compose.cron.yml up -d

# 3. 查看状态
docker-compose -f docker-compose.cron.yml ps

# 4. 查看日志
docker-compose -f docker-compose.cron.yml logs -f

# 5. 查看数据
ls -lh data/kline/ | head -20
```

### 手动触发采集
```bash
# 进入容器手动执行
docker exec -it xcnstock-cron python scripts/scheduled_fetch.py

# 或者使用单次执行容器
docker-compose run --rm xcnstock-fetcher
```

---

## 总结

- **推荐使用方案二**（定时任务容器）实现自动化数据采集
- 定时任务在工作日下午16:00执行，确保数据完整性
- 数据持久化到本地目录，便于备份和分析
- 日志完善，便于故障排查和监控
- 支持灵活的定时任务配置和扩展
