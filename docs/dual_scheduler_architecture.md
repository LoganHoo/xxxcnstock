# XCNStock 双调度器主备架构设计

> **优先级策略**: ⭐ Kestra > APScheduler
> - Kestra健康时，始终是主调度器
> - Kestra宕机时，APScheduler接管
> - Kestra恢复后，自动切回Kestra

## 架构概述

```
┌─────────────────────────────────────────────────────────────────┐
│              XCNStock 双调度器架构 (Kestra优先)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────┐  ┌─────────────────────────┐      │
│  │   🚀 Kestra             │  │   ⏰ APScheduler        │      │
│  │   ⭐ 主调度器 (优先)     │  │   备调度器 (待命)        │      │
│  │                         │  │                         │      │
│  │  • 可视化工作流          │  │  • Kestra宕机时接管      │      │
│  │  • 复杂依赖管理          │  │  • 轻量级快速启动        │      │
│  │  • 内置重试机制          │  │  • 状态持久化            │      │
│  │  • 任务历史记录          │  │  • Kestra恢复后自动让出   │      │
│  └──────────┬──────────────┘  └──────────┬──────────────┘      │
│             │                            │                      │
│             │      Kestra优先策略         │                      │
│             │    ┌─────────────────┐     │                      │
│             └───►│ 1. Kestra健康?  │◄────┘                      │
│                  │    是 → Kestra主 │                            │
│                  │    否 → APS接管  │                            │
│                  └─────────────────┘                            │
│                          │                                      │
│                   ┌──────▼──────┐                              │
│                   │   🔴 Redis   │                              │
│                   │  状态同步中心 │                              │
│                   └─────────────┘                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. Kestra (主调度器)

**角色**: Primary Scheduler

**职责**:
- 执行所有定时任务
- 管理复杂的工作流依赖
- 提供可视化监控界面
- 记录任务执行历史

**优势**:
- 可视化工作流设计
- 内置重试和错误处理
- 丰富的任务类型支持
- 完善的监控和告警

**部署**:
```yaml
# docker-compose.dual-scheduler.yml
kestra:
  image: kestra/kestra:latest
  ports:
    - "8082:8080"  # Web UI
```

### 2. APScheduler (备调度器)

**角色**: Backup Scheduler

**职责**:
- 监控Kestra健康状态
- Kestra故障时接管任务执行
- 状态持久化和断点续传
- 任务去重和互斥执行

**优势**:
- 轻量级，启动快速
- 状态持久化
- 灵活的调度策略
- 低资源占用

**部署**:
```bash
python3 scripts/dual_scheduler_manager.py --type apscheduler
```

### 3. Redis (状态同步中心)

**角色**: State Synchronization Hub

**功能**:
- 心跳数据存储
- Leader选举锁
- 任务状态共享
- 配置同步

**数据结构**:
```
xcnstock:scheduler:heartbeat  # Hash: 调度器心跳
xcnstock:scheduler:leader     # String: 当前Leader
xcnstock:scheduler:task_status # Hash: 任务执行状态
```

### 4. 双调度器管理器

**文件**: `scripts/dual_scheduler_manager.py`

**功能**:
- 健康检查循环
- Leader选举
- 故障转移决策
- 任务执行控制

**主备判定逻辑 (Kestra优先)**:
```python
# Kestra优先策略
if scheduler_type == "kestra":
    # Kestra只要健康就是主调度器
    if status == HEALTHY:
        acquire_leader_lock()
        role = PRIMARY
    else:
        role = BACKUP
else:
    # APScheduler只有Kestra宕机时才接管
    if kestra_status == DOWN:
        if failed_checks >= 3:
            acquire_leader_lock()
            role = PRIMARY
    else:
        role = BACKUP  # Kestra健康时永远是备
```

## 主备切换流程

### 正常状态

```
Kestra:     [主调度器] ──执行任务──► 任务完成
               ▲
               │ 心跳(30s)
               ▼
APScheduler: [备调度器] ──监控───► 待命状态
```

### 故障转移

```
时间线 ───────────────────────────────────────────────►

T+0s    Kestra宕机
        ▼
T+30s   APScheduler检测到Kestra心跳超时
        失败计数 = 1
        ▼
T+60s   APScheduler再次检测失败
        失败计数 = 2
        ▼
T+90s   APScheduler第三次检测失败
        失败计数 = 3 (达到阈值)
        ▼
T+90s   APScheduler强制接管Leader角色
        开始执行任务
        ▼
T+...   任务正常执行
```

### 自动恢复 (Kestra优先切回)

```
Kestra恢复上线
    ▼
Kestra检测到自己是备调度器
    ▼
检查APScheduler是Leader
    ▼
强制夺回Leader角色
    ▼
APScheduler降级为备
    ▼
Kestra恢复为主调度器
```

**Kestra优先恢复逻辑**:
```python
# Kestra恢复后强制夺回Leader
if scheduler_type == "kestra" and status == HEALTHY:
    current_leader = redis.get(LEADER_KEY)
    if "apscheduler" in current_leader:
        # 强制夺回
        redis.delete(LEADER_KEY)
        acquire_leader_lock()
        role = PRIMARY
        log("Kestra已恢复，从APScheduler夺回Leader角色")
```

## 任务执行互斥

### 去重机制

```python
# 只有Leader可以执行任务
def can_execute_task(task_name: str) -> bool:
    # 1. 检查自身角色
    if not is_leader():
        return False
    
    # 2. 检查任务是否已执行
    task_status = redis.get(f"task_status:{task_name}:{today}")
    if task_status and task_status['status'] == 'completed':
        return False  # 已执行，跳过
    
    return True

# 执行后标记
def mark_task_executed(task_name: str, result: dict):
    redis.setex(
        f"task_status:{task_name}:{today}",
        86400 * 7,  # 保留7天
        json.dumps({
            'status': 'completed',
            'scheduler': self.scheduler_name,
            'executed_at': datetime.now().isoformat(),
            'result': result
        })
    )
```

### 执行流程

```
任务触发
    ▼
检查是否是Leader?
    ├─ 否 ──► 跳过执行
    ▼
检查任务是否已执行?
    ├─ 是 ──► 跳过执行
    ▼
执行任务
    ▼
标记任务已执行
    ▼
上报执行结果
```

## 健康检查机制

### Kestra健康检查

```python
def check_kestra_health() -> SchedulerStatus:
    try:
        response = requests.get(
            "http://localhost:8082/api/v1/health",
            timeout=5
        )
        if response.status_code == 200:
            return SchedulerStatus.HEALTHY
        else:
            return SchedulerStatus.DEGRADED
    except requests.exceptions.ConnectionError:
        return SchedulerStatus.DOWN
```

### APScheduler健康检查

```python
def check_apscheduler_health() -> SchedulerStatus:
    heartbeat_file = Path("/app/logs/scheduler_heartbeat")
    
    if not heartbeat_file.exists():
        return SchedulerStatus.DOWN
    
    with open(heartbeat_file, 'r') as f:
        heartbeat_str = f.read().strip()
    
    heartbeat_time = datetime.strptime(heartbeat_str, '%Y-%m-%d %H:%M:%S')
    time_diff = (datetime.now() - heartbeat_time).total_seconds()
    
    if time_diff > 60:
        return SchedulerStatus.DOWN
    elif time_diff > 30:
        return SchedulerStatus.DEGRADED
    else:
        return SchedulerStatus.HEALTHY
```

## 监控面板

### Web界面

**地址**: http://localhost:8083

**功能**:
- 实时显示主备状态
- 显示最后心跳时间
- 显示活跃任务数
- 自动刷新(10秒)

### API接口

```bash
# 获取状态
GET /api/status

# 返回
{
  "kestra": {
    "name": "kestra_1234",
    "role": "PRIMARY",
    "status": "healthy",
    "last_heartbeat": "2026-01-01T12:00:00",
    "active_tasks": 10,
    "is_leader": true
  },
  "apscheduler": {
    "name": "apscheduler_5678",
    "role": "BACKUP",
    "status": "healthy",
    "last_heartbeat": "2026-01-01T12:00:00",
    "active_tasks": 0,
    "is_leader": false
  }
}
```

## 部署指南

### 1. 启动双调度器

```bash
# 使用Docker Compose启动
docker-compose -f docker-compose.dual-scheduler.yml up -d

# 服务列表:
# - redis: 状态同步
# - kestra: 主调度器 (http://localhost:8082)
# - kestra-db: Kestra数据库
# - kestra-health-adapter: Kestra健康适配器
# - apscheduler: 备调度器
# - scheduler-monitor: 监控面板 (http://localhost:8083)
```

### 2. 验证部署

```bash
# 检查Redis
docker exec xcnstock-redis redis-cli ping

# 检查Kestra
curl http://localhost:8082/api/v1/health

# 检查监控面板
curl http://localhost:8083/api/status
```

### 3. 测试故障转移

```bash
# 1. 停止Kestra容器
docker stop xcnstock-kestra

# 2. 观察监控面板
# APScheduler应该在90秒后接管Leader角色

# 3. 恢复Kestra
docker start xcnstock-kestra

# 4. 观察自动恢复
# Kestra恢复后应该成为备调度器
```

## 配置说明

### 双调度器配置

**文件**: `config/dual_scheduler.yaml`

```yaml
# Kestra配置
kestra:
  api_url: "http://localhost:8082/api/v1"
  timeout: 5

# APScheduler配置
apscheduler:
  heartbeat_file: "/app/logs/scheduler_heartbeat"
  timeout: 60

# Redis配置
redis:
  host: "localhost"
  port: 6379
  db: 0

# 故障转移配置
failover:
  enabled: true
  threshold: 3  # 连续失败3次后切换
  auto_recover: true
  health_check_interval: 30
```

## 优势总结

| 特性 | 单Kestra | 双调度器架构 |
|------|----------|--------------|
| 可用性 | 99% | 99.9% |
| 故障恢复时间 | 手动 | 自动(90秒内) |
| 任务丢失风险 | 中 | 低 |
| 运维复杂度 | 低 | 中 |
| 监控能力 | 强 | 更强 |

## 注意事项

1. **Redis高可用**: 生产环境建议使用Redis Cluster或Sentinel
2. **网络分区**: 考虑脑裂问题，可配置仲裁节点
3. **任务幂等**: 所有任务应该是幂等的，防止重复执行
4. **数据一致性**: 任务状态同步可能存在延迟，设计时考虑最终一致性
