# 定时任务系统重构计划

## 现状分析

### 配置文件
- `config/cron_tasks.yaml` - 主要定时任务配置（在用）
- `config/main.yaml` - 主配置
- `config/fund_behavior_config.yaml` - 资金行为配置
- `config/factors_config.yaml` - 因子配置
- `config/strategy_factors.yaml` - 策略因子配置
- `config/filters_config.yaml` - 过滤器配置
- `config/xcn_comm.yaml` - 通信配置

### 调度脚本（6个，存在重复）
1. `scripts/run_scheduler.py` - APScheduler启动脚本（使用services.data_service.scheduler）
2. `scripts/apscheduler_enhanced.py` - 增强版APScheduler（独立容器，在用）
3. `scripts/apscheduler_backup.py` - APScheduler备份调度器
4. `scripts/backup_scheduler.py` - Cron备用方案（独立任务调度器）
5. `scripts/run_daily_scheduler.py` - 每日任务调度器启动脚本
6. `scripts/cron_task_manager.py` - 定时任务配置管理器（从YAML生成cron任务）

## 问题识别

### 1. 调度脚本重复
- `apscheduler_enhanced.py` 和 `apscheduler_backup.py` 功能高度相似
- `backup_scheduler.py` 使用 croniter 实现，与其他方案重复
- `run_scheduler.py` 和 `run_daily_scheduler.py` 调用相同服务

### 2. 配置分散
- 任务配置在 `cron_tasks.yaml`
- 但部分调度器硬编码任务列表（如 backup_scheduler.py）
- 缺乏统一的配置管理

### 3. 目录结构混乱
- 调度脚本散落在 scripts 根目录
- 缺乏清晰的模块划分

## 重构方案

### 目标
1. **单一调度入口** - 只保留一个主调度器
2. **配置集中管理** - 所有任务从 cron_tasks.yaml 读取
3. **清晰的目录结构** - 调度相关脚本统一放置
4. **备份方案保留** - 保留一个备用调度方案

### 具体步骤

#### 步骤1: 创建新的目录结构
```
scripts/
├── scheduler/              # 调度器模块（新建）
│   ├── __init__.py
│   ├── main.py            # 主调度器入口（整合 apscheduler_enhanced.py）
│   ├── backup.py          # 备份调度器（整合 backup_scheduler.py）
│   ├── task_executor.py   # 任务执行器（从 cron_task_manager.py 提取）
│   └── state_manager.py   # 状态管理器
├── config/                # 配置管理（新建）
│   ├── __init__.py
│   ├── loader.py          # 配置加载器
│   └── validator.py       # 配置验证器
└── pipeline/              # 保持现有流水线脚本
```

#### 步骤2: 整合调度脚本
- **保留**: `apscheduler_enhanced.py` → 移动到 `scheduler/main.py`
- **整合**: `cron_task_manager.py` 的功能合并到调度器
- **归档**: 
  - `apscheduler_backup.py` → archive/
  - `backup_scheduler.py` → archive/
  - `run_scheduler.py` → archive/
  - `run_daily_scheduler.py` → archive/

#### 步骤3: 统一配置读取
- 所有调度器统一从 `cron_tasks.yaml` 读取任务
- 移除硬编码的任务列表
- 添加配置热加载支持

#### 步骤4: 更新启动方式
- 统一使用 `python -m scripts.scheduler.main`
- Docker 容器启动命令更新
- 文档更新

## 实施计划

### Phase 1: 准备工作
1. 备份现有调度脚本
2. 创建新的目录结构
3. 编写配置加载器

### Phase 2: 核心整合
1. 整合主调度器
2. 整合任务执行器
3. 整合状态管理器

### Phase 3: 测试验证
1. 配置读取测试
2. 任务执行测试
3. 故障转移测试

### Phase 4: 清理归档
1. 归档旧脚本
2. 更新文档
3. 更新启动脚本

## 预期收益

1. **减少维护成本** - 从6个调度脚本减少到2个
2. **统一配置管理** - 单一配置文件，热加载支持
3. **清晰的代码结构** - 模块化设计，易于扩展
4. **可靠的备份方案** - 保留备用调度器
