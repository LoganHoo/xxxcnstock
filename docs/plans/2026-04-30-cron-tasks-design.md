# Cron Tasks Design

## Goal

基于第一性原理重构 `config/cron_tasks.yaml`，使其同时满足两件事：

- 对 `scripts/scheduler.py` 来说，它是当前可执行任务的真实契约
- 对项目演进来说，它保留未来业务蓝图，但不会污染当前执行面

## Context Audit

### 当前事实

- 调度器当前真正消费的是 `tasks`，包括：
  - `day_type`
  - `depends_on`
  - `working_dir`
  - `env`
  - `run_once`
  - `progress_check`
  - `circuit_breaker`
  - 全局 `environment`
  - 全局 `use_redis_lock` / `redis_lock_timeout`
- `groups` 当前不被 `scripts/scheduler.py` 消费，因此它们不是执行真相，只是描述性结构
- 当前共有 `44` 个可执行任务、`2` 个规划态任务、`12` 个分组
- 当前确认失效的脚本路径：
  - `kline_pre_market -> scripts/pipeline/kline_collect.py`
  - `kline_post_market -> scripts/pipeline/kline_collect.py`

### 当前问题

1. **执行真相与描述真相混杂**
   - `tasks` 决定真正执行
   - `groups` 决定阶段叙事
   - 但两者放在同一层文件中，容易给维护者造成“分组也会执行”的错觉

2. **字段存在配置债**
   - `priority`、`optional` 等字段当前并未被调度器消费
   - 它们属于“组织/规划元数据”，不是运行契约

3. **脚本存在性未被配置层约束**
   - 至少已有两个任务指向不存在的脚本，说明 `enabled: true` 不等于“当前能跑”

4. **未来蓝图和当前执行面没有物理隔离**
   - 现在所有任务默认都在 `tasks` 中
   - 没有清晰表达“当前任务”和“规划任务”的边界

## First-Principles Model

从第一性原理看，`cron_tasks.yaml` 必须回答四个问题：

1. **什么任务今天会被调度器选中**
2. **这些任务按什么依赖关系执行**
3. **失败后如何止损、熔断、重试或降级**
4. **哪些任务只是规划，不应进入当前执行面**

因此配置应被拆成两层语义：

- **执行层**：当前调度器真实消费的任务
- **规划层**：保留蓝图，但默认禁用，且显式标记为规划态

## Options Considered

### 方案 A：最小修补

- 保留现有结构
- 仅修复脚本缺失、依赖错误和重复/无效字段

优点：

- 改动最小

缺点：

- 结构债继续累积
- `groups` 仍然和执行配置混在一起

### 方案 B：主文件只保留可执行任务

- 删除或迁移 `groups`
- 主文件只保留当前调度器能执行的任务契约

优点：

- 最干净

缺点：

- 丢失主文件内的业务阶段视图
- 不符合“主文件禁用保留未来任务”的确认结果

### 方案 C：执行契约 + 规划保留区

- `tasks` 中保留当前可执行任务
- 新增 `planned_tasks` 或等价规划区，主文件内保留未来任务但统一禁用
- 将 `groups` 明确降级为“阶段视图/评审视图”，不再伪装成执行结构

优点：

- 同时满足执行真实性与未来规划
- 主文件仍是唯一真相源

缺点：

- 需要一次系统性重排

最终采用 **方案 C**。

## Target Structure

### 1. `global`

仅保留调度器运行所需的全局配置与环境注入配置。

### 2. `tasks`

只保留当前真实可执行任务。

任务字段顺序统一为：

1. `name`
2. `description`
3. `schedule`
4. `script`
5. `args`
6. `enabled`
7. `day_type`
8. `timeout`
9. `depends_on`
10. `working_dir`
11. `env`
12. `run_once`
13. `progress_check`
14. `circuit_breaker`
15. `alert_on_failure`

### 3. `planned_tasks`

保留未来任务或当前未接线任务，统一规则：

- `enabled: false`
- `status: planned`
- 可选 `reason`

### 4. `groups`

本次保留字段名 `groups`，不改名为 `task_views`。原因是：

- 当前文件已经在项目内承担阶段视图角色，直接改名收益有限
- `scripts/scheduler.py` 不消费 `groups`，因此保留字段名不会影响运行兼容

约束明确为：

- `groups` 仅用于评审与阶段视图
- 调度器当前不消费 `groups`

## Implementation Status

当前已落实的结构为：

- `tasks`：只保留当前执行面中的 `44` 个任务，且不再保留禁用条目
- `planned_tasks`：承接 `kline_pre_market` 与 `kline_post_market` 两个未接线任务，统一 `enabled: false` + `status: planned`
- `groups`：继续保留为主文件内阶段视图，帮助评审业务流，但不作为运行契约
- `environment`：维持顶层结构，保持与 `scripts/scheduler.py` 现有加载逻辑兼容

## Scope of This Change

本次会完成：

- 重构 `cron_tasks.yaml` 为“当前任务 + 规划保留区 + 阶段视图”
- 修复已确认的失效脚本路径问题
- 清理当前执行面中的明显噪音字段或为其明确规划属性
- 校验所有启用任务的脚本存在性与依赖闭合性
- 保留 `groups` 作为评审视图，但明确其非执行性质

本次不会完成：

- 修改调度器去消费 `groups`
- 引入多依赖语义
- 重写所有业务脚本的运行逻辑

## Risk Decisions

### 1. `kline_collect.py` 缺失

当前两个启用任务指向缺失脚本。处理策略：

- 若仓库中存在明确替代实现，则改为替代脚本
- 若不存在明确替代，则转入 `planned_tasks` 并禁用

### 2. 非消费字段

对 `priority`、`optional` 等字段：

- 若仍有评审价值，则保留在阶段视图或规划态中
- 若位于当前执行任务中，会尽量减少其干扰，并在评审中明确“调度器当前不消费”

### 3. 兼容性

本次以 `tasks` 的运行兼容为最高优先级，不做会破坏现有 `scheduler.py` 加载逻辑的结构性改动。

## Acceptance Criteria

完成后需满足：

- `scripts/scheduler.py` 能成功加载 `config/cron_tasks.yaml`
- 启用任务不再指向缺失脚本
- 启用任务的 `depends_on` 指向真实存在任务
- 规划任务不会进入当前执行面
- 文件结构可读性明显提升，阶段视图与执行契约边界清晰
- 输出一份评审结论：当前可执行任务、规划任务、高风险点
