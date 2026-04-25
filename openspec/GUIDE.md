# OpenSpec 项目使用指南

## 项目概述

OpenSpec 是一个规范驱动的开发工作流系统，用于管理复杂软件项目的变更、设计和实现。

```
┌─────────────────────────────────────────────────────────────────┐
│                    OpenSpec 工作流                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   💡 Explore (探索)                                              │
│   └─────────────────────────────────────┐                       │
│                                         ▼                       │
│   📝 Propose (提案)  ──►  🏗️ Design (设计)                       │
│                              │                                  │
│                              ▼                                  │
│   ✅ Apply (实现)  ◄──  📋 Tasks (任务)                          │
│      │                                                          │
│      ▼                                                          │
│   📦 Archive (归档)                                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
openspec/
├── config.yaml          # 项目配置
├── GUIDE.md            # 本指南
├── changes/            # 变更目录
│   ├── archive/        # 已归档的变更
│   └── <change-name>/  # 活跃的变更
│       ├── proposal.md # 提案文档
│       ├── design.md   # 设计文档
│       ├── spec.md     # 规范文档
│       ├── tasks.md    # 任务列表
│       └── ACCEPTANCE.md # 验收报告
└── specs/              # 能力规范目录
    └── <capability>/   # 具体能力规范
        └── spec.md
```

## 核心概念

### 1. Change (变更)

一个变更代表一个完整的功能开发或修改周期，包含从提案到实现的完整生命周期。

**变更状态流转**:
```
proposed ──► designed ──► in_progress ──► completed ──► archived
```

### 2. 工作流阶段

| 阶段 | 命令 | 目的 | 输出 |
|------|------|------|------|
| **Explore** | `/openspec:explore` | 探索想法、澄清需求 | 思考记录 |
| **Propose** | `/openspec:propose` | 创建变更提案 | `proposal.md` |
| **Design** | 自动生成 | 系统设计 | `design.md` |
| **Tasks** | 自动生成 | 任务分解 | `tasks.md` |
| **Apply** | `/openspec:apply-change` | 执行实现 | 代码变更 |
| **Archive** | `/openspec:archive-change` | 归档完成 | 移动到 `archive/` |

## 使用方法

### 开始一个新变更

```
/openspec:propose <变更名称>
```

例如:
```
/openspec:propose add-user-authentication
```

这将创建:
- `changes/add-user-authentication/proposal.md`
- `changes/add-user-authentication/design.md`
- `changes/add-user-authentication/tasks.md`

### 探索模式 (思考阶段)

在正式创建变更前，使用探索模式澄清需求:

```
/openspec:explore <主题>
```

例如:
```
/openspec:explore user-auth-options
```

在探索模式中:
- 可以深入讨论需求和方案
- 不会生成代码
- 可以创建或更新 OpenSpec 文档

### 查看当前变更

```
/openspec:list
```

### 继续实现变更

```
/openspec:apply-change <变更名称>
```

### 完成并归档

```
/openspec:archive-change <变更名称>
```

## 文档规范

### proposal.md (提案)

包含:
- **Problem**: 要解决的问题
- **Success Criteria**: 成功标准
- **Scope**: 范围界定 (In-scope / Out-of-scope)
- **Risks**: 风险分析

### design.md (设计)

包含:
- **Architecture**: 架构设计
- **Data Model**: 数据模型
- **API Design**: 接口设计
- **Implementation Notes**: 实现注意事项

### tasks.md (任务)

包含:
- 可执行的任务列表
- 每个任务有明确的目标和验收标准
- 任务间依赖关系

### spec.md (规范)

位于 `specs/<capability>/` 目录，定义:
- 能力需求
- 接口契约
- 行为约束

## 最佳实践

### 1. 变更粒度

- 一个变更应该对应一个可交付的功能
- 避免过大变更 (超过2周工作量应拆分)
- 避免过小变更 (至少包含3个以上任务)

### 2. 探索先行

复杂功能先使用 `/openspec:explore` 探索:
- 澄清需求边界
- 比较不同方案
- 识别潜在风险

### 3. 渐进式设计

- 提案阶段关注"做什么"
- 设计阶段关注"怎么做"
- 任务阶段关注"具体步骤"

### 4. 保持同步

- 代码变更后更新相关文档
- 设计变更时更新 `design.md`
- 需求变更时更新 `proposal.md`

## 项目配置

编辑 `config.yaml` 自定义项目:

```yaml
schema: spec-driven

# 项目上下文 (AI生成文档时参考)
context: |
  Tech stack: Python, FastAPI, PostgreSQL
  Domain: Stock trading analysis platform
  
# 自定义规则
rules:
  proposal:
    - Include security considerations
    - Define rollback strategy
  tasks:
    - Max 4 hours per task
    - Include test requirements
```

## 示例工作流

### 场景: 添加数据导出功能

```bash
# 1. 探索需求
/openspec:explore data-export-feature

# 2. 创建变更提案
/openspec:propose add-data-export

# 3. AI生成设计和任务
# (自动生成 design.md 和 tasks.md)

# 4. 开始实现
/openspec:apply-change add-data-export

# 5. 完成任务...

# 6. 归档
/openspec:archive-change add-data-export
```

## 快捷命令

| 命令 | 功能 |
|------|------|
| `/openspec:explore <topic>` | 进入探索模式 |
| `/openspec:propose <name>` | 创建变更提案 |
| `/openspec:apply-change <name>` | 执行变更实现 |
| `/openspec:archive-change <name>` | 归档变更 |
| `/openspec:list` | 列出所有变更 |

## 注意事项

1. **不要直接修改** `changes/` 目录下的文件结构
2. **使用命令** 管理变更生命周期
3. **保持文档同步** 与代码实现一致
4. **及时归档** 完成的变更

---

*本指南基于 OpenSpec 规范驱动开发方法论*
