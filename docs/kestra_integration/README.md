# Kestra 工作流集成指南

## 快速开始

### 1. 环境配置

确保 `.env` 文件中包含 Kestra 配置：

```env
KESTRA_API_URL=http://192.168.1.168:8082/api/v1
KESTRA_WEB_URL=http://192.168.1.168:8082/ui/
KESTRA_USERNAME=admin@kestra.io
KESTRA_PASSWORD=Kestra123
KESTRA_NAMESPACE=xcnstock
```

### 2. 部署工作流

```bash
# 部署所有工作流
python kestra/deploy.py

# 验证 YAML 语法（不部署）
python kestra/deploy.py --validate-only

# 部署单个工作流
python kestra/deploy.py --flow xcnstock_data_pipeline.yml
```

### 3. 监控工作流

```bash
# 列出所有工作流
python kestra/monitor.py --list-flows

# 查看执行历史
python kestra/monitor.py --executions

# 实时监控执行
python kestra/monitor.py --watch --execution <execution_id>
```

## 工作流说明

| 工作流 | 用途 | 调度时间 |
|--------|------|----------|
| xcnstock_data_pipeline | 收盘后数据流水线 | 工作日 16:00 |
| xcnstock_morning_report | 盘前涨停板分析 | 工作日 09:26 |
| xcnstock_data_inspection | 数据质量巡检 | 每日 08:00 |
| xcnstock_weekly_review | 周度复盘报告 | 周日 20:00 |

## 目录结构

```
kestra/
├── lib/
│   └── kestra_client.py      # API 客户端
├── flows/                     # 工作流定义
│   ├── xcnstock_data_pipeline.yml
│   ├── xcnstock_morning_report.yml
│   ├── xcnstock_data_inspection.yml
│   └── xcnstock_weekly_review.yml
├── deploy.py                  # 部署脚本
├── monitor.py                 # 监控脚本
└── execute_flow.py            # 执行脚本
```

## 更多文档

- [需求对齐](ALIGNMENT.md) - 需求分析和边界界定
- [架构设计](DESIGN.md) - 详细架构设计
- [任务拆解](TASK_FLOW.md) - 开发任务规划
- [验收文档](ACCEPTANCE.md) - 功能验收清单
