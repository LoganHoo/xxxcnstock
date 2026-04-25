# DolphinScheduler 集成文档

## 1. 概述

本文档描述 xcnstock 项目与 DolphinScheduler 的集成方案，实现数据采集任务的自动化调度。

## 2. 环境配置

### 2.1 环境变量

在 `.env` 文件中配置 DolphinScheduler 连接信息：

```bash
# =============================================================================
# DolphinScheduler 配置
# =============================================================================
DOLPHINSCHEDULER_URL=http://192.168.1.168:12345
DOLPHINSCHEDULER_USER=admin
DOLPHINSCHEDULER_PASSWORD=dolphinscheduler123
DOLPHINSCHEDULER_PROJECT=xcnstock
DOLPHINSCHEDULER_TENANT=default
```

### 2.2 依赖安装

```bash
pip install apache-dolphinscheduler
```

## 3. 工作流设计

### 3.1 工作流列表

| 工作流名称 | 描述 | 调度周期 | 用途 |
|-----------|------|---------|------|
| `xcnstock_data_collection` | 历史数据采集 | 工作日 15:30 | 采集收盘后的K线数据 |
| `xcnstock_realtime_collection` | 实时数据采集 | 交易日每5分钟 | 采集实时行情快照 |
| `xcnstock_intraday_collection` | 盘中数据采集 | 交易日每分钟 | 高频采集Tick数据 |

### 3.2 历史数据采集工作流

```
┌─────────────────────────────────────────────────────────────────┐
│              xcnstock_data_collection 工作流                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐                                           │
│  │ check_market    │  检查市场状态（是否收盘）                   │
│  │ _status         │                                           │
│  └────────┬────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────┐                                           │
│  │ collect_stock   │  采集股票列表                               │
│  │ _list           │                                           │
│  └────────┬────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────┐                                           │
│  │ collect_kline   │  采集历史K线数据（并行）                    │
│  │ _data           │                                           │
│  └────────┬────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────┐                                           │
│  │ validate_data   │  数据质量验证                               │
│  │ _quality        │                                           │
│  └────────┬────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────┐                                           │
│  │ generate_       │  生成采集报告                               │
│  │ collection_     │                                           │
│  │ report          │                                           │
│  └─────────────────┘                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 实时数据采集工作流

```
┌─────────────────────────────────────────────────────────────────┐
│           xcnstock_realtime_collection 工作流                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              collect_realtime_quotes                     │   │
│  │                                                         │   │
│  │  - 采集实时行情                                          │   │
│  │  - 采集涨停池                                            │   │
│  │  - 保存到 data/realtime/                                 │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  调度: 0 */5 9-15 * * 1-5 (交易日 9-15点 每5分钟)               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.4 盘中数据采集工作流

```
┌─────────────────────────────────────────────────────────────────┐
│           xcnstock_intraday_collection 工作流                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              collect_tick_data                           │   │
│  │                                                         │   │
│  │  - 采集热门股票Tick数据                                  │   │
│  │  - 缓存到内存                                            │   │
│  │  - 批量写入 data/intraday/                               │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  调度: */1 9-15 * * 1-5 (交易日 9-15点 每分钟)                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 4. 使用指南

### 4.1 测试连接

```bash
python scripts/deploy_workflows.py --test-connection
```

### 4.2 列出工作流

```bash
python scripts/deploy_workflows.py --list
```

### 4.3 部署工作流

```bash
# 部署所有工作流
python scripts/deploy_workflows.py

# 部署指定工作流
python scripts/deploy_workflows.py --deploy data_collection
python scripts/deploy_workflows.py --deploy realtime_collection
python scripts/deploy_workflows.py --deploy intraday_collection
```

### 4.4 手动触发工作流

在 DolphinScheduler Web UI 中：
1. 进入项目 `xcnstock`
2. 选择工作流
3. 点击"启动"按钮

### 4.5 查看执行日志

```bash
# 本地日志
tail -f logs/system/dolphinscheduler.log

# 数据报告
cat data/collection_report.json
cat data/quality_report.json
```

## 5. 任务详情

### 5.1 check_market_status

**类型**: Shell

**功能**: 检查市场状态，确保已收盘

**命令**:
```bash
cd /app
python scripts/collect.py check-market
```

**失败处理**: 如果市场未收盘，任务失败

### 5.2 collect_stock_list

**类型**: Python

**功能**: 采集股票列表

**命令**:
```python
import asyncio
from services.data_service.collectors import HistoricalCollector

async def main():
    collector = HistoricalCollector()
    await collector.initialize()
    result = await collector.collect_stock_list()
    print(f'Stock list collected: {result.success}')

asyncio.run(main())
```

### 5.3 collect_kline_data

**类型**: Shell

**功能**: 采集历史K线数据

**命令**:
```bash
cd /app
python scripts/collect.py historical --mode daily
```

### 5.4 validate_data_quality

**类型**: Shell

**功能**: 验证数据质量

**命令**:
```bash
cd /app
python scripts/run_gx_validation.py --sample-size 200

# 检查成功率
if [ -f data/quality_report.json ]; then
    success_rate=$(python -c "import json; data=json.load(open('data/quality_report.json')); print(data['summary']['overall_success_rate'])")
    if (( $(echo "$success_rate < 0.90" | bc -l) )); then
        echo "警告: 数据质量低于 90%"
        exit 1
    fi
fi
```

**失败处理**: 成功率低于90%时任务失败

### 5.5 generate_collection_report

**类型**: Shell

**功能**: 生成采集报告并发送通知

**命令**:
```bash
cd /app
echo "数据采集完成"
echo "报告位置: data/collection_report.json"

# 发送通知
if [ -f scripts/send_notification.py ]; then
    python scripts/send_notification.py --subject "数据采集完成" --message "数据质量检查通过"
fi
```

## 6. 监控与告警

### 6.1 监控指标

| 指标 | 说明 | 告警阈值 |
|------|------|---------|
| 任务成功率 | 任务执行成功比例 | < 95% |
| 数据质量 | 数据验证成功率 | < 90% |
| 执行时长 | 工作流执行时间 | > 2小时 |
| 数据延迟 | 数据更新延迟 | > 1天 |

### 6.2 告警配置

在 DolphinScheduler 中配置告警组：
1. 进入"安全中心" → "告警组管理"
2. 创建告警组 `xcnstock_alerts`
3. 配置告警方式（邮件/钉钉/企业微信）
4. 在工作流中关联告警组

### 6.3 告警规则

```python
# 告警规则配置
ALERT_RULES = {
    'task_failure': {
        'condition': 'task_status == "FAILURE"',
        'level': 'ERROR',
        'message': '任务执行失败: {task_name}'
    },
    'quality_warning': {
        'condition': 'success_rate < 0.95',
        'level': 'WARNING',
        'message': '数据质量警告: 成功率 {success_rate:.1%}'
    },
    'execution_timeout': {
        'condition': 'duration > 7200',
        'level': 'WARNING',
        'message': '执行超时: {duration}秒'
    }
}
```

## 7. 故障处理

### 7.1 常见问题

**Q: 工作流部署失败？**
A: 检查：
1. DS 服务是否正常运行
2. 环境变量配置是否正确
3. 网络是否连通

**Q: 任务执行失败？**
A: 查看日志：
```bash
tail -f logs/system/dolphinscheduler.log
```

**Q: 数据质量验证失败？**
A: 运行验证脚本检查：
```bash
python scripts/run_gx_validation.py
```

### 7.2 应急处理

```bash
# 手动重新部署
python scripts/deploy_workflows.py --deploy data_collection

# 手动执行数据采集
python scripts/collect.py historical --mode daily

# 验证数据质量
python scripts/run_gx_validation.py
```

## 8. 附录

### 8.1 目录结构

```
services/scheduler/
├── __init__.py
├── dolphinscheduler_client.py    # DS 客户端
└── README.md

scripts/
├── collect.py                     # 统一采集入口
├── deploy_workflows.py            # 工作流部署脚本
└── run_gx_validation.py           # 数据验证脚本
```

### 8.2 API 参考

```python
from services.scheduler import DolphinSchedulerClient

# 创建客户端
client = DolphinSchedulerClient()

# 创建工作流
workflow = client.create_data_collection_workflow()

# 部署工作流
client.deploy_workflow(workflow)

# 部署所有工作流
client.deploy_all_workflows()
```

### 8.3 相关链接

- [DolphinScheduler 官方文档](https://dolphinscheduler.apache.org/)
- [pydolphinscheduler 文档](https://dolphinscheduler.apache.org/python/)
