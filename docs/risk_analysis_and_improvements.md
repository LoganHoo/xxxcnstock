# 收盘复盘流水线风险分析与改进方案

## 资深研究员提出的4个核心风险点

### 🚩 风险1: 16:50 data_audit 的"死循环"陷阱

**问题**: 如果交易所行情源出现结构性延迟或接口字段变更，脚本会陷入死循环

**改进方案**:
```yaml
# cron_tasks.yaml 新增配置
data_audit_unified:
  max_retry_count: 5          # 最大重试5次
  deadline_time: "17:20"      # 截止死线
  circuit_breaker_enabled: true
  fallback_mode: "partial"    # 熔断后使用部分数据模式
```

**熔断机制**:
1. 第1-3次重试: 正常补充采集
2. 第4-5次重试: 降低阈值(0.85→0.70)
3. 超过5次或超过17:20: 触发熔断，发送紧急告警
4. 熔断后: 使用"非核心数据缺失版"继续后续任务

---

### 🚩 风险2: "龙虎榜"时间错位

**问题**: 龙虎榜数据16:30-17:30才陆续发布，16:00采集时可能未出全

**改进方案**:
```yaml
# 新增独立任务
tasks:
  - name: "dragon_tiger_fetch"
    description: "【2.1.龙虎榜采集】延迟发布的龙虎榜数据"
    schedule: "30 16 * * 1-5"  # 16:30开始，每10分钟检查一次
    script: "scripts/pipeline/fetch_dragon_tiger.py --incremental"
    enabled: true
    retry_interval: 600         # 10分钟检查一次
    max_wait_time: 3600         # 最多等到17:30
    optional: true              # 标记为可选数据，不影响主流程
```

**数据标记策略**:
- 16:50审计时，龙虎榜状态为"等待中"不视为失败
- 17:30后仍未获取，标记为"数据缺失"但继续流程
- review_report中显示龙虎榜获取状态

---

### 🚩 风险3: 18:00 review_report 的"幸存者偏差"

**问题**: 只复盘成功个股会产生心理偏误

**改进方案**:
```python
# review_report.py 新增模块
def analyze_drawdowns():
    """回撤之最分析"""
    return {
        'max_loser': {
            'code': '股票代码',
            'name': '股票名称',
            'drawdown': -8.5,           # 最大回撤
            'factor_failure': '动量因子',  # 失效因子
            'market_condition': '系统性风险',  # 市场环境
            'lesson': '高动量股在大盘跳水时回撤更大'
        },
        'systematic_vs_idiosyncratic': {
            'systematic_ratio': 0.6,     # 系统性风险占比
            'factor_failure_ratio': 0.4   # 因子失效占比
        }
    }
```

**报告新增章节**:
1. **"回撤之最"**: 昨日选股中跌得最惨的3只
2. **"因子失效分析"**: 哪些因子在今日环境下失效
3. **"系统性风险检测"**: 大盘跳水导致的回撤 vs 选股错误

---

### 🚩 风险4: 缺乏"异动数据"捕捉

**问题**: 只有收盘数据，无法还原盘中跳水或脉冲过程

**改进方案**:
```yaml
# data_fetch 新增盘中快照采集
tasks:
  - name: "intraday_snapshot_fetch"
    description: "【1.2.盘中快照】采集关键时点分时数据"
    schedule: "0 16 * * 1-5"
    script: "scripts/pipeline/fetch_intraday_snapshots.py --timepoints 0935,1030,1130,1400,1410,1430,1500"
    enabled: true
```

**异动检测算法**:
```python
def detect_anomalies():
    """盘中异动检测"""
    return {
        '14:10_blue_chip_switch': {
            'time': '14:10',
            'type': '资金切换',
            'description': '资金集体切换到低位蓝筹',
            'affected_sectors': ['银行', '保险'],
            'volume_spike': 2.5  # 成交量放大倍数
        }
    }
```

---

## 优化后的任务流程

| 时间 | 任务名称 | 逻辑说明 |
|------|----------|----------|
| **16:00** | Base Fetch | 基础行情、成交量、资金流向（初版） |
| **16:30** | Dragon Tiger Fetch | 龙虎榜、融资融券、大宗交易（延迟数据） |
| **16:50** | Smart Audit | 设置Time-out(17:20)，失败时熔断并跳过非核心指标 |
| **17:15** | CVD Calculate | 计算技术指标 |
| **17:30** | Market Analysis | 引入"盘中分时形态分析" |
| **18:00** | Review Report | 增加胜率统计、盈亏比、回撤分析、情绪值 |

---

## 审计看板 (Audit Dashboard)

```json
{
  "audit_time": "2026-04-17T16:50:00",
  "deadline": "2026-04-17T17:20:00",
  "retry_count": 2,
  "circuit_breaker_status": "normal",
  "data_items": [
    {
      "item": "指数行情",
      "status": "success",
      "source": "tushare",
      "records": 120,
      "coverage": 1.0,
      "check_result": "正常"
    },
    {
      "item": "个股涨跌",
      "status": "success",
      "source": "tushare",
      "records": 5300,
      "coverage": 0.98,
      "check_result": "正常"
    },
    {
      "item": "龙虎榜",
      "status": "pending",
      "source": "eastmoney",
      "records": 0,
      "coverage": 0,
      "check_result": "等待延迟 (Retry #2)",
      "optional": true
    },
    {
      "item": "资金流向",
      "status": "success",
      "source": "tushare",
      "records": 5300,
      "coverage": 0.95,
      "check_result": "正常"
    }
  ],
  "overall_status": "passed_with_warnings",
  "fallback_activated": false
}
```

---

## 预测置信度与自我修正机制

### 置信度计算
```python
def calculate_prediction_confidence():
    """计算预测置信度"""
    return {
        'technical_confidence': 0.75,    # 技术面置信度
        'fundamental_confidence': 0.60,  # 基本面置信度
        'sentiment_confidence': 0.80,    # 情绪面置信度
        'overall_confidence': 0.72,      # 综合置信度
        'confidence_level': 'medium'     # 置信等级: high/medium/low
    }
```

### 自我修正机制
```yaml
# 08:30盘前报告覆盖18:00预测的条件
correction_triggers:
  - condition: "news_sentiment_delta > 0.3"  # 新闻情绪变化超过30%
    action: "override_prediction"
  - condition: "policy_announcement_detected"  # 检测到政策公告
    action: "override_prediction"
  - condition: "overnight_market_gap > 2%"  # 隔夜市场跳空超过2%
    action: "override_prediction"
```

---

## 实施优先级

| 优先级 | 改进项 | 预计工时 | 风险降低 |
|--------|--------|----------|----------|
| P0 | data_audit死循环防护 | 4h | 高 |
| P0 | 龙虎榜独立采集 | 6h | 高 |
| P1 | 回撤分析模块 | 8h | 中 |
| P1 | 审计看板 | 6h | 中 |
| P2 | 盘中异动捕捉 | 12h | 中 |
| P2 | 预测置信度 | 8h | 低 |
