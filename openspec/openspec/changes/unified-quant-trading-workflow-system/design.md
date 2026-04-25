# 统一量化交易工作流管理系统 - 设计文档

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           统一工作流管理系统                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        工作流编排层 (Workflow Orchestration)          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │   Kestra     │  │  APScheduler │  │   手动触发   │              │   │
│  │  │   (主调度)    │  │   (备份调度)  │  │   (API/CLI)  │              │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      工作流执行层 (Workflow Execution)                │   │
│  │                                                                     │   │
│  │   ┌──────────────────────────────────────────────────────────┐     │   │
│  │   │              UnifiedWorkflowManager (工作流管理器)          │     │   │
│  │   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │     │   │
│  │   │  │   状态管理   │  │   任务调度   │  │   监控告警   │        │     │   │
│  │   │  │  (SQLite)   │  │  (优先级)   │  │ (Webhook)   │        │     │   │
│  │   │  └─────────────┘  └─────────────┘  └─────────────┘        │     │   │
│  │   └──────────────────────────────────────────────────────────┘     │   │
│  │                              │                                      │   │
│  │                              ▼                                      │   │
│  │   ┌──────────────────────────────────────────────────────────┐     │   │
│  │   │              WorkflowExecutor (工作流执行器基类)            │     │   │
│  │   │  - 依赖检查 (DependencyCheck)                              │     │   │
│  │   │  - 自动重试 (RetryConfig)                                  │     │   │
│  │   │  - 断点续传 (Checkpoint)                                   │     │   │
│  │   │  - GE检查点 (GECheckpointValidators)                       │     │   │
│  │   └──────────────────────────────────────────────────────────┘     │   │
│  │                              │                                      │   │
│  │                              ▼                                      │   │
│  │   ┌──────────────────────────────────────────────────────────┐     │   │
│  │   │                    具体工作流实现                          │     │   │
│  │   │                                                          │     │   │
│  │   │  ┌─────────────────┐    ┌─────────────────┐             │     │   │
│  │   │  │  盘后工作流组    │    │  实时工作流组    │             │     │   │
│  │   │  │  (16:00-20:00) │    │  (09:26/盘中)   │             │     │   │
│  │   │  │                │    │                │             │     │   │
│  │   │  │ • 数据采集     │    │ • 涨停板分析    │             │     │   │
│  │   │  │ • 数据质检     │    │ • 盘中监控      │             │     │   │
│  │   │  │ • 数据清洗     │    │                │             │     │   │
│  │   │  │ • 复盘分析     │    │                │             │     │   │
│  │   │  │ • 选股评分     │    │                │             │     │   │
│  │   │  └─────────────────┘    └─────────────────┘             │     │   │
│  │   └──────────────────────────────────────────────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        数据存储层 (Data Storage)                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │   Parquet    │  │   SQLite     │  │    Redis     │              │   │
│  │  │  (行情数据)   │  │  (工作流状态) │  │  (缓存/锁)   │              │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 核心组件设计

### 1. UnifiedWorkflowManager (工作流管理器)

```python
class UnifiedWorkflowManager:
    """统一工作流管理器 - 中央协调器"""
    
    def __init__(self):
        self.state_db = WorkflowStateDB()  # SQLite状态存储
        self.scheduler = WorkflowScheduler()  # 任务调度器
        self.monitor = WorkflowMonitor()  # 监控器
        self.alerter = WorkflowAlerter()  # 告警器
    
    def register_workflow(self, workflow: WorkflowExecutor):
        """注册工作流"""
        pass
    
    def execute_workflow(self, workflow_id: str, params: dict) -> WorkflowResult:
        """执行工作流"""
        pass
    
    def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """获取工作流状态"""
        pass
    
    def retry_failed_workflow(self, workflow_id: str) -> bool:
        """重试失败的工作流"""
        pass
```

### 2. WorkflowExecutor (工作流执行器基类)

```python
class WorkflowExecutor(ABC):
    """工作流执行器基类 - 所有具体工作流继承此类"""
    
    def __init__(self, 
                 workflow_name: str,
                 retry_config: RetryConfig,
                 enable_checkpoint: bool = True,
                 enable_auto_fix: bool = True):
        self.workflow_name = workflow_name
        self.retry_config = retry_config
        self.enable_checkpoint = enable_checkpoint
        self.enable_auto_fix = enable_auto_fix
        self.checkpoint_validator = GECheckpointValidators()
    
    @abstractmethod
    def check_dependencies(self) -> List[DependencyCheck]:
        """检查依赖 - 子类必须实现"""
        pass
    
    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Any]:
        """执行工作流 - 子类必须实现"""
        pass
    
    def run(self, **kwargs) -> WorkflowResult:
        """运行工作流（模板方法）"""
        # 1. 检查依赖
        # 2. 执行前检查点
        # 3. 执行工作流
        # 4. 执行后检查点
        # 5. 保存状态
        pass
```

### 3. 工作流状态机

```
┌─────────┐    依赖检查    ┌─────────┐    执行     ┌─────────┐
│ PENDING │ ────────────▶ │ RUNNING │ ─────────▶ │ SUCCESS │
└─────────┘               └─────────┘            └─────────┘
     │                        │                       │
     │                        ▼                       │
     │                   ┌─────────┐                 │
     │                   │  FAILED │ ◀───────────────┘
     │                   └─────────┘    重试失败
     │                        │
     │                        ▼
     │                   ┌─────────┐
     └─────────────────▶ │  RETRY  │
          自动重试       └─────────┘
```

---

## 7大工作流详细设计

### 工作流1: UnifiedDataCollectionWorkflow (统一数据采集)

**职责**: 采集所有必要数据源

**采集顺序** (串行，避免API限流):
1. 股票列表采集 (ak.stock_zh_a_spot_em)
2. 个股K线数据采集 (ak.stock_zh_a_hist)
3. 基本面数据采集 (ak.stock_financial_report)
4. CCTV新闻联播采集 (自定义爬虫)
5. 大盘指数采集 (ak.index_zh_a_hist)
6. 外盘指数采集 (ak.index_us_stock_sina)
7. 大宗商品采集 (ak.futures_zh_realtime)

**GE检查点**:
- 检查点1: 股票列表非空 (>4000只)
- 检查点2: 个股数据完整性 (>95%)
- 检查点3: 大盘数据已更新

**输出**:
- `data/raw/YYYYMMDD/stock_list.parquet`
- `data/raw/YYYYMMDD/kline/*.parquet`
- `data/raw/YYYYMMDD/cctv_news.json`
- `data/raw/YYYYMMDD/index.parquet`

---

### 工作流2: DataQualityInspector (智能数据质检)

**职责**: 验证数据质量并自动修复

**检查项**:
| 检查项 | 规则 | 自动修复 |
|-------|------|---------|
| 新鲜度 | 数据日期 = 今日 | 无法修复，告警 |
| 完整性 | 缺失股票 < 5% | 触发补采 |
| 准确性 | 价格范围合理 | 标记异常 |
| 一致性 | 内外盘时间对齐 | 时间对齐修正 |

**修复策略**:
```python
class AutoFixStrategy:
    def fix_missing_data(self, missing_codes: List[str]):
        """缺失数据：触发补采"""
        pass
    
    def fix_abnormal_price(self, abnormal_codes: List[str]):
        """异常价格：标记并通知"""
        pass
```

**输出**:
- 质检报告 `reports/quality_report_YYYYMMDD.md`
- 修复记录 `data/quality_issues/fix_log_YYYYMMDD.json`

---

### 工作流3: DataCleaningPipeline (数据清洗流水线)

**职责**: 数据标准化和清洗

**清洗步骤**:
1. **去重**: 同一股票多源数据去重 (保留最新)
2. **标准化**: 统一字段名、日期格式、编码
3. **填充**: 缺失值前向填充/行业均值填充
4. **异常处理**: 3σ原则标记异常值
5. **类型转换**: Polars DataFrame优化

**配置示例**:
```yaml
# config/cleaning_rules.yaml
cleaning_rules:
  price:
    min: 0.01
    max: 10000
    outlier_method: "3sigma"
  volume:
    min: 0
    fill_method: "forward_fill"
```

**输出**:
- `data/cleaned/YYYYMMDD/kline_cleaned.parquet`
- 清洗统计 `data/cleaned/YYYYMMDD/cleaning_stats.json`

---

### 工作流4: ReviewAnalysisSystem (复盘分析系统)

**职责**: 昨日选股验证和大盘分析

**复盘内容**:
1. **昨日选股验证**:
   - 读取昨日选股 `data/selection_results/YYYYMMDD.parquet`
   - 计算今日收益率
   - 统计胜率、平均收益、最大回撤

2. **大盘预测验证**:
   - 对比预测 vs 实际涨跌幅
   - 计算预测准确率

3. **热门板块分析**:
   - 板块涨幅排名 (前10)
   - 板块资金流向

4. **龙虎榜解析**:
   - 游资动向
   - 机构席位买卖

**输出**:
- 复盘报告 `reports/review_report_YYYYMMDD.md`
- 更新持仓跟踪 `data/tracking/positions.db`

---

### 工作流5: MultiFactorScoringEngine (多因子选股评分)

**职责**: 综合多因子计算股票评分

**因子体系**:
```python
class FactorWeights:
    """因子权重配置"""
    TECHNICAL = 0.30    # 技术因子
    FUNDAMENTAL = 0.25  # 基本面因子
    FUND_FLOW = 0.25    # 资金因子
    SENTIMENT = 0.20    # 情绪因子

class TechnicalFactors:
    """技术因子"""
    macd_signal: float      # MACD信号
    rsi_level: float        # RSI水平
    kdj_cross: float        # KDJ金叉
    ma_alignment: float     # 均线排列

class FundamentalFactors:
    """基本面因子"""
    pe_percentile: float    # PE分位数
    pb_percentile: float    # PB分位数
    roe_growth: float       # ROE增长
    revenue_growth: float   # 营收增长
```

**评分计算**:
```
总评分 = Σ(因子值 × 因子权重)
```

**输出**:
- 评分结果 `data/scores/YYYYMMDD.parquet`
- 因子报告 `reports/factor_report_YYYYMMDD.md`

---

### 工作流6: LimitUpAnalysisSystem (涨停板分析系统) ⭐核心

**职责**: 09:26涨停板开板概率预测

**核心算法**:
```python
class LimitUpPredictor:
    """涨停板开板概率预测器"""
    
    def predict(self, stock_data: dict) -> PredictionResult:
        """
        预测开板概率
        
        因子权重:
        - 封单比 (封单金额/流通市值): 40%
        - 历史开板率 (近30日同类型): 30%
        - 板块热度: 20%
        - 大盘情绪: 10%
        """
        seal_ratio = stock_data['seal_amount'] / stock_data['float_market_cap']
        history_rate = self.get_history_open_rate(stock_data['code'])
        sector_heat = self.get_sector_heat(stock_data['sector'])
        market_sentiment = self.get_market_sentiment()
        
        open_probability = (
            seal_ratio * 0.40 +
            history_rate * 0.30 +
            sector_heat * 0.20 +
            market_sentiment * 0.10
        )
        
        return PredictionResult(
            probability=open_probability,
            grade=self.get_grade(open_probability),
            suggestion=self.get_suggestion(open_probability)
        )
```

**分级标准**:
| 等级 | 开板概率 | 封单比 | 建议 |
|-----|---------|--------|------|
| S | < 10% | > 10% | 强烈打板 |
| A | 10-30% | 5-10% | 可打板 |
| B | 30-50% | 3-5% | 谨慎观望 |
| C | > 50% | < 3% | 回避 |

**输出**:
- 涨停板报告 `reports/limit_up_YYYYMMDD.md`
- 预测数据 `data/limit_up/predictions_YYYYMMDD.parquet`

---

### 工作流7: IntradayMonitorSystem (盘中实时监控系统)

**职责**: 交易时段实时监控和信号生成

**监控周期**: 每5分钟

**监控项**:
1. **热点板块监控**:
   - 板块涨幅前10
   - 板块资金流向

2. **自选股监控**:
   - 价格突破
   - 成交量异常

3. **持仓股监控**:
   - 止盈触发
   - 止损触发
   - 涨停开板预警

**交易信号**:
```python
class TradingSignal:
    """交易信号"""
    class Type(Enum):
        OPEN = "建仓"      # 热点+突破+资金流入
        ADD = "加仓"       # 持仓+回调+缩量
        REDUCE = "减仓"    # 止盈/板块退潮
        CLOSE = "平仓"     # 止损/开板
```

**信号规则**:
```yaml
# config/trading_signals.yaml
signals:
  open:
    conditions:
      - sector_rank <= 3          # 板块前3
      - price_breakout: true      # 价格突破
      - fund_inflow: true         # 资金流入
    priority: high
  
  add:
    conditions:
      - has_position: true        # 已有持仓
      - pullback_rate >= 0.05    # 回调5%+
      - volume_shrink: true       # 缩量
    priority: medium
  
  reduce:
    conditions:
      - profit_ratio >= 0.10      # 盈利10%+
      - sector_rank_drop: true    # 板块排名下降
    priority: medium
  
  close:
    conditions:
      - loss_ratio >= 0.05        # 亏损5%+
      - limit_up_open: true       # 涨停开板
    priority: high
```

**输出**:
- 交易信号 `data/signals/YYYYMMDD.log`
- 实时推送 (企业微信)

---

## 数据流设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              数据流向图                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  16:00                    16:50                    17:00                     │
│    │                        │                        │                      │
│    ▼                        ▼                        ▼                      │
│ ┌────────┐             ┌────────┐             ┌────────┐                   │
│ │  采集   │────────────▶│  质检   │────────────▶│  清洗   │                   │
│ │        │   原始数据   │        │   质检通过   │        │                   │
│ └────────┘             └────────┘             └────────┘                   │
│    data/raw/              质检报告               data/cleaned/               │
│                                                                             │
│  17:30                    20:00                    09:26                     │
│    │                        │                        │                      │
│    ▼                        ▼                        ▼                      │
│ ┌────────┐             ┌────────┐             ┌────────┐                   │
│ │  复盘   │────────────▶│  选股   │────────────▶│  涨停板 │                   │
│ │        │   持仓更新   │        │   评分数据   │        │                   │
│ └────────┘             └────────┘             └────────┘                   │
│    reports/               data/scores/          reports/                   │
│                                                                             │
│  09:30-15:00 (每5分钟)                                                       │
│    │                                                                        │
│    ▼                                                                        │
│ ┌────────┐                                                                 │
│ │  盘中   │                                                                 │
│ │  监控   │                                                                 │
│ └────────┘                                                                 │
│    data/signals/                                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 状态管理设计

### SQLite表结构

```sql
-- 工作流执行记录
CREATE TABLE workflow_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_name TEXT NOT NULL,
    execution_id TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL,  -- PENDING, RUNNING, SUCCESS, FAILED, RETRY
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    params TEXT,  -- JSON
    result TEXT,  -- JSON
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 工作流检查点
CREATE TABLE workflow_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL,
    status TEXT NOT NULL,  -- PASS, FAIL
    details TEXT,  -- JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES workflow_executions(execution_id)
);

-- 工作流依赖
CREATE TABLE workflow_dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_name TEXT NOT NULL,
    dependency_name TEXT NOT NULL,
    status TEXT NOT NULL,  -- HEALTHY, UNHEALTHY
    message TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 监控面板设计

### 监控指标

| 指标 | 类型 | 告警阈值 |
|-----|------|---------|
| 工作流成功率 | 百分比 | < 95% |
| 平均执行时间 | 时间 | > 基准值 150% |
| 失败重试次数 | 计数 | > 3次 |
| 数据质量评分 | 百分比 | < 90% |
| 09:26任务延迟 | 时间 | > 09:30 |

### 面板布局

```
┌─────────────────────────────────────────────────────────────────┐
│                     XCNStock 工作流监控面板                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │ 今日成功率   │ │ 活跃工作流   │ │ 失败任务    │ │ 平均耗时    ││
│  │    98.5%    │ │     7      │ │     0      │ │   12.3min   ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    工作流状态一览                          │  │
│  │  ┌──────────┬──────────┬──────────┬──────────┬──────────┐ │  │
│  │  │ 工作流   │ 状态     │ 开始时间 │ 耗时     │ 结果    │ │  │
│  │  ├──────────┼──────────┼──────────┼──────────┼──────────┤ │  │
│  │  │ 数据采集 │ ✅成功   │ 16:00    │ 45min    │ 5000只  │ │  │
│  │  │ 数据质检 │ ✅成功   │ 16:50    │ 5min     │ 98分    │ │  │
│  │  │ ...      │ ...      │ ...      │ ...      │ ...     │ │  │
│  │  └──────────┴──────────┴──────────┴──────────┴──────────┘ │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────────┐ ┌─────────────────────────┐       │
│  │      数据质量趋势        │ │      工作流耗时趋势      │       │
│  │      [折线图]           │ │      [折线图]           │       │
│  └─────────────────────────┘ └─────────────────────────┘       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 告警规则设计

### 告警级别

| 级别 | 触发条件 | 通知方式 | 响应时间 |
|-----|---------|---------|---------|
| P0-紧急 | 09:26核心任务失败 | 电话+微信+邮件 | 立即 |
| P1-高 | 数据采集失败 | 微信+邮件 | 5分钟 |
| P2-中 | 数据质量评分<90% | 邮件 | 30分钟 |
| P3-低 | 工作流执行时间超长 | 邮件 | 2小时 |

### 告警模板

```markdown
## 🚨 工作流告警

**工作流**: {workflow_name}
**级别**: {alert_level}
**时间**: {timestamp}
**状态**: {status}

**详情**:
{details}

**建议操作**:
{suggested_action}

[查看详情]({dashboard_url})
```

---

## 配置管理

### 工作流配置 (config/workflows.yaml)

```yaml
workflows:
  unified_data_collection:
    enabled: true
    schedule: "0 16 * * 1-5"
    timeout: 3600
    retry: 3
    
  data_quality_inspector:
    enabled: true
    schedule: "50 16 * * 1-5"
    timeout: 600
    depends_on: unified_data_collection
    
  # ... 其他工作流
  
  limit_up_analysis:
    enabled: true
    schedule: "26 9 * * 1-5"
    timeout: 300
    priority: critical
    
  intraday_monitor:
    enabled: true
    schedule: "*/5 9-15 * * 1-5"
    timeout: 60
```

---

## 部署架构

```
┌─────────────────────────────────────────────────────────────────┐
│                          生产环境                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    Kestra 工作流引擎                        │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐         │ │
│  │  │ 盘后工作流   │ │ 09:26工作流 │ │ 盘中工作流   │         │ │
│  │  │  (16:00)   │ │  (09:26)   │ │  (循环)     │         │ │
│  │  └─────────────┘ └─────────────┘ └─────────────┘         │ │
│  └───────────────────────────────────────────────────────────┘ │
│                              │                                  │
│                              ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    工作流执行节点                          │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │              UnifiedWorkflowManager                  │ │ │
│  │  │  - 7个工作流模块                                     │ │ │
│  │  │  - Polars高性能计算                                  │ │ │
│  │  │  - SQLite状态管理                                    │ │ │
│  │  └─────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
│                              │                                  │
│                              ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                      数据存储                              │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │ │
│  │  │  Parquet │ │  SQLite  │ │  Redis   │ │  MySQL   │     │ │
│  │  │  (行情)  │ │  (状态)  │ │  (缓存)  │ │  (业务)  │     │ │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘     │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 性能指标

| 指标 | 目标值 | 测试方法 |
|-----|--------|---------|
| 数据采集时间 | < 60分钟 | 5000只股票全量采集 |
| 质检执行时间 | < 10分钟 | 全量数据质检 |
| 选股评分时间 | < 15分钟 | 5000只股票多因子计算 |
| 涨停板分析时间 | < 3分钟 | 09:26集合竞价分析 |
| 盘中监控延迟 | < 5分钟 | 信号生成到推送 |
| 系统吞吐量 | > 1000只/秒 | Polars批量处理 |

---

## 下一步

1. 审批设计文档
2. 进入任务拆解阶段 (tasks.md)
3. 开始实现工作流1-7
