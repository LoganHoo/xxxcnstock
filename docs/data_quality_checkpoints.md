# 业务流数据质量检查点设计

## 概述

数据质量检查应该贯穿整个业务流，确保每个环节的输入和输出数据都符合质量标准。

## 业务流阶段与检查点

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         业务流数据质量检查点                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │  数据采集    │───▶│  数据质量检查 │───▶│  评分计算    │                  │
│  │  (Collection)│    │  (Quality)   │    │  (Scoring)   │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│         │                  │                  │                            │
│         ▼                  ▼                  ▼                            │
│    [检查点1]           [检查点2]           [检查点3]                        │
│    采集前检查          采集后验证          计算前检查                        │
│                                                                             │
│                              │                                             │
│                              ▼                                             │
│                       ┌──────────────┐                                    │
│                       │  数据质量检查 │                                    │
│                       │  (Quality)   │                                    │
│                       └──────────────┘                                    │
│                              │                                             │
│                              ▼                                             │
│                         [检查点4]                                         │
│                         计算后验证                                         │
│                              │                                             │
│                              ▼                                             │
│                       ┌──────────────┐    ┌──────────────┐                │
│                       │  选股策略    │───▶│  数据质量检查 │                │
│                       │  (Selection) │    │  (Quality)   │                │
│                       └──────────────┘    └──────────────┘                │
│                              │                  │                          │
│                              ▼                  ▼                          │
│                         [检查点5]          [检查点6]                      │
│                         选股前检查         最终输出验证                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 详细检查点设计

### 检查点1: 采集前检查 (Pre-Collection Check)

**触发时机**: 数据采集开始前

**检查内容**:
- 市场状态检查 (是否已收盘)
- 数据源可用性检查
- 存储空间检查
- 网络连接检查

**实现位置**: `workflows/data_collection_workflow.py` - `run()` 方法开头

**代码示例**:
```python
def _pre_collection_check(self, date: str) -> bool:
    """采集前检查"""
    # 1. 市场状态检查
    try:
        enforce_market_closed(target_date=datetime.strptime(date, '%Y-%m-%d'))
    except SystemExit:
        self.logger.warning("市场未收盘，跳过当日数据采集")
        return False
    
    # 2. 数据源可用性检查
    if not self.data_service.health_check():
        self.logger.error("数据源不可用")
        return False
    
    # 3. 存储空间检查
    if not self._check_storage_space():
        self.logger.error("存储空间不足")
        return False
    
    return True
```

### 检查点2: 采集后验证 (Post-Collection Validation)

**触发时机**: 数据采集完成后

**检查内容**:
- 数据完整性检查 (行数、列数)
- 数据格式检查 (类型、范围)
- 数据新鲜度检查 (日期)
- 异常值检测

**实现位置**: `workflows/data_collection_workflow.py` - `_validate_collected_data()`

**已部分实现**: 当前代码中已调用 `quality_monitor` 进行检查

**增强建议**:
```python
def _validate_collected_data(self, results: Dict[str, CollectionResult]):
    """验证采集的数据 - 增强版"""
    self.logger.info("开始数据验证")
    
    for collection_type, result in results.items():
        if result.status == "failed":
            continue
        
        # 1. 基础质量检查
        quality_result = self._run_quality_checks(collection_type)
        
        # 2. GE 验证 (Great Expectations)
        ge_result = self._run_ge_validation(collection_type)
        
        # 3. 数据血缘记录
        self._record_lineage(collection_type, result)
        
        # 4. 质量评分
        score = self._calculate_quality_score(quality_result, ge_result)
        result.details['quality_score'] = score
        result.details['quality_passed'] = score >= 80  # 阈值可配置
        
        # 5. 如果质量不达标，触发告警
        if score < 80:
            self._trigger_quality_alert(collection_type, score)
```

### 检查点3: 计算前检查 (Pre-Scoring Check)

**触发时机**: 评分计算开始前

**检查内容**:
- 数据覆盖度检查 (是否有足够的历史数据)
- 数据一致性检查 (价格、成交量是否合理)
- 缺失值检查

**实现位置**: `scripts/generate_scores.py` - `calculate_all_scores()` 开头

**代码示例**:
```python
def pre_scoring_check(df: pl.DataFrame, code: str) -> bool:
    """计算前检查"""
    # 1. 数据覆盖度检查 (至少20天数据)
    if len(df) < 20:
        logger.warning(f"{code}: 数据不足20天，跳过")
        return False
    
    # 2. 缺失值检查
    required_cols = ['close', 'volume', 'high', 'low']
    for col in required_cols:
        if col not in df.columns:
            logger.warning(f"{code}: 缺少必要列 {col}")
            return False
        if df[col].null_count() > len(df) * 0.1:  # 缺失超过10%
            logger.warning(f"{code}: {col} 缺失值过多")
            return False
    
    # 3. 数据合理性检查
    if df['close'].min() <= 0:
        logger.warning(f"{code}: 收盘价异常")
        return False
    
    return True
```

### 检查点4: 计算后验证 (Post-Scoring Validation)

**触发时机**: 评分计算完成后

**检查内容**:
- 评分分布检查 (是否异常集中)
- 极端值检查
- 评分一致性检查

**实现位置**: `scripts/generate_scores.py` - 计算完成后

**代码示例**:
```python
def post_scoring_validation(scores_df: pl.DataFrame) -> bool:
    """计算后验证"""
    # 1. 评分分布检查
    if len(scores_df) == 0:
        logger.error("评分结果为空")
        return False
    
    # 2. 检查评分范围
    if scores_df['total_score'].min() < 0 or scores_df['total_score'].max() > 100:
        logger.error("评分超出有效范围")
        return False
    
    # 3. 检查异常分布
    score_std = scores_df['total_score'].std()
    if score_std < 1:  # 标准差太小，可能所有股票评分相同
        logger.warning("评分分布异常集中")
    
    return True
```

### 检查点5: 选股前检查 (Pre-Selection Check)

**触发时机**: 选股策略开始前

**检查内容**:
- 股票池质量检查
- 数据时效性检查
- 过滤器参数检查

**实现位置**: `workflows/stock_selection_workflow.py` - `run()` 方法中

**已部分实现**: 当前代码中已加载股票池和数据

**增强建议**:
```python
def _pre_selection_check(self, stock_pool: pd.DataFrame, date: str) -> bool:
    """选股前检查"""
    # 1. 股票池大小检查
    if len(stock_pool) < 100:
        logger.error(f"股票池太小: {len(stock_pool)}")
        return False
    
    # 2. 数据时效性检查
    latest_date = stock_pool['trade_date'].max()
    if latest_date != date:
        logger.warning(f"数据日期不匹配: {latest_date} != {date}")
    
    # 3. 必要列检查
    required_cols = ['code', 'name', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in stock_pool.columns]
    if missing_cols:
        logger.error(f"缺少必要列: {missing_cols}")
        return False
    
    return True
```

### 检查点6: 最终输出验证 (Final Output Validation)

**触发时机**: 选股结果输出前

**检查内容**:
- 结果完整性检查
- 推荐股票质量检查 (排除退市、ST股票)
- 结果一致性检查

**实现位置**: `workflows/stock_selection_workflow.py` - `_prepare_output()` 中

**已部分实现**: 当前代码中已准备输出

**增强建议**:
```python
def _validate_output(self, top_stocks: pd.DataFrame) -> bool:
    """验证输出结果"""
    # 1. 结果完整性
    if len(top_stocks) == 0:
        logger.error("选股结果为空")
        return False
    
    # 2. 排除问题股票
    excluded_keywords = ['退市', 'ST', '*ST']
    for keyword in excluded_keywords:
        if top_stocks['name'].str.contains(keyword).any():
            logger.error(f"输出包含{keyword}股票")
            return False
    
    # 3. 数据新鲜度检查
    max_age_days = 30
    latest_dates = pd.to_datetime(top_stocks['latest_date'])
    if (datetime.now() - latest_dates).dt.days.max() > max_age_days:
        logger.error("推荐股票数据过旧")
        return False
    
    return True
```

## 实施建议

### 1. 优先级

| 优先级 | 检查点 | 原因 |
|:---:|:---|:---|
| P0 | 检查点2 (采集后验证) | 防止脏数据进入下游 |
| P0 | 检查点6 (最终输出验证) | 防止推荐问题股票 |
| P1 | 检查点3 (计算前检查) | 确保计算数据质量 |
| P1 | 检查点4 (计算后验证) | 确保评分结果合理 |
| P2 | 检查点1 (采集前检查) | 提前发现问题 |
| P2 | 检查点5 (选股前检查) | 确保选股基础 |

### 2. 失败处理策略

```python
class QualityCheckResult:
    """质量检查结果"""
    PASSED = "passed"      # 通过，继续执行
    WARNING = "warning"    # 警告，记录但继续
    FAILED = "failed"      # 失败，停止执行
    RETRY = "retry"        # 可重试，尝试修复
```

### 3. 告警机制

- **严重问题**: 发送告警，停止工作流
- **一般问题**: 记录日志，继续执行
- **轻微问题**: 仅记录，不告警

### 4. 监控仪表板

建议创建数据质量监控仪表板，实时展示:
- 各检查点通过率
- 数据质量评分趋势
- 异常告警列表
- 历史问题统计

## 代码集成位置

```
scripts/
├── pipeline/
│   ├── data_collect.py          # 集成检查点1
│   └── generate_scores.py       # 集成检查点3、4
├── workflows/
│   ├── data_collection_workflow.py   # 集成检查点2
│   └── stock_selection_workflow.py   # 集成检查点5、6
└── quality/
    ├── pre_collection_check.py
    ├── post_collection_validation.py
    ├── pre_scoring_check.py
    ├── post_scoring_validation.py
    ├── pre_selection_check.py
    └── final_output_validation.py
```
