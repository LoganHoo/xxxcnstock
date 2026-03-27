# 股票推荐系统增强设计文档

**日期**: 2026-03-24  
**作者**: AI Assistant  
**状态**: 已批准  
**方案**: 渐进式增强

---

## 1. 概述

### 1.1 背景

当前 `tomorrow_picks.py` 脚本已完成基础重构，使用 Polars + DuckDB 优化性能，支持多种输出格式和邮件通知。但缺少以下关键功能：

- ❌ 数据质量检查机制
- ❌ 市值筛选功能
- ❌ 龙头股票识别
- ❌ 量价关系分析
- ❌ 关键位计算集成

### 1.2 目标

通过渐进式增强，实现：

- ✅ 完善的数据检查机制（完整性、有效性、新鲜度、一致性）
- ✅ 市值筛选功能（30亿-500亿）
- ✅ 涨幅龙头识别
- ✅ 量价关系分析
- ✅ 关键位计算集成
- ✅ 所有参数配置化

### 1.3 设计原则

- **渐进式开发**: 分阶段实施，每个阶段独立测试
- **配置驱动**: 所有参数通过配置文件管理，禁止硬编码
- **模块化设计**: 每个功能独立模块，低耦合高内聚
- **性能优先**: 使用 Polars 优化数据处理性能
- **测试覆盖**: 每个模块包含单元测试

---

## 2. 架构设计

### 2.1 整体架构

```
StockRecommender (主类)
├── ConfigManager (配置管理器) [增强]
│   └── 所有参数配置化
├── DataValidator (数据检查器) [新增]
│   ├── 完整性检查
│   ├── 有效性检查
│   ├── 新鲜度检查
│   └── 一致性检查
├── DataLoader (数据加载器) [增强]
│   ├── 加载评分数据
│   └── 加载K线数据
├── MarketCapFilter (市值筛选器) [新增]
│   ├── 股本数据加载
│   ├── 市值计算
│   └── 市值筛选
├── LeaderStockIdentifier (龙头识别器) [新增]
│   ├── 涨幅计算
│   ├── 走势强度分析
│   └── 龙头排名
├── VolumePriceAnalyzer (量价分析器) [新增]
│   ├── 量价关系识别
│   ├── 量价背离检测
│   └── 量价形态分类
├── KeyLevelsCalculator (关键位计算器) [集成现有]
│   ├── 支撑位/压力位
│   ├── Pivot点
│   └── 斐波那契回调位
├── FilterEngine (筛选引擎) [增强]
│   └── 集成新筛选器
├── Reporters (报告生成器) [增强]
│   ├── TextReporter
│   ├── HTMLReporter
│   └── JSONReporter
└── EmailNotifier (邮件通知器)
```

### 2.2 数据流设计

```
原始数据
    ↓
DataValidator (数据检查)
    ↓ [通过检查]
MarketCapFilter (市值筛选)
    ↓ [市值范围]
LeaderStockIdentifier (龙头识别)
    ↓ [涨幅排名]
VolumePriceAnalyzer (量价分析)
    ↓ [量价形态]
KeyLevelsCalculator (关键位计算)
    ↓ [支撑/压力位]
FilterEngine (综合筛选)
    ↓ [多维度筛选]
Reporters (报告生成)
    ↓
输出 (文本/HTML/JSON + 邮件)
```

---

## 3. 模块设计

### 3.1 DataValidator (数据检查器)

**职责**: 确保数据质量，在数据处理前进行全面检查

**类设计**:
```python
class DataValidator:
    """数据检查器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_all(self, df: pl.DataFrame) -> dict:
        """执行所有检查"""
        results = {
            'completeness': self.check_completeness(df),
            'validity': self.check_validity(df),
            'freshness': self.check_freshness(df),
            'consistency': self.check_consistency(df)
        }
        return results
    
    def check_completeness(self, df: pl.DataFrame) -> dict:
        """完整性检查"""
        # 检查必需字段
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score']
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        # 检查记录数
        min_records = self.config.get('min_records', 1000)
        
        return {
            'passed': len(missing_fields) == 0 and len(df) >= min_records,
            'missing_fields': missing_fields,
            'record_count': len(df),
            'min_records': min_records
        }
    
    def check_validity(self, df: pl.DataFrame) -> dict:
        """有效性检查"""
        # 价格范围检查
        price_range = self.config.get('price_range', [0.1, 1000])
        invalid_prices = df.filter(
            (pl.col('price') < price_range[0]) | 
            (pl.col('price') > price_range[1])
        )
        
        # 涨跌幅范围检查
        change_range = self.config.get('change_pct_range', [-20, 20])
        invalid_changes = df.filter(
            (pl.col('change_pct') < change_range[0]) | 
            (pl.col('change_pct') > change_range[1])
        )
        
        return {
            'passed': len(invalid_prices) == 0 and len(invalid_changes) == 0,
            'invalid_price_count': len(invalid_prices),
            'invalid_change_count': len(invalid_changes)
        }
    
    def check_freshness(self, df: pl.DataFrame) -> dict:
        """新鲜度检查"""
        # 检查数据更新时间（需要数据中包含时间戳）
        max_age_days = self.config.get('max_age_days', 7)
        
        # 如果没有时间戳字段，跳过检查
        if 'update_time' not in df.columns:
            return {'passed': True, 'message': '无时间戳字段，跳过检查'}
        
        # 计算数据年龄
        latest_time = df['update_time'].max()
        age_days = (datetime.now() - latest_time).days
        
        return {
            'passed': age_days <= max_age_days,
            'age_days': age_days,
            'max_age_days': max_age_days
        }
    
    def check_consistency(self, df: pl.DataFrame) -> dict:
        """一致性检查"""
        # 检查评分与等级的一致性
        inconsistent_grades = df.filter(
            ((pl.col('grade') == 'S') & (pl.col('enhanced_score') < 80)) |
            ((pl.col('grade') == 'A') & ((pl.col('enhanced_score') < 75) | (pl.col('enhanced_score') >= 80)))
        )
        
        return {
            'passed': len(inconsistent_grades) == 0,
            'inconsistent_count': len(inconsistent_grades)
        }
```

**配置参数**:
```yaml
recommendation:
  data_validation:
    enabled: true
    min_records: 1000
    max_age_days: 7
    price_range: [0.1, 1000]
    change_pct_range: [-20, 20]
```

---

### 3.2 MarketCapFilter (市值筛选器)

**职责**: 根据市值范围筛选股票

**类设计**:
```python
class MarketCapFilter:
    """市值筛选器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.share_data = None
    
    def load_share_data(self) -> pl.DataFrame:
        """加载股本数据"""
        share_path = self.config.get('share_data_path')
        
        if not Path(share_path).exists():
            self.logger.warning(f"股本数据文件不存在: {share_path}")
            return None
        
        return pl.read_parquet(share_path)
    
    def calculate_market_cap(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算市值"""
        if self.share_data is None:
            self.share_data = self.load_share_data()
        
        if self.share_data is None:
            self.logger.warning("无法加载股本数据，跳过市值计算")
            return df
        
        # 合并股本数据
        df = df.join(
            self.share_data,
            on='code',
            how='left'
        )
        
        # 计算市值（亿元）
        df = df.with_columns([
            (pl.col('price') * pl.col('float_share') / 10000).alias('market_cap')
        ])
        
        return df
    
    def filter_by_cap(self, df: pl.DataFrame) -> pl.DataFrame:
        """按市值筛选"""
        if 'market_cap' not in df.columns:
            df = self.calculate_market_cap(df)
        
        min_cap = self.config.get('min_cap', 30)
        max_cap = self.config.get('max_cap', 500)
        
        return df.filter(
            (pl.col('market_cap') >= min_cap) & 
            (pl.col('market_cap') <= max_cap)
        )
```

**股本数据结构**:
```python
# data/share_structure.parquet
{
    'code': str,           # 股票代码
    'name': str,           # 股票名称
    'total_share': float,  # 总股本（万股）
    'float_share': float,  # 流通股本（万股）
    'update_time': str     # 更新时间
}
```

**配置参数**:
```yaml
recommendation:
  market_cap:
    enabled: true
    min_cap: 30  # 亿
    max_cap: 500  # 亿
    share_data_path: "data/share_structure.parquet"
```

---

### 3.3 LeaderStockIdentifier (龙头识别器)

**职责**: 识别涨幅龙头股票

**类设计**:
```python
class LeaderStockIdentifier:
    """龙头识别器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def calculate_momentum(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算动量指标"""
        # 使用已有的动量字段
        # momentum_3d, momentum_10d, momentum_20d
        
        # 综合动量评分
        df = df.with_columns([
            (
                pl.col('momentum_3d') * 0.5 + 
                pl.col('momentum_10d') * 0.3 + 
                pl.col('momentum_20d') * 0.2
            ).alias('composite_momentum')
        ])
        
        return df
    
    def rank_by_performance(self, df: pl.DataFrame) -> pl.DataFrame:
        """按表现排名"""
        # 按综合动量评分排名
        df = df.sort('composite_momentum', descending=True)
        
        # 添加排名
        df = df.with_row_count('rank', offset=1)
        
        return df
    
    def identify_leaders(self, df: pl.DataFrame) -> pl.DataFrame:
        """识别龙头股票"""
        df = self.calculate_momentum(df)
        df = self.rank_by_performance(df)
        
        # 筛选龙头股票
        min_momentum = self.config.get('min_momentum', 5.0)
        top_n = self.config.get('top_n', 10)
        
        leaders = df.filter(
            (pl.col('composite_momentum') >= min_momentum) &
            (pl.col('rank') <= top_n)
        )
        
        self.logger.info(f"识别到 {len(leaders)} 只龙头股票")
        
        return leaders
```

**配置参数**:
```yaml
recommendation:
  leader_stocks:
    enabled: true
    lookback_days: 20
    top_n: 10
    min_momentum: 5.0
```

---

### 3.4 VolumePriceAnalyzer (量价分析器)

**职责**: 分析量价关系，识别量价形态

**类设计**:
```python
class VolumePriceAnalyzer:
    """量价分析器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze_volume_price_relation(self, df: pl.DataFrame) -> pl.DataFrame:
        """分析量价关系"""
        # 计算成交量均线
        volume_ma_period = self.config.get('volume_ma_period', 5)
        
        # 需要从K线数据中获取历史成交量
        # 这里简化处理，使用已有数据
        
        # 判断量价关系
        df = df.with_columns([
            pl.when(
                (pl.col('change_pct') > 0) & 
                (pl.col('volume') > pl.col('volume').shift(1))
            )
            .then(pl.lit('量价齐升'))
            .when(
                (pl.col('change_pct') > 0) & 
                (pl.col('volume') < pl.col('volume').shift(1))
            )
            .then(pl.lit('缩量上涨'))
            .when(
                (pl.col('change_pct') < 0) & 
                (pl.col('volume') > pl.col('volume').shift(1))
            )
            .then(pl.lit('放量下跌'))
            .when(
                (pl.col('change_pct') < 0) & 
                (pl.col('volume') < pl.col('volume').shift(1))
            )
            .then(pl.lit('缩量下跌'))
            .otherwise(pl.lit('量价平稳'))
            .alias('volume_price_pattern')
        ])
        
        return df
    
    def detect_divergence(self, df: pl.DataFrame) -> pl.DataFrame:
        """检测量价背离"""
        # 简化版：价格上涨但成交量下降
        df = df.with_columns([
            pl.when(
                (pl.col('change_pct') > 2) & 
                (pl.col('volume') < pl.col('volume').shift(1) * 0.8)
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias('volume_price_divergence')
        ])
        
        return df
    
    def classify_pattern(self, df: pl.DataFrame) -> pl.DataFrame:
        """分类量价形态"""
        patterns = self.config.get('patterns', [])
        
        # 标记符合形态的股票
        for pattern in patterns:
            df = df.with_columns([
                pl.when(
                    pl.col('reasons').str.contains(pattern)
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias(f'pattern_{pattern}')
            ])
        
        return df
```

**配置参数**:
```yaml
recommendation:
  volume_price:
    enabled: true
    volume_ma_period: 5
    patterns:
      - "量价齐升"
      - "缩量上涨"
      - "放量下跌"
      - "量价背离"
```

---

### 3.5 KeyLevelsCalculator (关键位计算器)

**职责**: 集成现有的 `services/key_levels.py`

**集成方式**:
```python
from services.key_levels import KeyLevels

class KeyLevelsCalculator:
    """关键位计算器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.key_levels = KeyLevels()
        self.logger = logging.getLogger(__name__)
    
    def calculate_for_stock(self, code: str, kline_df: pl.DataFrame) -> dict:
        """计算单只股票的关键位"""
        try:
            # 转换数据格式
            closes = kline_df['close'].to_list()
            highs = kline_df['high'].to_list()
            lows = kline_df['low'].to_list()
            
            # 调用现有的关键位计算方法
            levels = self.key_levels.calculate_key_levels(closes, highs, lows)
            
            return levels
        except Exception as e:
            self.logger.error(f"计算关键位失败 {code}: {e}")
            return {}
    
    def calculate_for_all(self, df: pl.DataFrame, kline_dir: str) -> pl.DataFrame:
        """为所有股票计算关键位"""
        # 这里需要遍历所有股票，加载K线数据
        # 考虑性能，可以只计算推荐股票的关键位
        
        results = []
        for row in df.iter_rows(named=True):
            code = row['code']
            kline_path = Path(kline_dir) / f"{code}.parquet"
            
            if kline_path.exists():
                kline_df = pl.read_parquet(kline_path)
                levels = self.calculate_for_stock(code, kline_df)
                
                # 添加关键位字段
                row_dict = row.copy()
                row_dict.update({
                    'support_level': levels.get('support_recent'),
                    'resistance_level': levels.get('resistance_recent'),
                    'pivot_point': levels.get('pivot'),
                    'bb_upper': levels.get('bb_upper'),
                    'bb_lower': levels.get('bb_lower')
                })
                results.append(row_dict)
        
        return pl.DataFrame(results)
```

---

## 4. 配置设计

### 4.1 完整配置文件

```yaml
# XCNStock 通用配置文件
# 包含所有共享的配置项

# 数据路径配置
data_paths:
  kline_dir: "data/kline/"
  enhanced_scores_full: "data/enhanced_scores_full.parquet"
  share_structure: "data/share_structure.parquet"
  temp_dir: "data/temp"
  report_dir: "data/reports"

# 输出配置
output:
  log_dir: "logs"
  report_dir: "data/reports"

# 日志配置
logging:
  level: "INFO"
  format: "%(asctime)s %(levelname)s: %(message)s"

# 数据采集配置
data_collection:
  kline_update_interval: 1
  stock_list_update_interval: 1

# 股票推荐配置
recommendation:
  # 数据检查配置
  data_validation:
    enabled: true
    min_records: 1000
    max_age_days: 7
    price_range: [0.1, 1000]
    change_pct_range: [-20, 20]
  
  # 市值筛选配置
  market_cap:
    enabled: true
    min_cap: 30  # 亿
    max_cap: 500  # 亿
    share_data_path: "data/share_structure.parquet"
  
  # 龙头股票配置
  leader_stocks:
    enabled: true
    lookback_days: 20
    top_n: 10
    min_momentum: 5.0
  
  # 量价分析配置
  volume_price:
    enabled: true
    volume_ma_period: 5
    patterns:
      - "量价齐升"
      - "缩量上涨"
      - "放量下跌"
      - "量价背离"
  
  # 关键位计算配置
  key_levels:
    enabled: true
    kline_dir: "data/kline/"
  
  # 筛选条件配置
  filters:
    s_grade:
      min_score: 80
      top_n: 15
      description: "S级 - 强烈推荐"
    
    a_grade:
      min_score: 75
      max_score: 80
      top_n: 10
      description: "A级 - 建议关注"
    
    bullish:
      trend: 100
      change_pct_min: 0
      change_pct_max: 8
      top_n: 10
      description: "多头排列+今日上涨"
    
    macd_volume:
      keywords: ["MACD", "量价齐升"]
      top_n: 10
      description: "MACD金叉+量价齐升"
    
    leader:
      top_n: 10
      description: "涨幅龙头"
    
    volume_price:
      patterns: ["量价齐升"]
      top_n: 10
      description: "量价齐升"
  
  # 输出配置
  output:
    formats: ["text", "html", "json"]
    save_to_file: true
    output_dir: "reports"
    filename_prefix: "daily_picks"
  
  # 邮件配置
  email:
    enabled: false
    recipients: ["287363@qq.com"]
    subject_prefix: "XCNStock 每日推荐"
```

---

## 5. 实施计划

### 5.1 阶段划分

#### 阶段1：数据检查 + 配置管理 (1-2小时)

**任务**:
- [ ] 实现 DataValidator 类
- [ ] 更新配置文件
- [ ] 集成到 StockRecommender
- [ ] 编写单元测试
- [ ] 更新文档

**交付物**:
- ✅ DataValidator 类
- ✅ 配置文件更新
- ✅ 单元测试
- ✅ 文档更新

---

#### 阶段2：市值筛选 (1-2小时)

**任务**:
- [ ] 创建股本数据采集脚本
- [ ] 实现 MarketCapFilter 类
- [ ] 集成到 StockRecommender
- [ ] 编写单元测试
- [ ] 更新文档

**交付物**:
- ✅ 股本数据采集脚本
- ✅ MarketCapFilter 类
- ✅ 单元测试
- ✅ 文档更新

---

#### 阶段3：龙头识别 + 量价分析 (1-2小时)

**任务**:
- [ ] 实现 LeaderStockIdentifier 类
- [ ] 实现 VolumePriceAnalyzer 类
- [ ] 集成到 StockRecommender
- [ ] 编写单元测试
- [ ] 更新文档

**交付物**:
- ✅ LeaderStockIdentifier 类
- ✅ VolumePriceAnalyzer 类
- ✅ 单元测试
- ✅ 文档更新

---

#### 阶段4：关键位计算集成 (1-2小时)

**任务**:
- [ ] 实现 KeyLevelsCalculator 类
- [ ] 集成到 StockRecommender
- [ ] 更新报告格式
- [ ] 编写单元测试
- [ ] 更新文档

**交付物**:
- ✅ KeyLevelsCalculator 类
- ✅ 报告格式更新
- ✅ 单元测试
- ✅ 文档更新

---

#### 阶段5：整合测试和优化 (1-2小时)

**任务**:
- [ ] 编写集成测试
- [ ] 性能测试和优化
- [ ] 边界测试
- [ ] 文档完善
- [ ] 使用指南

**交付物**:
- ✅ 集成测试
- ✅ 性能优化
- ✅ 完整文档
- ✅ 使用指南

---

## 6. 测试策略

### 6.1 单元测试

**测试范围**:
- DataValidator 各个检查方法
- MarketCapFilter 市值计算和筛选
- LeaderStockIdentifier 龙头识别
- VolumePriceAnalyzer 量价分析
- KeyLevelsCalculator 关键位计算

**测试工具**: pytest

**覆盖率目标**: > 80%

---

### 6.2 集成测试

**测试场景**:
- 完整流程测试（数据检查 → 筛选 → 报告生成）
- 配置文件加载测试
- 异常情况测试（数据缺失、配置错误）

---

### 6.3 性能测试

**测试指标**:
- 数据加载时间 < 1秒
- 筛选处理时间 < 2秒
- 报告生成时间 < 1秒
- 总体耗时 < 5秒

---

## 7. 风险和缓解

### 7.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 股本数据获取失败 | 市值筛选失效 | 提供降级方案，跳过市值筛选 |
| K线数据缺失 | 关键位计算失败 | 只计算有数据的股票 |
| 性能下降 | 处理时间过长 | 使用 Polars 惰性求值，并行处理 |

---

### 7.2 数据风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 数据不完整 | 检查失败 | DataValidator 提前发现并报警 |
| 数据过期 | 分析结果不准确 | 新鲜度检查，自动更新提示 |
| 数据异常 | 筛选结果异常 | 有效性检查，过滤异常数据 |

---

## 8. 成功标准

### 8.1 功能完整性

- ✅ 数据检查覆盖所有维度
- ✅ 市值筛选准确
- ✅ 龙头识别合理
- ✅ 量价分析准确
- ✅ 关键位计算正确

---

### 8.2 性能要求

- ✅ 数据加载 < 1秒
- ✅ 筛选处理 < 2秒
- ✅ 报告生成 < 1秒
- ✅ 总体耗时 < 5秒

---

### 8.3 代码质量

- ✅ 单元测试覆盖率 > 80%
- ✅ 无硬编码参数
- ✅ 日志记录完整
- ✅ 错误处理健全

---

## 9. 后续扩展

### 9.1 短期扩展 (1-2周)

- 龙虎榜数据集成
- 板块轮动分析
- 资金流向分析

---

### 9.2 中期扩展 (1-2月)

- 机器学习评分模型
- 回测系统
- 实时监控系统

---

### 9.3 长期扩展 (3-6月)

- 多因子模型
- 风险管理系统
- 自动交易接口

---

## 10. 附录

### 10.1 参考资料

- Polars 官方文档: https://pola-rs.github.io/polars-book/
- DuckDB 官方文档: https://duckdb.org/docs/
- 项目规则: `.trae/rules/project_rules.md`

---

### 10.2 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|------|------|----------|------|
| 1.0 | 2026-03-24 | 初始设计文档 | AI Assistant |

---

**文档状态**: 已批准  
**下一步**: 调用 writing-plans skill 创建实施计划
