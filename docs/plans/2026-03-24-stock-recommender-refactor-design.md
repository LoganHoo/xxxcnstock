# 股票推荐脚本重构设计文档

**日期**: 2026-03-24  
**作者**: AI Assistant  
**状态**: 已批准

---

## 1. 概述

### 1.1 背景

当前 `tomorrow_picks.py` 脚本存在以下问题：
- 使用 pandas 处理数据，性能有待提升
- 代码结构混乱，缺乏函数封装
- 没有错误处理和日志记录
- 硬编码的筛选条件和输出格式
- 只支持控制台输出

### 1.2 目标

通过轻量级重构，实现：
- ✅ 使用 Polars + DuckDB 优化性能
- ✅ 重构为类结构，提高可维护性
- ✅ 添加完整的错误处理和日志系统
- ✅ 配置化所有筛选参数
- ✅ 支持文本、HTML、JSON三种输出格式
- ✅ 可选的邮件通知功能

### 1.3 范围

**包含**：
- 重构 tomorrow_picks.py 脚本
- 更新配置文件 xcn_comm.yaml
- 添加单元测试
- 更新文档

**不包含**：
- Web界面开发
- API接口开发
- 数据库迁移

---

## 2. 架构设计

### 2.1 整体架构

```
StockRecommender (主类)
├── 配置管理 (ConfigManager)
├── 数据加载 (DataLoader)
├── 筛选引擎 (FilterEngine)
├── 报告生成 (ReportGenerator)
│   ├── 文本格式 (TextReporter)
│   ├── HTML格式 (HTMLReporter)
│   └── JSON格式 (JSONReporter)
└── 邮件通知 (EmailNotifier)
```

### 2.2 技术栈

| 组件 | 技术选择 | 原因 |
|------|----------|------|
| **数据处理** | Polars | 比pandas快10-100倍，内存效率高 |
| **复杂查询** | DuckDB | 支持SQL查询Parquet，零配置 |
| **数据存储** | Parquet | 列式存储，高压缩率 |
| **配置管理** | YAML | 人类可读，易于维护 |
| **日志系统** | logging | Python内置，功能完善 |
| **邮件发送** | smtplib | 标准库，稳定可靠 |

### 2.3 数据流

```
配置文件 → 数据加载 → 数据筛选 → 报告生成 → 输出/邮件
    ↓           ↓           ↓           ↓
 验证配置   验证数据    应用规则    格式化输出
```

---

## 3. 详细设计

### 3.1 配置管理

#### 配置文件结构 (xcn_comm.yaml)

```yaml
recommendation:
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

#### 配置管理类

```python
class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()
    
    def load_config(self) -> dict:
        """加载配置文件"""
        # 实现细节...
    
    def validate_config(self):
        """验证配置"""
        # 实现细节...
    
    def get_filter_config(self, filter_name: str) -> dict:
        """获取筛选器配置"""
        # 实现细节...
```

### 3.2 数据加载

#### 数据加载器

```python
class DataLoader:
    """数据加载器"""
    
    def __init__(self, data_path: str):
        self.data_path = data_path
        self.logger = logging.getLogger(__name__)
    
    def load_data(self) -> pl.DataFrame:
        """使用Polars加载数据"""
        try:
            df = pl.read_parquet(self.data_path)
            self.validate_data(df)
            return df
        except Exception as e:
            self.logger.error(f"数据加载失败: {e}")
            raise
    
    def validate_data(self, df: pl.DataFrame):
        """验证数据完整性"""
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score']
        missing = [f for f in required_fields if f not in df.columns]
        if missing:
            raise ValueError(f"缺少必需字段: {missing}")
```

### 3.3 筛选引擎

#### 筛选器基类

```python
class BaseFilter(ABC):
    """筛选器基类"""
    
    @abstractmethod
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        """应用筛选条件"""
        pass
```

#### 具体筛选器

```python
class SGradeFilter(BaseFilter):
    """S级股票筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        return (
            df.filter(pl.col('grade') == 'S')
            .filter(pl.col('enhanced_score') >= config['min_score'])
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )

class BullishFilter(BaseFilter):
    """多头排列筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        return (
            df.filter(pl.col('trend') == config['trend'])
            .filter(pl.col('change_pct') > config['change_pct_min'])
            .filter(pl.col('change_pct') < config['change_pct_max'])
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )
```

#### 筛选引擎

```python
class FilterEngine:
    """筛选引擎"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.filters = {
            's_grade': SGradeFilter(),
            'a_grade': AGradeFilter(),
            'bullish': BullishFilter(),
            'macd_volume': MACDVolumeFilter()
        }
    
    def apply_all_filters(self, df: pl.DataFrame) -> dict:
        """应用所有筛选器"""
        results = {}
        for filter_name, filter_obj in self.filters.items():
            config = self.config_manager.get_filter_config(filter_name)
            results[filter_name] = filter_obj.apply(df, config)
        return results
```

### 3.4 报告生成

#### 报告生成器基类

```python
class BaseReporter(ABC):
    """报告生成器基类"""
    
    @abstractmethod
    def generate(self, filter_results: dict, stats: dict) -> str:
        """生成报告"""
        pass
```

#### 文本报告生成器

```python
class TextReporter(BaseReporter):
    """文本报告生成器"""
    
    def generate(self, filter_results: dict, stats: dict) -> str:
        lines = []
        lines.append("=" * 60)
        lines.append("明日股票推荐 (基于技术分析)")
        lines.append("=" * 60)
        
        for filter_name, df in filter_results.items():
            config = self.config_manager.get_filter_config(filter_name)
            lines.append(f"\n【{config['description']}】")
            
            for row in df.iter_rows(named=True):
                change = f"+{row['change_pct']}" if row['change_pct'] >= 0 else str(row['change_pct'])
                lines.append(f"  {row['code']} {row['name']:8} {row['price']:7.2f}元 {change:>6}% 评分{row['enhanced_score']:.0f}")
        
        return "\n".join(lines)
```

#### HTML报告生成器

```python
class HTMLReporter(BaseReporter):
    """HTML报告生成器"""
    
    def generate(self, filter_results: dict, stats: dict) -> str:
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>股票推荐报告</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .stock { padding: 10px; margin: 5px 0; border: 1px solid #ddd; }
                .s-grade { background-color: #d4edda; }
                .a-grade { background-color: #fff3cd; }
            </style>
        </head>
        <body>
            <h1>明日股票推荐</h1>
            <!-- 动态生成内容 -->
        </body>
        </html>
        """
        # 实现细节...
        return html
```

#### JSON报告生成器

```python
class JSONReporter(BaseReporter):
    """JSON报告生成器"""
    
    def generate(self, filter_results: dict, stats: dict) -> str:
        report = {
            'timestamp': datetime.now().isoformat(),
            'filters': {},
            'stats': stats
        }
        
        for filter_name, df in filter_results.items():
            report['filters'][filter_name] = df.to_dicts()
        
        return json.dumps(report, ensure_ascii=False, indent=2)
```

### 3.5 邮件通知

```python
class EmailNotifier:
    """邮件通知器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def send_report(self, subject: str, content: str, html_content: str = None):
        """发送报告邮件"""
        if not self.config.get('enabled', False):
            self.logger.info("邮件通知未启用")
            return
        
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = os.getenv('SENDER_EMAIL')
            msg['To'] = ', '.join(self.config['recipients'])
            msg['Subject'] = subject
            
            msg.attach(MIMEText(content, 'plain', 'utf-8'))
            if html_content:
                msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            # 发送邮件...
            self.logger.info(f"邮件发送成功: {', '.join(self.config['recipients'])}")
        except Exception as e:
            self.logger.error(f"邮件发送失败: {e}")
            raise
```

### 3.6 主类

```python
class StockRecommender:
    """股票推荐系统"""
    
    def __init__(self, config_path: str):
        self.config_manager = ConfigManager(config_path)
        self.data_loader = DataLoader(self.config_manager.get_data_path())
        self.filter_engine = FilterEngine(self.config_manager)
        self.reporters = {
            'text': TextReporter(self.config_manager),
            'html': HTMLReporter(self.config_manager),
            'json': JSONReporter(self.config_manager)
        }
        self.email_notifier = EmailNotifier(self.config_manager.get_email_config())
        self.logger = self.setup_logger()
    
    def setup_logger(self) -> logging.Logger:
        """配置日志系统"""
        logger = logging.getLogger('StockRecommender')
        logger.setLevel(logging.INFO)
        
        # 文件处理器
        fh = logging.FileHandler('logs/recommender.log')
        fh.setLevel(logging.INFO)
        
        # 控制台处理器
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # 格式化器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def run(self):
        """执行推荐流程"""
        try:
            self.logger.info("开始股票推荐流程")
            
            # 1. 加载数据
            df = self.data_loader.load_data()
            self.logger.info(f"数据加载完成: {len(df)} 条记录")
            
            # 2. 应用筛选器
            filter_results = self.filter_engine.apply_all_filters(df)
            self.logger.info("筛选完成")
            
            # 3. 计算统计信息
            stats = self.calculate_stats(df)
            
            # 4. 生成报告
            reports = {}
            for format_name in self.config_manager.get_output_formats():
                reporter = self.reporters[format_name]
                reports[format_name] = reporter.generate(filter_results, stats)
            
            # 5. 保存报告
            self.save_reports(reports)
            
            # 6. 发送邮件
            if self.config_manager.get_email_config().get('enabled', False):
                self.email_notifier.send_report(
                    subject=f"{self.config_manager.get_email_config()['subject_prefix']} - {datetime.now().strftime('%Y-%m-%d')}",
                    content=reports['text'],
                    html_content=reports.get('html')
                )
            
            self.logger.info("股票推荐流程完成")
            
        except Exception as e:
            self.logger.error(f"推荐流程失败: {e}")
            raise
    
    def calculate_stats(self, df: pl.DataFrame) -> dict:
        """计算统计信息"""
        return {
            'total_stocks': len(df),
            's_grade_count': len(df.filter(pl.col('grade') == 'S')),
            'a_grade_count': len(df.filter(pl.col('grade') == 'A')),
            'bullish_count': len(df.filter(pl.col('trend') == 100)),
            'rising_count': len(df.filter(pl.col('change_pct') > 0))
        }
    
    def save_reports(self, reports: dict):
        """保存报告到文件"""
        output_dir = Path(self.config_manager.get_output_dir())
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d')
        prefix = self.config_manager.get_output_prefix()
        
        for format_name, content in reports.items():
            filename = f"{prefix}_{timestamp}.{format_name}"
            filepath = output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"报告已保存: {filepath}")
```

---

## 4. 错误处理和日志

### 4.1 错误处理策略

| 错误类型 | 处理方式 | 用户提示 |
|----------|----------|----------|
| 配置文件错误 | 抛出异常，记录日志 | "配置文件加载失败" |
| 数据文件不存在 | 抛出异常，记录日志 | "数据文件不存在" |
| 数据格式错误 | 抛出异常，记录日志 | "数据格式不正确" |
| 筛选条件错误 | 跳过该筛选器，记录警告 | "筛选条件无效" |
| 邮件发送失败 | 记录错误，继续执行 | "邮件发送失败" |

### 4.2 日志级别

- **DEBUG**: 详细的调试信息
- **INFO**: 正常的运行信息
- **WARNING**: 警告信息（不影响运行）
- **ERROR**: 错误信息（影响部分功能）
- **CRITICAL**: 严重错误（程序无法继续）

---

## 5. 性能优化

### 5.1 Polars优化

```python
# 使用懒加载
df = pl.scan_parquet(data_path).collect()

# 使用向量化操作
df = df.filter(pl.col('grade') == 'S')

# 避免循环
# ❌ 不推荐
for row in df.iter_rows(named=True):
    process(row)

# ✅ 推荐
df = df.with_columns(
    pl.col('change_pct').apply(lambda x: f"+{x}" if x >= 0 else str(x))
)
```

### 5.2 DuckDB优化

```python
# 复杂查询使用DuckDB
import duckdb

result = duckdb.query("""
    SELECT code, name, price, enhanced_score
    FROM df
    WHERE grade = 'S' AND enhanced_score >= 80
    ORDER BY enhanced_score DESC
    LIMIT 15
""").to_df()
```

---

## 6. 测试策略

### 6.1 单元测试

```python
import pytest
import polars as pl

class TestStockRecommender:
    
    @pytest.fixture
    def recommender(self):
        return StockRecommender('config/xcn_comm.yaml')
    
    def test_load_config(self, recommender):
        """测试配置加载"""
        assert recommender.config_manager.config is not None
    
    def test_load_data(self, recommender):
        """测试数据加载"""
        df = recommender.data_loader.load_data()
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
    
    def test_s_grade_filter(self, recommender):
        """测试S级筛选"""
        df = recommender.data_loader.load_data()
        filter_obj = SGradeFilter()
        config = recommender.config_manager.get_filter_config('s_grade')
        result = filter_obj.apply(df, config)
        
        assert all(result['grade'] == 'S')
        assert all(result['enhanced_score'] >= config['min_score'])
```

### 6.2 集成测试

```python
def test_full_workflow():
    """测试完整工作流"""
    recommender = StockRecommender('config/xcn_comm.yaml')
    recommender.run()
    
    # 验证报告文件生成
    output_dir = Path(recommender.config_manager.get_output_dir())
    assert output_dir.exists()
    
    # 验证报告内容
    for format_name in recommender.config_manager.get_output_formats():
        report_file = output_dir / f"daily_picks_*.{format_name}"
        assert len(list(output_dir.glob(report_file.name))) > 0
```

---

## 7. 部署和使用

### 7.1 安装依赖

```bash
pip install polars duckdb pyyaml
```

### 7.2 配置文件

编辑 `config/xcn_comm.yaml`，设置筛选条件和输出选项。

### 7.3 运行脚本

```bash
# 基本运行
python scripts/tomorrow_picks.py

# 启用邮件通知
# 在配置文件中设置 email.enabled: true
```

### 7.4 定时任务

```bash
# 添加到crontab
0 17 * * 1-5 cd /path/to/xcnstock && python scripts/tomorrow_picks.py
```

---

## 8. 未来扩展

### 8.1 短期扩展（1-2周）

- 添加更多筛选器（RSI、布林带等）
- 支持Excel格式输出
- 添加历史推荐记录

### 8.2 中期扩展（1-2月）

- Web界面展示
- RESTful API接口
- 实时数据推送

### 8.3 长期扩展（3-6月）

- 机器学习模型集成
- 多市场支持
- 分布式部署

---

## 9. 风险和限制

### 9.1 风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 数据质量差 | 推荐不准确 | 数据验证和清洗 |
| 配置错误 | 程序崩溃 | 配置验证和默认值 |
| 邮件发送失败 | 用户未收到通知 | 重试机制和日志记录 |

### 9.2 限制

- 仅支持A股市场
- 基于历史数据，无法预测未来
- 技术分析有局限性

---

## 10. 总结

本设计文档详细描述了股票推荐脚本的重构方案，采用轻量级架构，使用 Polars + DuckDB 优化性能，支持多种输出格式和可选的邮件通知。通过模块化设计和配置化管理，提高了代码的可维护性和可扩展性。

**关键改进**：
- ✅ 性能提升：Polars比pandas快10-100倍
- ✅ 代码质量：重构为类结构，添加错误处理和日志
- ✅ 可维护性：配置化管理，易于修改和扩展
- ✅ 用户体验：支持多种输出格式和邮件通知

**下一步**：创建详细的实施计划并开始编码。
