# 股票推荐脚本重构实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 重构 tomorrow_picks.py 脚本，使用 Polars + DuckDB 优化性能，支持多种输出格式和邮件通知

**Architecture:** 采用轻量级类结构，模块化设计，配置化管理，支持文本/HTML/JSON输出和可选邮件通知

**Tech Stack:** Python 3.11+, Polars, DuckDB, PyYAML, logging, smtplib

---

## 前置准备

### Task 0: 环境准备

**Files:**
- Check: `requirements.txt` 或 `pyproject.toml`

**Step 1: 检查依赖是否已安装**

Run: `python -c "import polars; import duckdb; print('Dependencies OK')"`

Expected: 输出 "Dependencies OK"

**Step 2: 如果缺少依赖，安装它们**

Run: `pip install polars duckdb pyyaml`

Expected: 成功安装

**Step 3: 创建备份**

Run: `cp scripts/tomorrow_picks.py scripts/tomorrow_picks.py.backup`

Expected: 备份文件创建成功

---

## 第一阶段：配置管理

### Task 1: 更新配置文件

**Files:**
- Modify: `config/xcn_comm.yaml`

**Step 1: 添加推荐配置**

在 `config/xcn_comm.yaml` 文件末尾添加：

```yaml
# 股票推荐配置
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

**Step 2: 验证配置文件格式**

Run: `python -c "import yaml; yaml.safe_load(open('config/xcn_comm.yaml'))"`

Expected: 无错误输出

**Step 3: 提交配置更新**

```bash
git add config/xcn_comm.yaml
git commit -m "feat: add recommendation configuration"
```

---

## 第二阶段：核心类实现

### Task 2: 创建配置管理器类

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: 添加导入语句**

在文件开头添加：

```python
"""
明日推荐股票
使用 Polars + DuckDB 优化性能
支持文本、HTML、JSON输出和邮件通知
"""
import polars as pl
import duckdb
import yaml
import json
import logging
import os
import smtplib
from pathlib import Path
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
```

**Step 2: 添加配置管理器类**

在导入语句后添加：

```python
class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config()
        self.validate_config()
        self.logger = logging.getLogger(__name__)
    
    def load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"配置文件加载失败: {e}")
    
    def validate_config(self):
        """验证配置"""
        if 'recommendation' not in self.config:
            raise ValueError("配置文件缺少 recommendation 部分")
        
        if 'filters' not in self.config['recommendation']:
            raise ValueError("配置文件缺少 filters 部分")
    
    def get_filter_config(self, filter_name: str) -> dict:
        """获取筛选器配置"""
        return self.config['recommendation']['filters'].get(filter_name, {})
    
    def get_data_path(self) -> str:
        """获取数据路径"""
        return self.config['data_paths']['enhanced_scores_full']
    
    def get_output_formats(self) -> List[str]:
        """获取输出格式列表"""
        return self.config['recommendation']['output']['formats']
    
    def get_output_dir(self) -> str:
        """获取输出目录"""
        return self.config['recommendation']['output']['output_dir']
    
    def get_output_prefix(self) -> str:
        """获取输出文件前缀"""
        return self.config['recommendation']['output']['filename_prefix']
    
    def get_email_config(self) -> dict:
        """获取邮件配置"""
        return self.config['recommendation']['email']
```

**Step 3: 测试配置管理器**

Run: `python -c "
from scripts.tomorrow_picks import ConfigManager
cm = ConfigManager('config/xcn_comm.yaml')
print('配置加载成功')
print('筛选器数量:', len(cm.config['recommendation']['filters']))
"`

Expected: 输出配置加载成功和筛选器数量

**Step 4: 提交代码**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: add ConfigManager class"
```

---

### Task 3: 创建数据加载器类

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: 添加数据加载器类**

在 ConfigManager 类后添加：

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
            self.logger.info(f"数据加载成功: {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.error(f"数据加载失败: {e}")
            raise
    
    def validate_data(self, df: pl.DataFrame):
        """验证数据完整性"""
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score', 
                          'change_pct', 'trend', 'reasons']
        missing = [f for f in required_fields if f not in df.columns]
        if missing:
            raise ValueError(f"缺少必需字段: {missing}")
```

**Step 2: 测试数据加载器**

Run: `python -c "
import logging
logging.basicConfig(level=logging.INFO)
from scripts.tomorrow_picks import ConfigManager, DataLoader
cm = ConfigManager('config/xcn_comm.yaml')
dl = DataLoader(cm.get_data_path())
df = dl.load_data()
print('数据加载成功')
print('列名:', df.columns[:5])
"`

Expected: 输出数据加载成功和列名

**Step 3: 提交代码**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: add DataLoader class"
```

---

### Task 4: 创建筛选器类

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: 添加筛选器基类**

在 DataLoader 类后添加：

```python
class BaseFilter(ABC):
    """筛选器基类"""
    
    @abstractmethod
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        """应用筛选条件"""
        pass
```

**Step 2: 添加S级筛选器**

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
```

**Step 3: 添加A级筛选器**

```python
class AGradeFilter(BaseFilter):
    """A级股票筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        return (
            df.filter(pl.col('grade') == 'A')
            .filter(pl.col('enhanced_score') >= config['min_score'])
            .filter(pl.col('enhanced_score') < config['max_score'])
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )
```

**Step 4: 添加多头排列筛选器**

```python
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

**Step 5: 添加MACD+量价筛选器**

```python
class MACDVolumeFilter(BaseFilter):
    """MACD金叉+量价齐升筛选器"""
    
    def apply(self, df: pl.DataFrame, config: dict) -> pl.DataFrame:
        keywords = config.get('keywords', [])
        df_filtered = df
        for keyword in keywords:
            df_filtered = df_filtered.filter(
                pl.col('reasons').str.contains(keyword)
            )
        return (
            df_filtered
            .sort('enhanced_score', descending=True)
            .head(config['top_n'])
        )
```

**Step 6: 添加筛选引擎**

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
        self.logger = logging.getLogger(__name__)
    
    def apply_all_filters(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """应用所有筛选器"""
        results = {}
        for filter_name, filter_obj in self.filters.items():
            try:
                config = self.config_manager.get_filter_config(filter_name)
                results[filter_name] = filter_obj.apply(df, config)
                self.logger.info(f"筛选器 {filter_name} 完成: {len(results[filter_name])} 条记录")
            except Exception as e:
                self.logger.warning(f"筛选器 {filter_name} 失败: {e}")
                results[filter_name] = pl.DataFrame()
        return results
```

**Step 7: 测试筛选器**

Run: `python -c "
import logging
logging.basicConfig(level=logging.INFO)
from scripts.tomorrow_picks import ConfigManager, DataLoader, FilterEngine
cm = ConfigManager('config/xcn_comm.yaml')
dl = DataLoader(cm.get_data_path())
df = dl.load_data()
fe = FilterEngine(cm)
results = fe.apply_all_filters(df)
print('筛选完成')
for name, result in results.items():
    print(f'{name}: {len(result)} 条')
"`

Expected: 输出各筛选器的记录数

**Step 8: 提交代码**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: add filter classes and FilterEngine"
```

---

### Task 5: 创建报告生成器类

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: 添加报告生成器基类**

在 FilterEngine 类后添加：

```python
class BaseReporter(ABC):
    """报告生成器基类"""
    
    @abstractmethod
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        """生成报告"""
        pass
```

**Step 2: 添加文本报告生成器**

```python
class TextReporter(BaseReporter):
    """文本报告生成器"""
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        lines = []
        lines.append("=" * 60)
        lines.append("明日股票推荐 (基于技术分析)")
        lines.append("=" * 60)
        
        for filter_name, df in filter_results.items():
            if len(df) == 0:
                continue
            
            config = config_manager.get_filter_config(filter_name)
            lines.append(f"\n【{config['description']}】")
            
            for row in df.iter_rows(named=True):
                change = f"+{row['change_pct']}" if row['change_pct'] >= 0 else str(row['change_pct'])
                line = f"  {row['code']} {row['name']:8} {row['price']:7.2f}元 {change:>6}% 评分{row['enhanced_score']:.0f}"
                lines.append(line)
                
                if 'reasons' in row and row['reasons']:
                    lines.append(f"    理由: {row['reasons'][:50]}...")
        
        lines.append("\n" + "=" * 60)
        lines.append("统计摘要")
        lines.append("=" * 60)
        lines.append(f"  S级: {stats['s_grade_count']} 只 (强烈推荐)")
        lines.append(f"  A级: {stats['a_grade_count']} 只 (建议关注)")
        lines.append(f"  多头排列: {stats['bullish_count']} 只")
        lines.append(f"  今日上涨: {stats['rising_count']} 只")
        
        lines.append("\n【风险提示】")
        lines.append("  以上分析基于技术指标，仅供参考，不构成投资建议。")
        lines.append("  股市有风险，投资需谨慎。")
        
        return "\n".join(lines)
```

**Step 3: 添加HTML报告生成器**

```python
class HTMLReporter(BaseReporter):
    """HTML报告生成器"""
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        html_parts = [
            '<!DOCTYPE html>',
            '<html lang="zh-CN">',
            '<head>',
            '    <meta charset="UTF-8">',
            '    <meta name="viewport" content="width=device-width, initial-scale=1.0">',
            '    <title>股票推荐报告</title>',
            '    <style>',
            '        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }',
            '        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }',
            '        h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }',
            '        .section { margin: 20px 0; padding: 15px; border-radius: 5px; }',
            '        .s-grade { background-color: #d4edda; border-left: 4px solid #28a745; }',
            '        .a-grade { background-color: #fff3cd; border-left: 4px solid #ffc107; }',
            '        .bullish { background-color: #cce5ff; border-left: 4px solid #007bff; }',
            '        .macd { background-color: #f8d7da; border-left: 4px solid #dc3545; }',
            '        .stock { padding: 8px; margin: 5px 0; background: white; border-radius: 3px; }',
            '        .code { font-weight: bold; color: #007bff; }',
            '        .name { margin-left: 10px; }',
            '        .price { margin-left: 10px; color: #28a745; }',
            '        .change { margin-left: 10px; }',
            '        .positive { color: #28a745; }',
            '        .negative { color: #dc3545; }',
            '        .score { margin-left: 10px; font-weight: bold; }',
            '        .reasons { margin-left: 20px; color: #666; font-size: 0.9em; }',
            '        .stats { background-color: #e9ecef; padding: 15px; border-radius: 5px; margin-top: 20px; }',
            '        .warning { background-color: #fff3cd; padding: 10px; border-radius: 5px; margin-top: 20px; }',
            '    </style>',
            '</head>',
            '<body>',
            '    <div class="container">',
            '        <h1>📈 明日股票推荐报告</h1>',
            '        <p>生成时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '</p>',
        ]
        
        for filter_name, df in filter_results.items():
            if len(df) == 0:
                continue
            
            config = config_manager.get_filter_config(filter_name)
            css_class = filter_name.replace('_', '-')
            
            html_parts.append(f'        <div class="section {css_class}">')
            html_parts.append(f'            <h2>{config["description"]}</h2>')
            
            for row in df.iter_rows(named=True):
                change = f"+{row['change_pct']}" if row['change_pct'] >= 0 else str(row['change_pct'])
                change_class = 'positive' if row['change_pct'] >= 0 else 'negative'
                
                html_parts.append('            <div class="stock">')
                html_parts.append(f'                <span class="code">{row["code"]}</span>')
                html_parts.append(f'                <span class="name">{row["name"]}</span>')
                html_parts.append(f'                <span class="price">{row["price"]:.2f}元</span>')
                html_parts.append(f'                <span class="change {change_class}">{change}%</span>')
                html_parts.append(f'                <span class="score">评分{row["enhanced_score"]:.0f}</span>')
                
                if 'reasons' in row and row['reasons']:
                    html_parts.append(f'                <div class="reasons">理由: {row["reasons"][:50]}...</div>')
                
                html_parts.append('            </div>')
            
            html_parts.append('        </div>')
        
        html_parts.append('        <div class="stats">')
        html_parts.append('            <h2>📊 统计摘要</h2>')
        html_parts.append(f'            <p>✅ S级: {stats["s_grade_count"]} 只 (强烈推荐)</p>')
        html_parts.append(f'            <p>✅ A级: {stats["a_grade_count"]} 只 (建议关注)</p>')
        html_parts.append(f'            <p>📈 多头排列: {stats["bullish_count"]} 只</p>')
        html_parts.append(f'            <p>⬆️  今日上涨: {stats["rising_count"]} 只</p>')
        html_parts.append('        </div>')
        
        html_parts.append('        <div class="warning">')
        html_parts.append('            <h3>⚠️ 风险提示</h3>')
        html_parts.append('            <p>以上分析基于技术指标，仅供参考，不构成投资建议。</p>')
        html_parts.append('            <p>股市有风险，投资需谨慎。</p>')
        html_parts.append('        </div>')
        
        html_parts.append('    </div>')
        html_parts.append('</body>')
        html_parts.append('</html>')
        
        return '\n'.join(html_parts)
```

**Step 4: 添加JSON报告生成器**

```python
class JSONReporter(BaseReporter):
    """JSON报告生成器"""
    
    def generate(self, filter_results: Dict[str, pl.DataFrame], 
                 stats: dict, config_manager: ConfigManager) -> str:
        report = {
            'timestamp': datetime.now().isoformat(),
            'filters': {},
            'stats': stats
        }
        
        for filter_name, df in filter_results.items():
            config = config_manager.get_filter_config(filter_name)
            report['filters'][filter_name] = {
                'description': config.get('description', ''),
                'stocks': df.to_dicts()
            }
        
        return json.dumps(report, ensure_ascii=False, indent=2)
```

**Step 5: 测试报告生成器**

Run: `python -c "
import logging
logging.basicConfig(level=logging.INFO)
from scripts.tomorrow_picks import ConfigManager, DataLoader, FilterEngine, TextReporter
cm = ConfigManager('config/xcn_comm.yaml')
dl = DataLoader(cm.get_data_path())
df = dl.load_data()
fe = FilterEngine(cm)
results = fe.apply_all_filters(df)
stats = {'s_grade_count': 100, 'a_grade_count': 200, 'bullish_count': 50, 'rising_count': 150}
reporter = TextReporter()
report = reporter.generate(results, stats, cm)
print('报告生成成功')
print(report[:200])
"`

Expected: 输出报告前200字符

**Step 6: 提交代码**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: add reporter classes (Text, HTML, JSON)"
```

---

### Task 6: 创建邮件通知类

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: 添加邮件通知类**

在 JSONReporter 类后添加：

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
            return False
        
        try:
            sender_email = os.getenv('SENDER_EMAIL')
            sender_password = os.getenv('SENDER_PASSWORD')
            
            if not sender_email or not sender_password:
                self.logger.warning("邮件配置不完整，跳过发送")
                return False
            
            msg = MIMEMultipart('alternative')
            msg['From'] = sender_email
            msg['To'] = ', '.join(self.config['recipients'])
            msg['Subject'] = subject
            
            msg.attach(MIMEText(content, 'plain', 'utf-8'))
            
            if html_content:
                msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            with smtplib.SMTP('smtp.qq.com', 587) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            self.logger.info(f"邮件发送成功: {', '.join(self.config['recipients'])}")
            return True
            
        except Exception as e:
            self.logger.error(f"邮件发送失败: {e}")
            return False
```

**Step 2: 测试邮件通知（不实际发送）**

Run: `python -c "
from scripts.tomorrow_picks import EmailNotifier
config = {'enabled': False, 'recipients': ['test@test.com']}
notifier = EmailNotifier(config)
result = notifier.send_report('测试', '测试内容')
print('邮件测试完成:', result)
"`

Expected: 输出 "邮件测试完成: False"（因为未启用）

**Step 3: 提交代码**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: add EmailNotifier class"
```

---

### Task 7: 创建主类

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: 添加主类**

在 EmailNotifier 类后添加：

```python
class StockRecommender:
    """股票推荐系统"""
    
    def __init__(self, config_path: str):
        self.config_manager = ConfigManager(config_path)
        self.data_loader = DataLoader(self.config_manager.get_data_path())
        self.filter_engine = FilterEngine(self.config_manager)
        self.reporters = {
            'text': TextReporter(),
            'html': HTMLReporter(),
            'json': JSONReporter()
        }
        self.email_notifier = EmailNotifier(self.config_manager.get_email_config())
        self.logger = self.setup_logger()
    
    def setup_logger(self) -> logging.Logger:
        """配置日志系统"""
        logger = logging.getLogger('StockRecommender')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # 文件处理器
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            fh = logging.FileHandler(log_dir / 'recommender.log')
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
            if format_name == 'text':
                filename = f"{prefix}_{timestamp}.txt"
            elif format_name == 'html':
                filename = f"{prefix}_{timestamp}.html"
            else:
                filename = f"{prefix}_{timestamp}.json"
            
            filepath = output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"报告已保存: {filepath}")
    
    def run(self):
        """执行推荐流程"""
        try:
            self.logger.info("="*70)
            self.logger.info("开始股票推荐流程")
            self.logger.info("="*70)
            
            # 1. 加载数据
            df = self.data_loader.load_data()
            
            # 2. 应用筛选器
            filter_results = self.filter_engine.apply_all_filters(df)
            
            # 3. 计算统计信息
            stats = self.calculate_stats(df)
            
            # 4. 生成报告
            reports = {}
            for format_name in self.config_manager.get_output_formats():
                if format_name in self.reporters:
                    reporter = self.reporters[format_name]
                    reports[format_name] = reporter.generate(
                        filter_results, stats, self.config_manager
                    )
            
            # 5. 保存报告
            if self.config_manager.config['recommendation']['output']['save_to_file']:
                self.save_reports(reports)
            
            # 6. 发送邮件
            email_config = self.config_manager.get_email_config()
            if email_config.get('enabled', False):
                subject = f"{email_config['subject_prefix']} - {datetime.now().strftime('%Y-%m-%d')}"
                self.email_notifier.send_report(
                    subject=subject,
                    content=reports.get('text', ''),
                    html_content=reports.get('html')
                )
            
            # 7. 输出到控制台
            if 'text' in reports:
                print(reports['text'])
            
            self.logger.info("="*70)
            self.logger.info("股票推荐流程完成")
            self.logger.info("="*70)
            
        except Exception as e:
            self.logger.error(f"推荐流程失败: {e}")
            raise
```

**Step 2: 更新主函数**

在文件末尾添加：

```python
def main():
    """主函数"""
    PROJECT_ROOT = Path(__file__).parent.parent
    CONFIG_FILE = PROJECT_ROOT / "config" / "xcn_comm.yaml"
    
    recommender = StockRecommender(str(CONFIG_FILE))
    recommender.run()


if __name__ == '__main__':
    main()
```

**Step 3: 删除旧代码**

删除旧的代码（从 `config = load_config()` 开始到文件末尾的所有旧代码）

**Step 4: 测试完整流程**

Run: `python scripts/tomorrow_picks.py`

Expected: 输出完整的推荐报告

**Step 5: 检查生成的文件**

Run: `ls -lh reports/`

Expected: 看到生成的 txt, html, json 文件

**Step 6: 提交代码**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: complete StockRecommender main class and refactor"
```

---

## 第三阶段：测试和文档

### Task 8: 创建单元测试

**Files:**
- Create: `tests/test_tomorrow_picks.py`

**Step 1: 创建测试文件**

```python
"""
股票推荐系统单元测试
"""
import pytest
import polars as pl
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from tomorrow_picks import (
    ConfigManager, DataLoader, FilterEngine,
    SGradeFilter, AGradeFilter, BullishFilter, MACDVolumeFilter,
    TextReporter, JSONReporter, StockRecommender
)


class TestConfigManager:
    """配置管理器测试"""
    
    def test_load_config(self):
        """测试配置加载"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        cm = ConfigManager(str(config_path))
        
        assert cm.config is not None
        assert 'recommendation' in cm.config
    
    def test_get_filter_config(self):
        """测试获取筛选器配置"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        cm = ConfigManager(str(config_path))
        
        s_grade_config = cm.get_filter_config('s_grade')
        assert 'min_score' in s_grade_config
        assert s_grade_config['min_score'] == 80


class TestDataLoader:
    """数据加载器测试"""
    
    def test_load_data(self):
        """测试数据加载"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        cm = ConfigManager(str(config_path))
        dl = DataLoader(cm.get_data_path())
        
        df = dl.load_data()
        
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert 'code' in df.columns
        assert 'name' in df.columns


class TestFilters:
    """筛选器测试"""
    
    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        return pl.DataFrame({
            'code': ['000001', '000002', '000003'],
            'name': ['平安银行', '万科A', '国农科技'],
            'price': [10.0, 20.0, 30.0],
            'grade': ['S', 'A', 'B'],
            'enhanced_score': [85, 78, 65],
            'change_pct': [2.5, -1.0, 5.0],
            'trend': [100, 50, 100],
            'reasons': ['MACD金叉,量价齐升', '测试', '测试']
        })
    
    def test_s_grade_filter(self, sample_data):
        """测试S级筛选器"""
        filter_obj = SGradeFilter()
        config = {'min_score': 80, 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 1
        assert result['code'][0] == '000001'
    
    def test_a_grade_filter(self, sample_data):
        """测试A级筛选器"""
        filter_obj = AGradeFilter()
        config = {'min_score': 75, 'max_score': 80, 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 1
        assert result['code'][0] == '000002'
    
    def test_bullish_filter(self, sample_data):
        """测试多头排列筛选器"""
        filter_obj = BullishFilter()
        config = {'trend': 100, 'change_pct_min': 0, 'change_pct_max': 8, 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 2  # 000001 和 000003
    
    def test_macd_volume_filter(self, sample_data):
        """测试MACD+量价筛选器"""
        filter_obj = MACDVolumeFilter()
        config = {'keywords': ['MACD', '量价齐升'], 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 1
        assert result['code'][0] == '000001'


class TestReporters:
    """报告生成器测试"""
    
    @pytest.fixture
    def sample_results(self):
        """创建测试结果"""
        return {
            's_grade': pl.DataFrame({
                'code': ['000001'],
                'name': ['平安银行'],
                'price': [10.0],
                'enhanced_score': [85],
                'change_pct': [2.5],
                'reasons': ['MACD金叉']
            })
        }
    
    @pytest.fixture
    def sample_stats(self):
        """创建测试统计"""
        return {
            'total_stocks': 100,
            's_grade_count': 10,
            'a_grade_count': 20,
            'bullish_count': 15,
            'rising_count': 50
        }
    
    def test_text_reporter(self, sample_results, sample_stats):
        """测试文本报告生成器"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        cm = ConfigManager(str(config_path))
        
        reporter = TextReporter()
        report = reporter.generate(sample_results, sample_stats, cm)
        
        assert '明日股票推荐' in report
        assert '000001' in report
    
    def test_json_reporter(self, sample_results, sample_stats):
        """测试JSON报告生成器"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        cm = ConfigManager(str(config_path))
        
        reporter = JSONReporter()
        report = reporter.generate(sample_results, sample_stats, cm)
        
        import json
        data = json.loads(report)
        
        assert 'timestamp' in data
        assert 'filters' in data
        assert 'stats' in data


class TestStockRecommender:
    """股票推荐系统测试"""
    
    def test_init(self):
        """测试初始化"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        recommender = StockRecommender(str(config_path))
        
        assert recommender.config_manager is not None
        assert recommender.data_loader is not None
        assert recommender.filter_engine is not None
    
    def test_calculate_stats(self):
        """测试统计计算"""
        config_path = Path(__file__).parent.parent / "config" / "xcn_comm.yaml"
        recommender = StockRecommender(str(config_path))
        
        df = pl.DataFrame({
            'code': ['000001', '000002'],
            'grade': ['S', 'A'],
            'trend': [100, 50],
            'change_pct': [2.5, -1.0]
        })
        
        stats = recommender.calculate_stats(df)
        
        assert stats['total_stocks'] == 2
        assert stats['s_grade_count'] == 1
        assert stats['a_grade_count'] == 1
```

**Step 2: 运行测试**

Run: `pytest tests/test_tomorrow_picks.py -v`

Expected: 所有测试通过

**Step 3: 提交测试代码**

```bash
git add tests/test_tomorrow_picks.py
git commit -m "test: add unit tests for StockRecommender"
```

---

### Task 9: 更新文档

**Files:**
- Modify: `说明文档.md`

**Step 1: 添加股票推荐系统说明**

在 `说明文档.md` 中添加：

```markdown
## 股票推荐系统

### 功能说明
基于技术分析的股票推荐系统，每日生成推荐报告。

### 核心功能
- ✅ **多维度筛选**：S级、A级、多头排列、MACD+量价齐升
- ✅ **多种输出格式**：文本、HTML、JSON
- ✅ **邮件通知**：可选的邮件推送功能
- ✅ **配置化管理**：所有参数可通过配置文件调整

### 快速开始

#### 运行推荐系统
```bash
python scripts/tomorrow_picks.py
```

#### 配置文件
编辑 `config/xcn_comm.yaml` 调整筛选条件和输出选项。

#### 启用邮件通知
在 `.env` 文件中配置：
```bash
SENDER_EMAIL=your_email@qq.com
SENDER_PASSWORD=your_authorization_code
```

在 `config/xcn_comm.yaml` 中设置：
```yaml
recommendation:
  email:
    enabled: true
    recipients: ["287363@qq.com"]
```

### 输出文件
- `reports/daily_picks_YYYYMMDD.txt` - 文本格式报告
- `reports/daily_picks_YYYYMMDD.html` - HTML格式报告
- `reports/daily_picks_YYYYMMDD.json` - JSON格式报告

### 技术栈
- **Polars**: 高性能数据处理（比pandas快10-100倍）
- **DuckDB**: 嵌入式分析数据库
- **Parquet**: 列式存储格式
```

**Step 2: 提交文档更新**

```bash
git add 说明文档.md
git commit -m "docs: update documentation for StockRecommender"
```

---

## 第四阶段：验证和优化

### Task 10: 性能测试

**Files:**
- None

**Step 1: 运行性能测试**

Run: `time python scripts/tomorrow_picks.py`

Expected: 记录执行时间

**Step 2: 对比性能**

如果之前有备份，可以对比旧版本的性能：

Run: `time python scripts/tomorrow_picks.py.backup`

Expected: 新版本应该更快

**Step 3: 记录性能数据**

在日志中记录执行时间，用于后续优化。

---

### Task 11: 清理和最终提交

**Files:**
- None

**Step 1: 删除备份文件**

Run: `rm scripts/tomorrow_picks.py.backup`

**Step 2: 最终提交**

```bash
git add -A
git commit -m "refactor: complete StockRecommender refactoring with Polars+DuckDB"
```

**Step 3: 推送到远程仓库**

```bash
git push origin main
```

---

## 完成检查清单

- [ ] 配置文件更新完成
- [ ] ConfigManager 类实现完成
- [ ] DataLoader 类实现完成
- [ ] 筛选器类实现完成
- [ ] 报告生成器类实现完成
- [ ] EmailNotifier 类实现完成
- [ ] StockRecommender 主类实现完成
- [ ] 单元测试编写完成
- [ ] 文档更新完成
- [ ] 性能测试完成
- [ ] 所有测试通过
- [ ] 代码已提交

---

## 总结

本实施计划详细描述了股票推荐脚本的重构步骤，采用 TDD 方式，每个任务都是小而可验证的步骤。通过使用 Polars + DuckDB，显著提升性能，同时提高了代码的可维护性和可扩展性。

**关键改进**：
- ✅ 性能提升：Polars比pandas快10-100倍
- ✅ 代码质量：重构为类结构，添加错误处理和日志
- ✅ 可维护性：配置化管理，易于修改和扩展
- ✅ 用户体验：支持多种输出格式和邮件通知
- ✅ 测试覆盖：完整的单元测试

**下一步**：执行实施计划，完成重构。
