# 股票推荐系统增强实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为股票推荐系统添加数据检查、市值筛选、龙头识别、量价分析和关键位计算功能

**Architecture:** 采用渐进式增强策略，分5个阶段实施。每个阶段独立开发和测试，确保功能稳定。使用 Polars 进行高性能数据处理，所有参数配置化。

**Tech Stack:** Python 3.11+, Polars, DuckDB, Pytest, YAML

---

## 阶段1：数据检查 + 配置管理

### Task 1.1: 创建 DataValidator 类

**Files:**
- Create: `services/data_validator.py`
- Test: `tests/test_data_validator.py`

**Step 1: Write the failing test**

```python
# tests/test_data_validator.py
import pytest
import polars as pl
from services.data_validator import DataValidator


def test_data_validator_init():
    """测试 DataValidator 初始化"""
    config = {
        'min_records': 1000,
        'max_age_days': 7,
        'price_range': [0.1, 1000],
        'change_pct_range': [-20, 20]
    }
    validator = DataValidator(config)
    assert validator.config == config


def test_check_completeness():
    """测试完整性检查"""
    config = {'min_records': 100}
    validator = DataValidator(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001'] * 150,
        'name': ['平安银行'] * 150,
        'price': [10.0] * 150,
        'grade': ['S'] * 150,
        'enhanced_score': [85.0] * 150
    })
    
    result = validator.check_completeness(df)
    assert result['passed'] is True
    assert result['record_count'] == 150


def test_check_validity():
    """测试有效性检查"""
    config = {
        'price_range': [0.1, 1000],
        'change_pct_range': [-20, 20]
    }
    validator = DataValidator(config)
    
    # 创建有效数据
    df = pl.DataFrame({
        'price': [10.0, 20.0, 30.0],
        'change_pct': [1.5, -2.0, 0.5]
    })
    
    result = validator.check_validity(df)
    assert result['passed'] is True


def test_check_consistency():
    """测试一致性检查"""
    config = {}
    validator = DataValidator(config)
    
    # 创建一致数据
    df = pl.DataFrame({
        'grade': ['S', 'A', 'B'],
        'enhanced_score': [85.0, 77.0, 70.0]
    })
    
    result = validator.check_consistency(df)
    assert result['passed'] is True
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_data_validator.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'services.data_validator'"

**Step 3: Write minimal implementation**

```python
# services/data_validator.py
"""数据检查器"""
import logging
import polars as pl
from typing import Dict, Any
from datetime import datetime
from pathlib import Path


class DataValidator:
    """数据检查器
    
    负责检查数据质量，包括完整性、有效性、新鲜度和一致性
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化数据检查器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_all(self, df: pl.DataFrame) -> Dict[str, Any]:
        """执行所有检查
        
        Args:
            df: 待检查的数据框
            
        Returns:
            包含所有检查结果的字典
        """
        results = {
            'completeness': self.check_completeness(df),
            'validity': self.check_validity(df),
            'freshness': self.check_freshness(df),
            'consistency': self.check_consistency(df)
        }
        
        # 汇总结果
        results['passed'] = all(
            r.get('passed', False) for r in results.values() 
            if isinstance(r, dict)
        )
        
        return results
    
    def check_completeness(self, df: pl.DataFrame) -> Dict[str, Any]:
        """完整性检查
        
        检查必需字段是否存在，记录数是否足够
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        # 检查必需字段
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score']
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        # 检查记录数
        min_records = self.config.get('min_records', 1000)
        record_count = len(df)
        
        passed = len(missing_fields) == 0 and record_count >= min_records
        
        return {
            'passed': passed,
            'missing_fields': missing_fields,
            'record_count': record_count,
            'min_records': min_records
        }
    
    def check_validity(self, df: pl.DataFrame) -> Dict[str, Any]:
        """有效性检查
        
        检查价格和涨跌幅是否在合理范围内
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        # 价格范围检查
        price_range = self.config.get('price_range', [0.1, 1000])
        invalid_prices = df.filter(
            (pl.col('price') < price_range[0]) | 
            (pl.col('price') > price_range[1])
        ) if 'price' in df.columns else pl.DataFrame()
        
        # 涨跌幅范围检查
        change_range = self.config.get('change_pct_range', [-20, 20])
        invalid_changes = df.filter(
            (pl.col('change_pct') < change_range[0]) | 
            (pl.col('change_pct') > change_range[1])
        ) if 'change_pct' in df.columns else pl.DataFrame()
        
        passed = len(invalid_prices) == 0 and len(invalid_changes) == 0
        
        return {
            'passed': passed,
            'invalid_price_count': len(invalid_prices),
            'invalid_change_count': len(invalid_changes)
        }
    
    def check_freshness(self, df: pl.DataFrame) -> Dict[str, Any]:
        """新鲜度检查
        
        检查数据是否过期
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        # 检查数据更新时间（需要数据中包含时间戳）
        max_age_days = self.config.get('max_age_days', 7)
        
        # 如果没有时间戳字段，跳过检查
        if 'update_time' not in df.columns:
            return {'passed': True, 'message': '无时间戳字段，跳过检查'}
        
        # 计算数据年龄
        latest_time = df['update_time'].max()
        if isinstance(latest_time, str):
            latest_time = datetime.fromisoformat(latest_time)
        
        age_days = (datetime.now() - latest_time).days
        
        return {
            'passed': age_days <= max_age_days,
            'age_days': age_days,
            'max_age_days': max_age_days
        }
    
    def check_consistency(self, df: pl.DataFrame) -> Dict[str, Any]:
        """一致性检查
        
        检查评分与等级的一致性
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        if 'grade' not in df.columns or 'enhanced_score' not in df.columns:
            return {'passed': True, 'message': '缺少评分或等级字段'}
        
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

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_data_validator.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add services/data_validator.py tests/test_data_validator.py
git commit -m "feat: add DataValidator class for data quality checks"
```

---

### Task 1.2: 更新配置文件

**Files:**
- Modify: `config/xcn_comm.yaml`

**Step 1: Add data validation configuration**

```yaml
# 在 recommendation 部分添加以下配置

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
```

**Step 2: Commit**

```bash
git add config/xcn_comm.yaml
git commit -m "feat: add configuration for data validation, market cap, leader stocks, volume-price analysis"
```

---

### Task 1.3: 集成 DataValidator 到 StockRecommender

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: Add import and initialization**

```python
# 在文件开头添加导入
from services.data_validator import DataValidator

# 在 StockRecommender.__init__ 方法中添加
def __init__(self, config_path: str):
    # ... 现有代码 ...
    
    # 初始化数据检查器
    validation_config = self.config_manager.config.get('recommendation', {}).get('data_validation', {})
    self.data_validator = DataValidator(validation_config) if validation_config.get('enabled', False) else None
```

**Step 2: Add validation step in run method**

```python
# 在 run 方法中，加载数据后添加
def run(self):
    """执行推荐流程"""
    try:
        self.logger.info("="*70)
        self.logger.info("开始股票推荐流程")
        self.logger.info("="*70)
        
        # 1. 加载数据
        df = self.data_loader.load_data()
        
        # 2. 数据检查（新增）
        if self.data_validator:
            self.logger.info("执行数据检查...")
            validation_results = self.data_validator.validate_all(df)
            
            if not validation_results['passed']:
                self.logger.error("数据检查失败:")
                for check_name, result in validation_results.items():
                    if isinstance(result, dict) and not result.get('passed', True):
                        self.logger.error(f"  {check_name}: {result}")
                raise ValueError("数据质量检查失败，请检查数据源")
            
            self.logger.info("数据检查通过 ✓")
        
        # 3. 应用筛选器
        filter_results = self.filter_engine.apply_all_filters(df)
        
        # ... 后续代码 ...
```

**Step 3: Run the script to test**

Run: `python scripts/tomorrow_picks.py`
Expected: Script runs successfully with data validation logs

**Step 4: Commit**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: integrate DataValidator into StockRecommender"
```

---

## 阶段2：市值筛选

### Task 2.1: 创建股本数据采集脚本

**Files:**
- Create: `scripts/fetch_share_structure.py`
- Test: `tests/test_fetch_share_structure.py`

**Step 1: Write the failing test**

```python
# tests/test_fetch_share_structure.py
import pytest
from pathlib import Path
from scripts.fetch_share_structure import ShareDataFetcher


def test_fetch_share_structure_init():
    """测试 ShareDataFetcher 初始化"""
    fetcher = ShareDataFetcher()
    assert fetcher is not None


def test_fetch_share_structure_save():
    """测试保存股本数据"""
    fetcher = ShareDataFetcher()
    
    # 模拟数据
    test_data = [
        {'code': '000001', 'name': '平安银行', 'total_share': 1940592, 'float_share': 1940592},
        {'code': '000002', 'name': '万科A', 'total_share': 1103915, 'float_share': 1103915}
    ]
    
    # 保存到临时文件
    output_path = 'data/test_share_structure.parquet'
    fetcher.save_to_parquet(test_data, output_path)
    
    # 验证文件存在
    assert Path(output_path).exists()
    
    # 清理
    Path(output_path).unlink()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetch_share_structure.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'scripts.fetch_share_structure'"

**Step 3: Write minimal implementation**

```python
# scripts/fetch_share_structure.py
"""股本数据采集脚本"""
import logging
import time
import requests
import polars as pl
from typing import List, Dict, Any
from pathlib import Path


class ShareDataFetcher:
    """股本数据采集器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch_from_eastmoney(self, code: str) -> Dict[str, Any]:
        """从东方财富获取股本数据
        
        Args:
            code: 股票代码
            
        Returns:
            股本数据字典
        """
        try:
            # 东方财富股本数据API
            url = f"http://push2.eastmoney.com/api/qt/stock/get"
            params = {
                'secid': f"{'1' if code.startswith('6') else '0'}.{code}",
                'fields': 'f57,f58,f116,f117,f118,f119,f120,f121,f122,f123'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data and 'data' in data and data['data']:
                stock_data = data['data']
                return {
                    'code': code,
                    'name': stock_data.get('f58', ''),
                    'total_share': stock_data.get('f119', 0) / 10000,  # 转换为万股
                    'float_share': stock_data.get('f120', 0) / 10000   # 转换为万股
                }
        except Exception as e:
            self.logger.error(f"获取股本数据失败 {code}: {e}")
        
        return None
    
    def fetch_all_stocks(self, stock_codes: List[str]) -> List[Dict[str, Any]]:
        """获取所有股票的股本数据
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            股本数据列表
        """
        results = []
        total = len(stock_codes)
        
        for i, code in enumerate(stock_codes, 1):
            self.logger.info(f"获取股本数据 [{i}/{total}]: {code}")
            
            data = self.fetch_from_eastmoney(code)
            if data:
                results.append(data)
            
            # 限流
            time.sleep(0.1)
        
        return results
    
    def save_to_parquet(self, data: List[Dict[str, Any]], output_path: str):
        """保存股本数据到 Parquet 文件
        
        Args:
            data: 股本数据列表
            output_path: 输出文件路径
        """
        # 确保目录存在
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 转换为 Polars DataFrame
        df = pl.DataFrame(data)
        
        # 添加更新时间
        from datetime import datetime
        df = df.with_columns([
            pl.lit(datetime.now().isoformat()).alias('update_time')
        ])
        
        # 保存
        df.write_parquet(output_path)
        self.logger.info(f"股本数据已保存到 {output_path}, 共 {len(df)} 条记录")


def main():
    """主函数"""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from config.config_manager import ConfigManager
    
    # 加载配置
    config = ConfigManager('config/xcn_comm.yaml')
    
    # 加载股票列表
    stock_list_path = config.config['data_paths']['stock_list']
    stock_list = pl.read_parquet(stock_list_path)
    stock_codes = stock_list['code'].to_list()
    
    # 获取股本数据
    fetcher = ShareDataFetcher()
    share_data = fetcher.fetch_all_stocks(stock_codes)
    
    # 保存
    output_path = config.config['recommendation']['market_cap']['share_data_path']
    fetcher.save_to_parquet(share_data, output_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_fetch_share_structure.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add scripts/fetch_share_structure.py tests/test_fetch_share_structure.py
git commit -m "feat: add share structure data fetcher"
```

---

### Task 2.2: 创建 MarketCapFilter 类

**Files:**
- Create: `services/market_cap_filter.py`
- Test: `tests/test_market_cap_filter.py`

**Step 1: Write the failing test**

```python
# tests/test_market_cap_filter.py
import pytest
import polars as pl
from services.market_cap_filter import MarketCapFilter


def test_market_cap_filter_init():
    """测试 MarketCapFilter 初始化"""
    config = {
        'min_cap': 30,
        'max_cap': 500,
        'share_data_path': 'data/share_structure.parquet'
    }
    filter = MarketCapFilter(config)
    assert filter.config == config


def test_calculate_market_cap():
    """测试市值计算"""
    config = {
        'min_cap': 30,
        'max_cap': 500,
        'share_data_path': 'data/test_share_structure.parquet'
    }
    filter = MarketCapFilter(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001', '000002'],
        'price': [10.0, 20.0]
    })
    
    # 创建股本数据
    share_data = pl.DataFrame({
        'code': ['000001', '000002'],
        'float_share': [100000.0, 200000.0]  # 万股
    })
    share_data.write_parquet('data/test_share_structure.parquet')
    
    # 计算市值
    result = filter.calculate_market_cap(df)
    
    # 验证市值列存在
    assert 'market_cap' in result.columns
    
    # 清理
    Path('data/test_share_structure.parquet').unlink()


def test_filter_by_cap():
    """测试市值筛选"""
    config = {
        'min_cap': 30,
        'max_cap': 500
    }
    filter = MarketCapFilter(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001', '000002', '000003'],
        'market_cap': [50.0, 600.0, 20.0]  # 亿
    })
    
    # 筛选
    result = filter.filter_by_cap(df)
    
    # 验证结果
    assert len(result) == 1
    assert result['code'].to_list() == ['000001']
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_market_cap_filter.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'services.market_cap_filter'"

**Step 3: Write minimal implementation**

```python
# services/market_cap_filter.py
"""市值筛选器"""
import logging
import polars as pl
from typing import Dict, Any
from pathlib import Path


class MarketCapFilter:
    """市值筛选器
    
    根据市值范围筛选股票
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化市值筛选器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.share_data = None
    
    def load_share_data(self) -> pl.DataFrame:
        """加载股本数据
        
        Returns:
            股本数据 DataFrame
        """
        share_path = self.config.get('share_data_path')
        
        if not share_path or not Path(share_path).exists():
            self.logger.warning(f"股本数据文件不存在: {share_path}")
            return None
        
        return pl.read_parquet(share_path)
    
    def calculate_market_cap(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算市值
        
        Args:
            df: 包含股价的数据框
            
        Returns:
            添加市值列的数据框
        """
        if self.share_data is None:
            self.share_data = self.load_share_data()
        
        if self.share_data is None:
            self.logger.warning("无法加载股本数据，跳过市值计算")
            return df
        
        # 合并股本数据
        df = df.join(
            self.share_data.select(['code', 'float_share']),
            on='code',
            how='left'
        )
        
        # 计算市值（亿元）= 股价 × 流通股本（万股） / 10000
        df = df.with_columns([
            (pl.col('price') * pl.col('float_share') / 10000).alias('market_cap')
        ])
        
        return df
    
    def filter_by_cap(self, df: pl.DataFrame) -> pl.DataFrame:
        """按市值筛选
        
        Args:
            df: 包含市值的数据框
            
        Returns:
            筛选后的数据框
        """
        if 'market_cap' not in df.columns:
            df = self.calculate_market_cap(df)
        
        # 如果仍然没有市值列，返回原数据
        if 'market_cap' not in df.columns:
            self.logger.warning("无法计算市值，返回原数据")
            return df
        
        min_cap = self.config.get('min_cap', 30)
        max_cap = self.config.get('max_cap', 500)
        
        # 筛选
        result = df.filter(
            (pl.col('market_cap') >= min_cap) & 
            (pl.col('market_cap') <= max_cap)
        )
        
        self.logger.info(f"市值筛选: {len(df)} -> {len(result)} (范围: {min_cap}-{max_cap}亿)")
        
        return result
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_market_cap_filter.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add services/market_cap_filter.py tests/test_market_cap_filter.py
git commit -m "feat: add MarketCapFilter class"
```

---

### Task 2.3: 集成 MarketCapFilter 到 StockRecommender

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: Add import and initialization**

```python
# 在文件开头添加导入
from services.market_cap_filter import MarketCapFilter

# 在 StockRecommender.__init__ 方法中添加
def __init__(self, config_path: str):
    # ... 现有代码 ...
    
    # 初始化市值筛选器
    market_cap_config = self.config_manager.config.get('recommendation', {}).get('market_cap', {})
    self.market_cap_filter = MarketCapFilter(market_cap_config) if market_cap_config.get('enabled', False) else None
```

**Step 2: Add market cap filtering step in run method**

```python
# 在 run 方法中，数据检查后添加
def run(self):
    """执行推荐流程"""
    try:
        # ... 数据检查代码 ...
        
        # 3. 市值筛选（新增）
        if self.market_cap_filter:
            self.logger.info("执行市值筛选...")
            df = self.market_cap_filter.filter_by_cap(df)
            self.logger.info(f"市值筛选后剩余 {len(df)} 只股票")
        
        # 4. 应用筛选器
        filter_results = self.filter_engine.apply_all_filters(df)
        
        # ... 后续代码 ...
```

**Step 3: Run the script to test**

Run: `python scripts/tomorrow_picks.py`
Expected: Script runs successfully with market cap filtering logs

**Step 4: Commit**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: integrate MarketCapFilter into StockRecommender"
```

---

## 阶段3：龙头识别 + 量价分析

### Task 3.1: 创建 LeaderStockIdentifier 类

**Files:**
- Create: `services/leader_stock_identifier.py`
- Test: `tests/test_leader_stock_identifier.py`

**Step 1: Write the failing test**

```python
# tests/test_leader_stock_identifier.py
import pytest
import polars as pl
from services.leader_stock_identifier import LeaderStockIdentifier


def test_leader_stock_identifier_init():
    """测试 LeaderStockIdentifier 初始化"""
    config = {
        'lookback_days': 20,
        'top_n': 10,
        'min_momentum': 5.0
    }
    identifier = LeaderStockIdentifier(config)
    assert identifier.config == config


def test_calculate_momentum():
    """测试动量计算"""
    config = {}
    identifier = LeaderStockIdentifier(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001', '000002', '000003'],
        'momentum_3d': [1.0, 2.0, 3.0],
        'momentum_10d': [2.0, 3.0, 4.0],
        'momentum_20d': [3.0, 4.0, 5.0]
    })
    
    # 计算动量
    result = identifier.calculate_momentum(df)
    
    # 验证综合动量列存在
    assert 'composite_momentum' in result.columns


def test_identify_leaders():
    """测试龙头识别"""
    config = {
        'top_n': 2,
        'min_momentum': 1.0
    }
    identifier = LeaderStockIdentifier(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001', '000002', '000003'],
        'momentum_3d': [1.0, 2.0, 3.0],
        'momentum_10d': [2.0, 3.0, 4.0],
        'momentum_20d': [3.0, 4.0, 5.0]
    })
    
    # 识别龙头
    leaders = identifier.identify_leaders(df)
    
    # 验证结果
    assert len(leaders) == 2
    assert 'rank' in leaders.columns
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_leader_stock_identifier.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'services.leader_stock_identifier'"

**Step 3: Write minimal implementation**

```python
# services/leader_stock_identifier.py
"""龙头股票识别器"""
import logging
import polars as pl
from typing import Dict, Any


class LeaderStockIdentifier:
    """龙头股票识别器
    
    识别涨幅龙头股票
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化龙头识别器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def calculate_momentum(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算综合动量指标
        
        Args:
            df: 包含动量数据的数据框
            
        Returns:
            添加综合动量列的数据框
        """
        # 检查必需字段
        required_fields = ['momentum_3d', 'momentum_10d', 'momentum_20d']
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        if missing_fields:
            self.logger.warning(f"缺少动量字段: {missing_fields}")
            return df
        
        # 计算综合动量评分
        df = df.with_columns([
            (
                pl.col('momentum_3d') * 0.5 + 
                pl.col('momentum_10d') * 0.3 + 
                pl.col('momentum_20d') * 0.2
            ).alias('composite_momentum')
        ])
        
        return df
    
    def rank_by_performance(self, df: pl.DataFrame) -> pl.DataFrame:
        """按表现排名
        
        Args:
            df: 包含综合动量的数据框
            
        Returns:
            添加排名的数据框
        """
        if 'composite_momentum' not in df.columns:
            df = self.calculate_momentum(df)
        
        # 按综合动量评分排名
        df = df.sort('composite_momentum', descending=True)
        
        # 添加排名
        df = df.with_row_count('rank', offset=1)
        
        return df
    
    def identify_leaders(self, df: pl.DataFrame) -> pl.DataFrame:
        """识别龙头股票
        
        Args:
            df: 待分析的数据框
            
        Returns:
            龙头股票数据框
        """
        # 计算动量
        df = self.calculate_momentum(df)
        
        # 排名
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

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_leader_stock_identifier.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add services/leader_stock_identifier.py tests/test_leader_stock_identifier.py
git commit -m "feat: add LeaderStockIdentifier class"
```

---

### Task 3.2: 创建 VolumePriceAnalyzer 类

**Files:**
- Create: `services/volume_price_analyzer.py`
- Test: `tests/test_volume_price_analyzer.py`

**Step 1: Write the failing test**

```python
# tests/test_volume_price_analyzer.py
import pytest
import polars as pl
from services.volume_price_analyzer import VolumePriceAnalyzer


def test_volume_price_analyzer_init():
    """测试 VolumePriceAnalyzer 初始化"""
    config = {
        'volume_ma_period': 5,
        'patterns': ['量价齐升', '缩量上涨']
    }
    analyzer = VolumePriceAnalyzer(config)
    assert analyzer.config == config


def test_analyze_volume_price_relation():
    """测试量价关系分析"""
    config = {}
    analyzer = VolumePriceAnalyzer(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001', '000002', '000003'],
        'change_pct': [1.5, -2.0, 0.5],
        'volume': [1000000, 800000, 1200000],
        'reasons': ['量价齐升', '放量下跌', '缩量上涨']
    })
    
    # 分析量价关系
    result = analyzer.analyze_volume_price_relation(df)
    
    # 验证量价形态列存在
    assert 'volume_price_pattern' in result.columns


def test_classify_pattern():
    """测试量价形态分类"""
    config = {
        'patterns': ['量价齐升', '缩量上涨']
    }
    analyzer = VolumePriceAnalyzer(config)
    
    # 创建测试数据
    df = pl.DataFrame({
        'code': ['000001', '000002', '000003'],
        'reasons': ['量价齐升,MACD金叉', '放量下跌', '缩量上涨,突破']
    })
    
    # 分类量价形态
    result = analyzer.classify_pattern(df)
    
    # 验证形态列存在
    assert 'pattern_量价齐升' in result.columns
    assert 'pattern_缩量上涨' in result.columns
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_volume_price_analyzer.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'services.volume_price_analyzer'"

**Step 3: Write minimal implementation**

```python
# services/volume_price_analyzer.py
"""量价分析器"""
import logging
import polars as pl
from typing import Dict, Any, List


class VolumePriceAnalyzer:
    """量价分析器
    
    分析量价关系，识别量价形态
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化量价分析器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze_volume_price_relation(self, df: pl.DataFrame) -> pl.DataFrame:
        """分析量价关系
        
        Args:
            df: 包含价格和成交量的数据框
            
        Returns:
            添加量价形态列的数据框
        """
        # 检查必需字段
        if 'change_pct' not in df.columns or 'volume' not in df.columns:
            self.logger.warning("缺少价格或成交量字段")
            return df
        
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
        """检测量价背离
        
        Args:
            df: 包含价格和成交量的数据框
            
        Returns:
            添加量价背离列的数据框
        """
        if 'change_pct' not in df.columns or 'volume' not in df.columns:
            return df
        
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
        """分类量价形态
        
        Args:
            df: 包含原因字段的数据框
            
        Returns:
            添加形态标记的数据框
        """
        if 'reasons' not in df.columns:
            self.logger.warning("缺少原因字段")
            return df
        
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
    
    def analyze_all(self, df: pl.DataFrame) -> pl.DataFrame:
        """执行所有量价分析
        
        Args:
            df: 待分析的数据框
            
        Returns:
            添加所有量价分析结果的数据框
        """
        df = self.analyze_volume_price_relation(df)
        df = self.detect_divergence(df)
        df = self.classify_pattern(df)
        
        return df
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_volume_price_analyzer.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add services/volume_price_analyzer.py tests/test_volume_price_analyzer.py
git commit -m "feat: add VolumePriceAnalyzer class"
```

---

### Task 3.3: 集成龙头识别和量价分析到 StockRecommender

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: Add imports and initialization**

```python
# 在文件开头添加导入
from services.leader_stock_identifier import LeaderStockIdentifier
from services.volume_price_analyzer import VolumePriceAnalyzer

# 在 StockRecommender.__init__ 方法中添加
def __init__(self, config_path: str):
    # ... 现有代码 ...
    
    # 初始化龙头识别器
    leader_config = self.config_manager.config.get('recommendation', {}).get('leader_stocks', {})
    self.leader_identifier = LeaderStockIdentifier(leader_config) if leader_config.get('enabled', False) else None
    
    # 初始化量价分析器
    volume_price_config = self.config_manager.config.get('recommendation', {}).get('volume_price', {})
    self.volume_price_analyzer = VolumePriceAnalyzer(volume_price_config) if volume_price_config.get('enabled', False) else None
```

**Step 2: Add leader identification and volume-price analysis in run method**

```python
# 在 run 方法中，市值筛选后添加
def run(self):
    """执行推荐流程"""
    try:
        # ... 市值筛选代码 ...
        
        # 5. 龙头识别（新增）
        if self.leader_identifier:
            self.logger.info("执行龙头识别...")
            leaders = self.leader_identifier.identify_leaders(df)
            self.logger.info(f"识别到 {len(leaders)} 只龙头股票")
        
        # 6. 量价分析（新增）
        if self.volume_price_analyzer:
            self.logger.info("执行量价分析...")
            df = self.volume_price_analyzer.analyze_all(df)
            self.logger.info("量价分析完成")
        
        # 7. 应用筛选器
        filter_results = self.filter_engine.apply_all_filters(df)
        
        # ... 后续代码 ...
```

**Step 3: Run the script to test**

Run: `python scripts/tomorrow_picks.py`
Expected: Script runs successfully with leader identification and volume-price analysis logs

**Step 4: Commit**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: integrate LeaderStockIdentifier and VolumePriceAnalyzer into StockRecommender"
```

---

## 阶段4：关键位计算集成

### Task 4.1: 创建 KeyLevelsCalculator 类

**Files:**
- Create: `services/key_levels_calculator.py`
- Test: `tests/test_key_levels_calculator.py`

**Step 1: Write the failing test**

```python
# tests/test_key_levels_calculator.py
import pytest
import polars as pl
from services.key_levels_calculator import KeyLevelsCalculator


def test_key_levels_calculator_init():
    """测试 KeyLevelsCalculator 初始化"""
    config = {
        'kline_dir': 'data/kline/'
    }
    calculator = KeyLevelsCalculator(config)
    assert calculator.config == config


def test_calculate_for_stock():
    """测试单只股票关键位计算"""
    config = {'kline_dir': 'data/kline/'}
    calculator = KeyLevelsCalculator(config)
    
    # 创建测试K线数据
    kline_df = pl.DataFrame({
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'open': [10.0, 10.5, 11.0],
        'high': [10.5, 11.0, 11.5],
        'low': [9.8, 10.2, 10.8],
        'close': [10.3, 10.8, 11.2]
    })
    
    # 计算关键位
    levels = calculator.calculate_for_stock('000001', kline_df)
    
    # 验证关键位字段存在
    assert 'support_recent' in levels or 'error' in levels
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_key_levels_calculator.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'services.key_levels_calculator'"

**Step 3: Write minimal implementation**

```python
# services/key_levels_calculator.py
"""关键位计算器"""
import logging
import polars as pl
from typing import Dict, Any
from pathlib import Path
from services.key_levels import KeyLevels


class KeyLevelsCalculator:
    """关键位计算器
    
    集成现有的 KeyLevels 类
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化关键位计算器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.key_levels = KeyLevels()
        self.logger = logging.getLogger(__name__)
    
    def calculate_for_stock(self, code: str, kline_df: pl.DataFrame) -> Dict[str, Any]:
        """计算单只股票的关键位
        
        Args:
            code: 股票代码
            kline_df: K线数据 DataFrame
            
        Returns:
            关键位字典
        """
        try:
            # 检查数据长度
            if len(kline_df) < 5:
                return {'error': '数据不足'}
            
            # 转换数据格式
            closes = kline_df['close'].to_list()
            highs = kline_df['high'].to_list()
            lows = kline_df['low'].to_list()
            
            # 调用现有的关键位计算方法
            levels = self.key_levels.calculate_key_levels(closes, highs, lows)
            
            return levels
        except Exception as e:
            self.logger.error(f"计算关键位失败 {code}: {e}")
            return {'error': str(e)}
    
    def calculate_for_all(self, df: pl.DataFrame) -> pl.DataFrame:
        """为所有股票计算关键位
        
        Args:
            df: 包含股票代码的数据框
            
        Returns:
            添加关键位字段的数据框
        """
        kline_dir = self.config.get('kline_dir', 'data/kline/')
        
        # 这里需要遍历所有股票，加载K线数据
        # 考虑性能，可以只计算推荐股票的关键位
        
        results = []
        for row in df.iter_rows(named=True):
            code = row['code']
            kline_path = Path(kline_dir) / f"{code}.parquet"
            
            row_dict = dict(row)
            
            if kline_path.exists():
                kline_df = pl.read_parquet(kline_path)
                levels = self.calculate_for_stock(code, kline_df)
                
                # 添加关键位字段
                row_dict.update({
                    'support_level': levels.get('support_recent'),
                    'resistance_level': levels.get('resistance_recent'),
                    'pivot_point': levels.get('pivot'),
                    'bb_upper': levels.get('bb_upper'),
                    'bb_lower': levels.get('bb_lower')
                })
            else:
                # K线数据不存在，设置为 None
                row_dict.update({
                    'support_level': None,
                    'resistance_level': None,
                    'pivot_point': None,
                    'bb_upper': None,
                    'bb_lower': None
                })
            
            results.append(row_dict)
        
        return pl.DataFrame(results)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_key_levels_calculator.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add services/key_levels_calculator.py tests/test_key_levels_calculator.py
git commit -m "feat: add KeyLevelsCalculator class"
```

---

### Task 4.2: 集成 KeyLevelsCalculator 到 StockRecommender

**Files:**
- Modify: `scripts/tomorrow_picks.py`

**Step 1: Add import and initialization**

```python
# 在文件开头添加导入
from services.key_levels_calculator import KeyLevelsCalculator

# 在 StockRecommender.__init__ 方法中添加
def __init__(self, config_path: str):
    # ... 现有代码 ...
    
    # 初始化关键位计算器
    key_levels_config = self.config_manager.config.get('recommendation', {}).get('key_levels', {})
    self.key_levels_calculator = KeyLevelsCalculator(key_levels_config) if key_levels_config.get('enabled', False) else None
```

**Step 2: Add key levels calculation in run method**

```python
# 在 run 方法中，量价分析后添加
def run(self):
    """执行推荐流程"""
    try:
        # ... 量价分析代码 ...
        
        # 8. 关键位计算（新增）
        if self.key_levels_calculator:
            self.logger.info("执行关键位计算...")
            df = self.key_levels_calculator.calculate_for_all(df)
            self.logger.info("关键位计算完成")
        
        # 9. 应用筛选器
        filter_results = self.filter_engine.apply_all_filters(df)
        
        # ... 后续代码 ...
```

**Step 3: Update report format to include key levels**

```python
# 在 TextReporter.generate 方法中添加关键位显示
def generate(self, filter_results: Dict, stats: Dict, config_manager) -> str:
    """生成文本报告"""
    # ... 现有代码 ...
    
    # 在每只股票信息后添加关键位
    for stock in stocks[:top_n]:
        report += f"  {stock['code']} {stock['name']}  {stock['price']}元 {change_pct_str} 评分{stock['enhanced_score']}\n"
        report += f"    理由: {stock['reasons']}\n"
        
        # 添加关键位信息
        if 'support_level' in stock and stock['support_level']:
            report += f"    支撑位: {stock['support_level']:.2f}\n"
        if 'resistance_level' in stock and stock['resistance_level']:
            report += f"    压力位: {stock['resistance_level']:.2f}\n"
        
        report += "\n"
    
    # ... 后续代码 ...
```

**Step 4: Run the script to test**

Run: `python scripts/tomorrow_picks.py`
Expected: Script runs successfully with key levels calculation and display in report

**Step 5: Commit**

```bash
git add scripts/tomorrow_picks.py
git commit -m "feat: integrate KeyLevelsCalculator into StockRecommender and update report format"
```

---

## 阶段5：整合测试和优化

### Task 5.1: 编写集成测试

**Files:**
- Create: `tests/test_integration_tomorrow_picks.py`

**Step 1: Write integration test**

```python
# tests/test_integration_tomorrow_picks.py
"""集成测试"""
import pytest
import polars as pl
from pathlib import Path
from scripts.tomorrow_picks import StockRecommender


def test_full_workflow():
    """测试完整工作流"""
    # 创建配置文件
    config_path = 'config/xcn_comm.yaml'
    
    # 初始化推荐器
    recommender = StockRecommender(config_path)
    
    # 运行推荐流程
    try:
        recommender.run()
        assert True  # 如果没有抛出异常，则测试通过
    except Exception as e:
        pytest.fail(f"推荐流程失败: {e}")


def test_data_validation():
    """测试数据检查"""
    config_path = 'config/xcn_comm.yaml'
    recommender = StockRecommender(config_path)
    
    # 加载数据
    df = recommender.data_loader.load_data()
    
    # 执行数据检查
    if recommender.data_validator:
        results = recommender.data_validator.validate_all(df)
        assert results['passed'] or 'error' in results


def test_market_cap_filter():
    """测试市值筛选"""
    config_path = 'config/xcn_comm.yaml'
    recommender = StockRecommender(config_path)
    
    # 加载数据
    df = recommender.data_loader.load_data()
    
    # 执行市值筛选
    if recommender.market_cap_filter:
        result = recommender.market_cap_filter.filter_by_cap(df)
        assert len(result) <= len(df)
```

**Step 2: Run integration test**

Run: `pytest tests/test_integration_tomorrow_picks.py -v`
Expected: PASS (all tests)

**Step 3: Commit**

```bash
git add tests/test_integration_tomorrow_picks.py
git commit -m "feat: add integration tests for tomorrow_picks"
```

---

### Task 5.2: 性能测试和优化

**Files:**
- Create: `scripts/performance_test_enhanced.py`

**Step 1: Write performance test**

```python
# scripts/performance_test_enhanced.py
"""性能测试"""
import time
import polars as pl
from scripts.tomorrow_picks import StockRecommender


def test_performance():
    """测试性能"""
    config_path = 'config/xcn_comm.yaml'
    recommender = StockRecommender(config_path)
    
    start_time = time.time()
    
    # 运行推荐流程
    recommender.run()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"总耗时: {elapsed_time:.2f} 秒")
    
    # 验证性能要求
    assert elapsed_time < 10.0, f"性能不达标: {elapsed_time:.2f} 秒 > 10 秒"


if __name__ == '__main__':
    test_performance()
```

**Step 2: Run performance test**

Run: `python scripts/performance_test_enhanced.py`
Expected: Total time < 10 seconds

**Step 3: Commit**

```bash
git add scripts/performance_test_enhanced.py
git commit -m "feat: add performance test for enhanced tomorrow_picks"
```

---

### Task 5.3: 更新文档

**Files:**
- Modify: `docs/stock_recommender_README.md`

**Step 1: Update documentation**

```markdown
# 股票推荐系统使用指南

## 功能特性

### 1. 数据检查
- 完整性检查：确保必需字段存在，记录数足够
- 有效性检查：验证价格和涨跌幅在合理范围内
- 新鲜度检查：确保数据未过期
- 一致性检查：验证评分与等级的一致性

### 2. 市值筛选
- 自动计算市值（股价 × 流通股本）
- 筛选市值在 30亿-500亿 之间的股票
- 支持自定义市值范围

### 3. 龙头股票识别
- 基于涨幅和走势强度识别龙头
- 综合动量评分（3日、10日、20日加权）
- 支持自定义龙头数量和动量阈值

### 4. 量价分析
- 识别量价形态（量价齐升、缩量上涨、放量下跌等）
- 检测量价背离
- 支持自定义量价形态

### 5. 关键位计算
- 计算支撑位和压力位
- 计算 Pivot 点
- 计算斐波那契回调位
- 计算布林带

## 配置说明

所有参数均在 `config/xcn_comm.yaml` 中配置，包括：
- 数据检查参数
- 市值筛选参数
- 龙头识别参数
- 量价分析参数
- 关键位计算参数

## 使用方法

```bash
python scripts/tomorrow_picks.py
```

## 输出示例

```
【S级 - 强烈推荐】
  600721 百花医药  9.37元 +1.63% 评分92
    理由: 偏多趋势,强势上涨(3日+1.3%),量价齐升
    支撑位: 9.20
    压力位: 9.55
```
```

**Step 2: Commit**

```bash
git add docs/stock_recommender_README.md
git commit -m "docs: update stock recommender README with new features"
```

---

## 执行选项

**Plan complete and saved to `docs/plans/2026-03-24-stock-recommender-enhancement-plan.md`. Two execution options:**

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
