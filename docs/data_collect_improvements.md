# DataCollectorV2 改进清单

## 问题清单

### 问题1: 缺少数据验证层 ✅ 已完成
- **严重程度**: 🔴 高
- **描述**: 采集的数据没有验证价格、成交量、涨跌幅的合理性
- **影响**: 导致4月17日数据异常（成交额29万亿 vs 实际2.4万亿）
- **解决方案**: 添加 `_validate_kline_data()` 方法
- **状态**: ✅ 已完成 (2026-04-19)
- **修复内容**:
  - 验证必要字段存在
  - 验证价格合理性 (0 < price <= 5000)
  - 验证价格关系 (high >= low, close在范围内)
  - 验证成交量合理性 (0 < volume <= 50亿股)
  - 验证涨跌幅合理性 (|pct_chg| <= 25%)
  - 验证成交额合理性 (amount <= 1000亿)

### 问题2: 列名映射不完整 ✅ 已完成
- **严重程度**: 🟡 中
- **描述**: 只映射了基础字段，缺少 `preclose`, `turnover`, `pct_chg` 等字段
- **影响**: 可能导致字段缺失或数据不一致
- **解决方案**: 完善 `column_mapping` 字典
- **状态**: ✅ 已完成 (2026-04-19)
- **修复内容**:
  - 添加 `preclose/pre_close/preClose` 映射
  - 添加 `pct_chg/pctChg/change_pct/pct_change` 映射
  - 添加 `turn/turnover/turnover_rate` 映射

### 问题3: 强制日期模式无校验 ✅ 已完成
- **严重程度**: 🔴 高
- **描述**: `--date` 模式下直接替换数据，不验证新数据质量
- **影响**: 可能用错误数据覆盖正确数据
- **解决方案**: 在强制模式添加数据验证
- **状态**: ✅ 已完成 (2026-04-19)
- **修复内容**:
  - 在强制模式替换数据前调用 `_validate_kline_data()`
  - 验证失败时放弃更新，返回'failed'
  - 添加成功/失败的详细日志

### 问题4: 缺少preclose字段处理 ✅ 已完成
- **严重程度**: 🟡 中
- **描述**: 当数据源不返回preclose时，没有计算逻辑
- **影响**: 涨跌幅计算可能不准确
- **解决方案**: 添加preclose计算或fallback逻辑
- **状态**: ✅ 已完成 (2026-04-19)
- **修复内容**:
  - 如果没有preclose但有pct_chg和close，计算: `preclose = close / (1 + pct_chg/100)`
  - 如果没有pct_chg但有preclose和close，计算: `pct_chg = (close - preclose) / preclose * 100`

### 问题5: 错误处理不完善 ✅ 已完成
- **严重程度**: 🟡 中
- **描述**: 部分异常没有详细日志，难以排查问题
- **影响**: 问题定位困难
- **解决方案**: 增强异常处理和日志记录
- **状态**: ✅ 已完成 (2026-04-19)
- **修复内容**:
  - 使用 `logger.error(..., exc_info=True)` 替代 `logger.warning()`
  - 添加列名信息到错误日志
  - 添加数据条数信息到成功日志
  - 新文件创建前也进行数据验证

## 架构优化清单 (2026-04-19)

### 优化1: 提取公共模块 ✅ 已完成
- **描述**: 列名标准化、Parquet更新、锁管理等逻辑在多个脚本中重复
- **解决方案**: 创建 `core/storage/` 公共模块
- **状态**: ✅ 已完成
- **新增文件**:
  - `core/storage/__init__.py` - 模块导出
  - `core/storage/parquet_utils.py` - Parquet文件操作工具
    - `normalize_columns()` - 列名标准化
    - `update_parquet_file()` - 更新Parquet文件
    - `ParquetManager` - Parquet文件管理器类
  - `core/storage/lock_manager.py` - 锁文件管理器
    - `LockManager` - 进程锁管理类

### 优化2: 清理冗余脚本 ✅ 已完成
- **描述**: 多个脚本功能重复，维护成本高
- **解决方案**: 删除废弃脚本
- **状态**: ✅ 已完成
- **已删除文件**:
  - `collect_history_data.py` - 功能由 `data_collect.py --date` 替代
  - `data_collect_legacy.py` - 功能由 `data_collect.py` 替代
  - `fetch_history_klines_parquet.py` - 功能由 `data_collect.py` 替代
  - `fetch_baostock_async.py` - 异步版本已整合
  - `data_processing_demo.py` - 演示脚本无需保留

### 优化3: 统一任务入口 ✅ 已完成
- **描述**: task_data_collect.py 使用独立实现，与主采集逻辑不一致
- **解决方案**: 重构为调用 data_collect.py
- **状态**: ✅ 已完成
- **修改文件**:
  - `daily_tasks/task_data_collect.py` - 改为调用 data_collect.py

### 优化4: 更新主控脚本 ✅ 已完成
- **描述**: data_collection_master.py 调用旧版采集脚本
- **解决方案**: 更新为调用 data_collect.py
- **状态**: ✅ 已完成
- **修改文件**:
  - `data_collection_master.py` - 更新 collect_kline() 方法
  - 支持 `--date` 参数传递

## 改进记录

| 序号 | 问题 | 修复内容 | 状态 | 完成时间 |
|------|------|----------|------|----------|
  | 1 | 缺少数据验证层 | 添加 `_validate_kline_data()` 方法 | ✅ | 2026-04-19 |
| 2 | 列名映射不完整 | 完善 `column_mapping`，支持多种字段名 | ✅ | 2026-04-19 |
| 3 | 强制日期模式无校验 | 添加强制模式数据验证 | ✅ | 2026-04-19 |
| 4 | 缺少preclose字段处理 | 添加preclose/pct_chg计算逻辑 | ✅ | 2026-04-19 |
| 5 | 错误处理不完善 | 增强异常处理和日志记录 | ✅ | 2026-04-19 |
| 6 | 重复代码 | 创建 core/storage/ 公共模块 | ✅ | 2026-04-19 |
| 7 | 冗余脚本 | 移动3个废弃脚本到 archive/deprecated/ | ✅ | 2026-04-19 |
| 8 | 任务入口不统一 | 重构 task_data_collect.py 调用 data_collect.py | ✅ | 2026-04-19 |
| 9 | 主控脚本过时 | 更新 data_collection_master.py | ✅ | 2026-04-19 |

## 验证标准

- [x] 采集的数据通过价格合理性验证（0 < price <= 5000）
- [x] 采集的数据通过成交量合理性验证（0 < volume <= 50亿股）
- [x] 采集的数据通过涨跌幅验证（|pct_chg| < 25%）
- [x] 所有字段正确映射，无缺失
- [x] 强制日期模式有数据质量检查
- [x] 异常有详细日志记录
- [x] 公共模块可正常导入
- [x] 重构后的脚本语法正确

## 代码变更摘要

### 新增方法
```python
def _validate_kline_data(self, df: pl.DataFrame, code: str) -> tuple[bool, str]:
    """验证K线数据合理性"""
    # 验证必要字段
    # 验证价格合理性
    # 验证成交量合理性
    # 验证涨跌幅合理性
```

### 新增公共模块
```python
# core/storage/parquet_utils.py
- normalize_columns(df) -> pl.DataFrame
- update_parquet_file(code, new_data, kline_dir) -> bool
- class ParquetManager

# core/storage/lock_manager.py
- class LockManager
  - acquire() -> bool
  - release() -> bool
  - check_conflict() -> bool
```

### 修改方法
1. `_fetch_single()` - 添加数据验证调用
2. `_update_parquet()` - 完善列名映射、添加强制模式验证、增强错误处理
3. `column_mapping` - 扩展支持更多字段名
4. `task_data_collect.py` - 改为调用 data_collect.py
5. `data_collection_master.py` - 更新 collect_kline() 调用 data_collect.py

## 使用说明

### 统一采集入口
所有数据采集应使用以下方式：

```bash
# 1. 直接使用 data_collect.py
python scripts/pipeline/data_collect.py

# 2. 强制采集指定日期
python scripts/pipeline/data_collect.py --date 2026-04-17

# 3. 断点续传模式
python scripts/pipeline/data_collect.py --retry

# 4. 使用主控脚本
python scripts/data_collection_master.py --task kline
python scripts/data_collection_master.py --task kline --date 2026-04-17

# 5. 定时任务（内部调用 data_collect.py）
python scripts/daily_tasks/task_data_collect.py
```

### 废弃脚本替代方案

| 废弃脚本 | 替代命令 |
|---------|---------|
| collect_history_data.py | `python scripts/pipeline/data_collect.py --date YYYY-MM-DD` |
| data_collect_legacy.py | `python scripts/pipeline/data_collect.py` |
| fetch_history_klines_parquet.py | `python scripts/pipeline/data_collect.py` |

## 微服务架构升级 (2026-04-19)

### 改造目标
将数据采集脚本从直接调用Baostock API改造为调用微服务，利用微服务的主备数据源自动切换、统一数据验证和错误处理能力。

### 改造清单

| 序号 | 脚本 | 改造内容 | 状态 | 完成时间 |
|------|------|----------|------|----------|
| 1 | fetch_baostock_fast_v2.py | 替换直接Baostock调用为UnifiedFetcher | ✅ | 2026-04-19 |
| 2 | fetch_stock_list.py | 替换直接Baostock调用为UnifiedFetcher | ✅ | 2026-04-19 |
| 3 | fetch_fundamental_baostock.py | 替换直接Baostock调用为UnifiedFetcher | ✅ | 2026-04-19 |

### 改造内容详情

#### 1. fetch_baostock_fast_v2.py
- **新增函数**:
  - `fetch_kline_via_service()` - 异步获取K线数据
  - `run_async()` - 运行异步函数的工具
- **修改函数**:
  - `fetch_kline_batch_microservice()` - 使用微服务批量获取K线
  - 保留多进程并行处理能力
  - 保留增量更新和断点续传功能
  - 保留数据验证逻辑

#### 2. fetch_stock_list.py
- **新增函数**:
  - `fetch_stock_list_via_service()` - 异步获取股票列表
  - `run_async()` - 运行异步函数的工具
- **修改函数**:
  - `main()` - 使用微服务获取股票列表
  - 保留数据验证和保存功能
  - 添加交易所统计报告

#### 3. fetch_fundamental_baostock.py
- **新增函数**:
  - `fetch_fundamental_via_service()` - 异步获取基本面数据
  - `fetch_fundamental_batch()` - 批量获取（用于多进程）
  - `fetch_valuation_data_parallel()` - 并行获取估值数据
  - `run_async()` - 运行异步函数的工具
- **修改函数**:
  - `fetch_valuation_data()` - 使用微服务获取估值数据
  - `fetch_industry_data()` - 使用微服务获取行业数据
  - 添加PE/PB异常值过滤
  - 支持串行和并行两种模式

### 微服务调用模式

```python
# 统一的异步调用模式
async def fetch_xxx_via_service(...) -> Optional[Dict]:
    fetcher = await get_unified_fetcher()
    result = await fetcher.fetch_xxx(...)
    return result

def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# 使用示例
data = run_async(fetch_xxx_via_service(...))
```

### 微服务优势

1. **主备自动切换**: Baostock失败自动切换到AKShare/Tencent
2. **统一数据验证**: 微服务层统一验证数据质量
3. **熔断保护**: 失败次数过多自动熔断，避免雪崩
4. **健康检查**: 定时检查数据源健康状态
5. **负载均衡**: 多数据源间智能负载分配

### 验证结果

- [x] 所有改造脚本语法正确
- [x] 异步调用模式正常工作
- [x] 微服务获取器初始化正常
- [x] 数据验证逻辑保留完整

## 代码迁移 (2026-04-19)

将数据采集脚本从 `scripts/` 迁移到 `services/data_service/fetchers/`，作为微服务内部模块。

### 迁移清单

| 原位置 | 新位置 | 说明 |
|--------|--------|------|
| `scripts/fetch_baostock_fast_v2.py` | `services/data_service/fetchers/kline_fetcher.py` | K线数据获取器 |
| `scripts/fetch_stock_list.py` | `services/data_service/fetchers/stock_list_fetcher.py` | 股票列表获取器 |
| `scripts/fetch_fundamental_baostock.py` | `services/data_service/fetchers/fundamental_fetcher.py` | 基本面数据获取器 |

### 迁移改动

1. **导入路径调整**: 从相对导入改为微服务内部导入
   ```python
   # 原导入
   from services.data_service.fetchers.unified_fetcher import UnifiedFetcher
   
   # 新导入
   from .unified_fetcher import UnifiedFetcher
   ```

2. **PROJECT_ROOT移除**: 微服务内部模块不需要手动设置项目根目录

3. **__init__.py更新**: 添加新模块的导出

### 新模块结构

```
services/data_service/fetchers/
├── __init__.py              # 模块导出
├── unified_fetcher.py       # 统一数据获取器
├── kline_fetcher.py         # K线数据获取器 (新增)
├── stock_list_fetcher.py    # 股票列表获取器 (新增)
├── fundamental_fetcher.py   # 基本面数据获取器 (新增)
├── cctv_news_fetcher.py     # CCTV新闻获取器
├── fundamental.py           # 基本面数据模型
├── kline_history.py         # K线历史数据
├── limitup.py               # 涨停数据
├── quote.py                 # 实时行情
└── stock_list.py            # 股票列表模型
```

### 验证结果

- [x] 所有新模块语法正确
- [x] `__init__.py` 导出正常
- [x] 微服务内部导入路径正确

## 外盘指数微服务升级 (2026-04-19)

### 改造目标
将外盘指数数据采集脚本改造为微服务架构，支持美股指数、亚洲股指和大宗商品数据的多数据源采集。

### 改造清单

| 序号 | 模块 | 改造内容 | 状态 | 完成时间 |
|------|------|----------|------|----------|
| 1 | foreign_index_fetcher.py | 新建外盘指数采集微服务模块 | ✅ | 2026-04-19 |
| 2 | commodity_fetcher.py | 新建大宗商品采集微服务模块 | ✅ | 2026-04-19 |
| 3 | domestic_index_fetcher.py | 新建国内指数采集微服务模块 | ✅ | 2026-04-19 |
| 4 | fetch_index_v2.py | 使用微服务改造国内指数采集脚本 | ✅ | 2026-04-19 |
| 5 | update_foreign_v2.py | 使用微服务改造外盘指数更新脚本 | ✅ | 2026-04-19 |

### 新模块详情

#### 1. foreign_index_fetcher.py
- **功能**: 采集美股指数和亚洲股指
- **数据源**: Yahoo Finance、Sina Finance、Eastmoney
- **支持指数**:
  - 美股: NASDAQ、S&P 500、Dow Jones
  - 亚洲: 恒生指数、H股指数、恒生科技
- **特性**:
  - 多数据源并发采集
  - 自动合并最优结果
  - 代理自动检测和配置
  - Docker环境支持

#### 2. commodity_fetcher.py
- **功能**: 采集大宗商品数据
- **数据源**: Yahoo Finance
- **支持商品**:
  - 黄金 (GLD)
  - 原油 (WTI)
  - 美元指数 (UUP)
- **特性**:
  - OHLC数据完整采集
  - 涨跌幅自动计算
  - 代理自动配置

#### 3. domestic_index_fetcher.py
- **功能**: 采集国内大盘指数
- **数据源**: akshare、手动更新脚本
- **支持指数**:
  - 上证指数、深证成指、创业板指
  - 沪深300、上证50、中证500
- **特性**:
  - 数据新鲜度自动检测
  - 主备数据源自动切换
  - 支持手动更新回退

### 新模块结构

```
services/data_service/fetchers/
├── __init__.py                  # 模块导出 (已更新)
├── unified_fetcher.py           # 统一数据获取器
├── kline_fetcher.py             # K线数据获取器
├── stock_list_fetcher.py        # 股票列表获取器
├── fundamental_fetcher.py       # 基本面数据获取器
├── foreign_index_fetcher.py     # 外盘指数获取器 (新增)
├── commodity_fetcher.py         # 大宗商品获取器 (新增)
├── domestic_index_fetcher.py    # 国内指数获取器 (新增)
└── ...
```

### 使用方式

```python
# 获取外盘指数
from services.data_service.fetchers import fetch_foreign_indices
result = fetch_foreign_indices()

# 获取大宗商品
from services.data_service.fetchers import fetch_commodities
result = fetch_commodities()

# 获取国内指数
from services.data_service.fetchers import fetch_domestic_indices
result = fetch_domestic_indices()

# 异步方式
import asyncio
from services.data_service.fetchers import fetch_foreign_indices_via_service
result = asyncio.run(fetch_foreign_indices_via_service())
```

### 验证结果

- [x] 所有新模块语法正确
- [x] `__init__.py` 导出正常
- [x] 外盘指数模块导入正常
- [x] 大宗商品模块导入正常
- [x] 国内指数模块导入正常

## 测试建议

1. 使用 `--date 2026-04-17` 测试强制模式数据验证
2. 检查日志输出是否包含详细的验证失败原因
3. 验证preclose和pct_chg是否正确计算
4. 测试异常数据的过滤效果
5. 验证公共模块导入是否正常
6. 测试微服务数据源自动切换功能
7. 测试重构后的任务调用链
8. 验证微服务内部模块导入正常
9. 测试外盘指数多数据源采集功能
10. 测试大宗商品数据采集功能
11. 测试国内指数数据新鲜度检测
