# 真实数据获取指南

## 当前数据状况

### 已获取的真实数据

| 数据类型 | 覆盖度 | 来源 | 状态 |
|---------|--------|------|------|
| 行业分类 | 100% (5381只) | Baostock | ✅ 已获取 |
| K线数据 | 100% (日线) | 本地数据 | ✅ 已有 |
| 财务数据 | 5.6% (300只) | Baostock | ⚠️ 部分 |
| 估值数据(PE/PB) | 0% | - | ❌ 待获取 |

## 推荐数据源

### 1. Tushare (推荐)

**优点**: 数据全面、质量高
**缺点**: 需要积分，部分数据收费

```python
import tushare as ts
pro = ts.pro_api('your_token')

# 获取估值数据
df = pro.daily_basic(ts_code='000001.SZ', fields='ts_code,trade_date,pe,pb,ps,total_mv')

# 获取财务数据
df = pro.fina_indicator(ts_code='000001.SZ', fields='ts_code,roe,profit_yoy,or_yoy')
```

**注册**: https://tushare.pro/register
**积分获取**: 注册送积分，邀请好友、贡献代码可获更多

### 2. AKShare (免费)

**优点**: 免费、数据丰富
**缺点**: 需要处理反爬、稳定性一般

```python
import akshare as ak

# 获取A股估值
df = ak.stock_zh_a_spot_em()  # 东方财富实时数据

# 获取财务指标
df = ak.stock_financial_analysis_indicator(symbol="600004")
```

**安装**: `pip install akshare`

### 3. Baostock (免费)

**优点**: 免费、历史数据完整
**缺点**: 实时数据有限、接口有频率限制

```python
import baostock as bs
bs.login()

# 获取估值数据
rs = bs.query_history_k_data_plus("sh.600000",
    "date,code,peTTM,pbMRQ",
    start_date='2024-01-01',
    end_date='2024-12-31')
```

**官网**: http://baostock.com

### 4. 金融终端 (专业级)

| 终端 | 优点 | 缺点 | 价格 |
|-----|------|------|------|
| Wind | 数据最全 | 昂贵 | 数万/年 |
| Choice | 性价比高 | 需安装 | 数千/年 |
| iFinD | 功能丰富 | 较重 | 数千/年 |

## 当前模型的代理因子

由于真实财务数据获取受限，当前模型使用以下代理因子：

### 估值代理
- `factor_pe_proxy`: 价格/收益稳定性
- `factor_pb_proxy`: 市净率代理 (价格/均线)
- `factor_ps_proxy`: 市销率代理 (价格/成交量)

### 盈利能力代理
- `factor_roe_proxy`: 收益率稳定性
- `factor_profitability`: 收益/价格
- `factor_sharpe_proxy`: 风险调整后收益

### 成长性代理
- `factor_revenue_growth_proxy`: 短期vs长期收益趋势
- `factor_momentum_health`: 价格动量

## 建议的改进方案

### 短期 (使用免费数据源)

1. **使用AKShare获取实时估值**
   ```python
   import akshare as ak
   df = ak.stock_zh_a_spot_em()  # 包含PE/PB/市值
   ```

2. **使用Baostock获取历史财务**
   ```python
   # 批量获取ROE、营收增长
   bs.query_profit_data()
   bs.query_growth_data()
   ```

### 长期 (建议购买数据)

1. **Tushare积分版** (约￥500/年)
   - 每日更新估值数据
   - 季度财务数据
   - 行业数据

2. **Wind/Choice** (专业级)
   - 完整的财务数据
   - 宏观经济数据
   - 行业景气度指数

## 下一步行动

1. **注册Tushare**并获取token
2. **修改数据获取脚本**使用Tushare API
3. **更新模型训练流程**使用真实财务因子
4. **重新训练模型**并验证效果

## 联系方式

如需协助配置数据源，请提供：
- Tushare token (如有)
- 其他数据源账号
