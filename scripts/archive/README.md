# 脚本归档目录

## 目录结构

```
archive/
├── deprecated/          # 废弃脚本（不再使用）
├── old_versions/        # 旧版本脚本（被新版本替代）
└── alternatives/        # 替代方案脚本（功能重复）
```

---

## deprecated/ 废弃脚本

这些脚本由于技术问题或不再适用而被废弃：

| 脚本 | 废弃原因 |
|------|----------|
| `fetch_baostock_async.py` | 线程不安全，Baostock API不支持并发 |
| `data_processing_demo.py` | 演示脚本，无实际用途 |

---

## old_versions/ 旧版本

这些脚本的旧版本，已被新版本替代：

| 脚本 | 替代版本 | 说明 |
|------|----------|------|
| `fetch_baostock_fast.py` | `fetch_baostock_fast_v2.py` | v2版本修复了多进程问题 |
| `fetch_baostock_complete.py` | `fetch_baostock_complete_v2.py` | v2版本优化了性能 |
| `fetch_fundamental_data.py` | `fetch_fundamental_baostock.py` | 新版本功能更完善 |
| `fetch_all_stocks.py` | `fetch_stock_list.py` | 功能重复，保留一个 |

---

## alternatives/ 替代方案

这些脚本提供相同功能的替代实现：

| 脚本 | 主版本 | 说明 |
|------|--------|------|
| `fetch_fundamental_akshare.py` | `fetch_fundamental_baostock.py` | AKShare版本，备用 |
| `fetch_all_enhanced.py` | `fetch_baostock_fast_v2.py` | 腾讯API版本，备用 |
| `fetch_tushare_data.py` | `fetch_baostock_fast_v2.py` | Tushare版本，备用 |
| `fetch_klines_and_analyze.py` | - | 采集+分析组合脚本 |

---

## 当前推荐使用的脚本

位于 `scripts/` 根目录的核心脚本：

```
scripts/
├── fetch_stock_list.py              # 股票列表采集 ⭐
├── fetch_baostock_fast_v2.py        # K线数据采集 ⭐
├── fetch_fundamental_baostock.py    # 基本面数据采集 ⭐
├── fetch_missing_fundamental.py     # 补充采集
├── fetch_share_structure.py         # 股本结构
├── fetch_index_data.py              # 指数数据
├── fetch_realtime_valuation.py      # 实时估值
├── fetch_news_macro.py              # 新闻宏观
├── data_collection_master.py        # 统一入口 ⭐
├── data_integrity_check.py          # 完整性检查
└── data_validator.py                # 数据验证
```

---

## 使用建议

1. **日常采集**: 使用 `data_collection_master.py` 统一入口
2. **单独任务**: 直接使用根目录下的核心脚本
3. **归档脚本**: 仅在特殊情况下参考，不建议使用

---

## 归档日期

2026-04-17
