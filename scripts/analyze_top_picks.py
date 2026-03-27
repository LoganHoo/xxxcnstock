#!/usr/bin/env python3
"""明日精选股票深度分析"""
import polars as pl
from pathlib import Path

data_path = Path("data/enhanced_full_temp.parquet")
key_levels_path = Path("data/key_levels_latest.parquet")
cvd_path = Path("data/cvd_latest.parquet")

df = pl.read_parquet(data_path)
key_levels = pl.read_parquet(key_levels_path)
cvd = pl.read_parquet(cvd_path)

merged = df.join(key_levels.drop(['trade_date', 'price'], strict=False), on='code', how='left')
merged = merged.join(cvd.drop(['trade_date', 'price', 'volume'], strict=False), on='code', how='left')

print("="*80)
print("明日精选股票深度分析")
print("="*80)

high_potential = merged.filter(
    (pl.col('grade') == 'S') &
    (pl.col('enhanced_score') >= 120) &
    (pl.col('change_pct') > 3) &
    (pl.col('change_pct') < 15)
)

high_potential = high_potential.with_columns([
    ((pl.col('resistance_strong') - pl.col('price')) / pl.col('price') * 100)
    .alias('upside_pct'),
    ((pl.col('ma20') - pl.col('ma60')) / pl.col('ma60') * 100)
    .alias('ma_strength')
])

high_potential = high_potential.sort('enhanced_score', descending=True).head(20)

print("\n高潜力股票筛选条件:")
print("  - S级评级")
print("  - 综合评分 >= 120")
print("  - 今日涨幅 3%-15% (避免追高)")
print("  - 多头排列 + CVD买方占优")

print("\n" + "-"*80)
header = f"{'代码':<8} {'名称':<8} {'价格':>8} {'涨幅%':>7} {'评分':>5} {'上涨空间':>8} {'均线强度':>8}"
print(header)
print("-"*80)

for row in high_potential.iter_rows(named=True):
    name = (row.get('name') or '')[:6]
    cvd_signal = row.get('cvd_signal', 'neutral')
    upside = row.get('upside_pct', 0) or 0
    ma_strength = row.get('ma_strength', 0) or 0
    
    line = f"{row['code']:<8} {name:<8} {row['price']:>8.2f} {row['change_pct']:>6.2f}% {row['enhanced_score']:>5.0f} {upside:>7.1f}% {ma_strength:>7.1f}%"
    print(line)

print("\n" + "="*80)
print("明日最值得关注的5只股票")
print("="*80)

top5 = high_potential.head(5)
for i, row in enumerate(top5.iter_rows(named=True), 1):
    name = row.get('name') or ''
    print(f"\n{i}. {row['code']} {name}")
    print(f"   当前价格: {row['price']:.2f}元")
    print(f"   今日涨幅: +{row['change_pct']:.2f}%")
    print(f"   综合评分: {row['enhanced_score']:.0f}")
    
    support = row.get('support_strong', 0) or 0
    resistance = row.get('resistance_strong', 0) or 0
    ma20 = row.get('ma20', 0) or 0
    ma60 = row.get('ma60', 0) or 0
    
    if support:
        dist = (row['price'] - support) / support * 100
        print(f"   支撑位: {support:.2f} (距当前 {dist:.1f}%)")
    if resistance:
        upside = (resistance - row['price']) / row['price'] * 100
        print(f"   压力位: {resistance:.2f} (上涨空间 {upside:.1f}%)")
    if ma20 and ma60:
        print(f"   均线: MA20={ma20:.2f} > MA60={ma60:.2f} (多头排列)")
    
    cvd_signal = row.get('cvd_signal', 'neutral')
    cvd_trend = row.get('cvd_trend', 'neutral')
    divergence = row.get('divergence_5d', 'no_divergence')
    
    print(f"   CVD: {cvd_signal} | {cvd_trend}")
    if divergence != 'no_divergence':
        print(f"   背离信号: {divergence}")
    
    reasons = row.get('reasons', '') or ''
    print(f"   理由: {reasons[:50]}...")

print("\n" + "="*80)
print("风险提示")
print("="*80)
print("  1. 以上分析基于技术指标，仅供参考")
print("  2. 请结合基本面、市场情绪综合判断")
print("  3. 建议设置止损位，控制风险")
print("  4. 股市有风险，投资需谨慎")
