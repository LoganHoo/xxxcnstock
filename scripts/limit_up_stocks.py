"""打板股票分析 - 涨停板股票"""
import pandas as pd

df = pd.read_parquet('data/enhanced_scores_full.parquet')

print("=" * 70)
print("打板股票分析 (涨停/接近涨停)")
print("=" * 70)

# 1. 涨停股 (涨幅>=9.5%)
limit_up = df[df['change_pct'] >= 9.5].sort_values('change_pct', ascending=False)
print(f"\n【涨停板】(涨幅>=9.5%, 共{len(limit_up)}只)")
print("-" * 70)
for _, r in limit_up.iterrows():
    print(f"  {r['code']} {r['name']:8} {r['price']:7.2f}元 +{r['change_pct']:.2f}% 评分{r['enhanced_score']:.0f} {r['grade']}级")
    if pd.notna(r['reasons']):
        print(f"    理由: {r['reasons'][:60]}")

# 2. 接近涨停 (涨幅7-9.5%)
near_limit = df[(df['change_pct'] >= 7) & (df['change_pct'] < 9.5)].sort_values('change_pct', ascending=False)
print(f"\n【接近涨停】(涨幅7%-9.5%, 共{len(near_limit)}只)")
print("-" * 70)
for _, r in near_limit.head(15).iterrows():
    print(f"  {r['code']} {r['name']:8} {r['price']:7.2f}元 +{r['change_pct']:.2f}% 评分{r['enhanced_score']:.0f} {r['grade']}级")

# 3. 涨停股中评分高的
high_score_limit = limit_up[limit_up['enhanced_score'] >= 70]
print(f"\n【涨停股-高评分】(评分>=70, 共{len(high_score_limit)}只)")
print("-" * 70)
for _, r in high_score_limit.iterrows():
    print(f"  {r['code']} {r['name']:8} {r['price']:7.2f}元 +{r['change_pct']:.2f}% 评分{r['enhanced_score']:.0f}")
    if pd.notna(r['reasons']):
        print(f"    理由: {r['reasons']}")

# 4. 连板潜力 (涨停+多头排列)
bullish_limit = limit_up[limit_up['trend'] == 100]
print(f"\n【涨停+多头排列】(连板潜力, 共{len(bullish_limit)}只)")
print("-" * 70)
for _, r in bullish_limit.iterrows():
    print(f"  {r['code']} {r['name']:8} {r['price']:7.2f}元 +{r['change_pct']:.2f}%")

# 5. 涨停股板块分析
print(f"\n【涨停股统计】")
print("-" * 70)
print(f"  涨停总数: {len(limit_up)}只")
print(f"  S级涨停: {len(limit_up[limit_up['grade']=='S'])}只")
print(f"  A级涨停: {len(limit_up[limit_up['grade']=='A'])}只")
print(f"  B级涨停: {len(limit_up[limit_up['grade']=='B'])}只")
print(f"  C级涨停: {len(limit_up[limit_up['grade']=='C'])}只")
print(f"  多头排列: {len(bullish_limit)}只")
print(f"  平均评分: {limit_up['enhanced_score'].mean():.1f}")

print("\n" + "=" * 70)
print("风险提示: 打板风险极高，追高需谨慎!")
print("=" * 70)
