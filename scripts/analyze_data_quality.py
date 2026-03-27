"""分析数据质量问题"""
import polars as pl

# 加载数据
df = pl.read_parquet('data/enhanced_full_temp.parquet')

print('=== 问题1: 价格异常 ===')
# 检查价格范围
price_range = [0.1, 1000]
invalid_price = df.filter(
    (pl.col('price') < price_range[0]) | (pl.col('price') > price_range[1])
)
print(f'价格超出范围 [0.1, 1000] 的股票数量: {len(invalid_price)}')
if len(invalid_price) > 0:
    print(invalid_price.select(['code', 'name', 'price', 'grade', 'enhanced_score']))

print('\n=== 问题2: 涨跌幅异常 ===')
# 检查涨跌幅范围
change_range = [-20, 20]
invalid_change = df.filter(
    (pl.col('change_pct') < change_range[0]) | (pl.col('change_pct') > change_range[1])
)
print(f'涨跌幅超出范围 [-20, 20] 的股票数量: {len(invalid_change)}')
if len(invalid_change) > 0:
    print(invalid_change.select(['code', 'name', 'price', 'change_pct', 'grade']))

print('\n=== 问题3: 评分与等级不一致 ===')
# 添加期望等级列
df_with_expected = df.with_columns([
    pl.when(pl.col('enhanced_score') >= 80).then(pl.lit('S'))
    .when(pl.col('enhanced_score') >= 75).then(pl.lit('A'))
    .when(pl.col('enhanced_score') >= 70).then(pl.lit('B'))
    .otherwise(pl.lit('C')).alias('expected_grade')
])

# 找出不一致的记录
inconsistent = df_with_expected.filter(pl.col('grade') != pl.col('expected_grade'))
print(f'评分与等级不一致的股票数量: {len(inconsistent)}')
print('\n前10条不一致记录:')
print(inconsistent.select(['code', 'name', 'price', 'grade', 'expected_grade', 'enhanced_score']).head(10))

print('\n=== 等级分布 ===')
grade_dist = df.group_by('grade').agg(pl.len().alias('count')).sort('grade')
print(grade_dist)

print('\n=== 评分分布 ===')
score_stats = df.select([
    pl.col('enhanced_score').min().alias('min'),
    pl.col('enhanced_score').max().alias('max'),
    pl.col('enhanced_score').mean().alias('mean'),
    pl.col('enhanced_score').median().alias('median')
])
print(score_stats)
