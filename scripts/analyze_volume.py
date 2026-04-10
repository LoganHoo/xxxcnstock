import polars as pl
from pathlib import Path
kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))

vol_buckets = {'0':0, '1-100':0, '100-1000':0, '1000-10000':0, '10000+':0}
rising, falling, unchanged = 0, 0, 0

for f in files:
    try:
        df = pl.read_parquet(str(f))
        if len(df) >= 2:
            df = df.sort('trade_date', descending=True)
            latest = df.row(0)
            prev = df.row(1)
            if str(latest[1]) == '2026-04-03':
                vol = int(latest[6])
                close_t = float(latest[3])
                close_y = float(prev[3])

                if vol == 0:
                    vol_buckets['0'] += 1
                elif vol < 100:
                    vol_buckets['1-100'] += 1
                elif vol < 1000:
                    vol_buckets['100-1000'] += 1
                elif vol < 10000:
                    vol_buckets['1000-10000'] += 1
                else:
                    vol_buckets['10000+'] += 1

                if close_t > close_y:
                    rising += 1
                elif close_t < close_y:
                    falling += 1
                else:
                    unchanged += 1
    except:
        continue

print('成交量分布:')
for k, v in vol_buckets.items():
    print(f'  {k}: {v}')
total = rising + falling + unchanged
print(f'\n总有效: {total}')
print(f'上涨:{rising} 下跌:{falling} 平盘:{unchanged}')