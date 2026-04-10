import polars as pl
from pathlib import Path
kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))

print('成交量层级 | 股票数 | 上涨 | 下跌 | 平盘')
print('-' * 50)

for min_vol in [0, 100, 1000, 10000, 50000]:
    rising, falling, unchanged = 0, 0, 0
    for f in files:
        try:
            df = pl.read_parquet(str(f))
            if len(df) >= 2:
                df = df.sort('trade_date', descending=True)
                latest = df.row(0)
                prev = df.row(1)
                if str(latest[1]) == '2026-04-03' and int(latest[6]) >= min_vol:
                    close_t = float(latest[3])
                    close_y = float(prev[3])
                    if close_t > close_y:
                        rising += 1
                    elif close_t < close_y:
                        falling += 1
                    else:
                        unchanged += 1
        except:
            continue
    total = rising + falling + unchanged
    if min_vol == 0:
        label = 'all'
    else:
        label = f'>={min_vol}'
    print(f'{label:>10} | {total:>5} | {rising:>4} | {falling:>4} | {unchanged:>4}')