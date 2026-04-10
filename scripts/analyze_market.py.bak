import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))

rising, falling, unchanged, limit_up, limit_down = 0, 0, 0, 0, 0

for f in files:
    try:
        df = pl.read_parquet(str(f))
        if len(df) >= 2:
            df = df.sort('trade_date', descending=True)
            latest = df[0]
            prev = df[1]
            close_t = latest['close']
            close_y = prev['close']

            if close_t > close_y:
                rising += 1
                if close_t >= close_y * 1.099 and close_t == latest['high']:
                    limit_up += 1
            elif close_t < close_y:
                falling += 1
                if close_t <= close_y * 0.901 and close_t == latest['low']:
                    limit_down += 1
            else:
                unchanged += 1
    except:
        continue

total = rising + falling + unchanged
print(f'=== 2026-04-03 市场概况 ===')
print(f'总股票: {total}')
print(f'上涨: {rising} ({rising*100//total}%)')
print(f'下跌: {falling} ({falling*100//total}%)')
print(f'平盘: {unchanged}')
print(f'涨停: {limit_up}')
print(f'跌停: {limit_down}')