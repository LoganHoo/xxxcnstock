import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))

# 排除北交所(688)和科创板(688)
rising, falling, unchanged, limit_up, limit_down = 0, 0, 0, 0, 0
excluded = 0

for f in files:
    try:
        code = f.stem
        # 跳过北交所
        if code.startswith('688'):
            excluded += 1
            continue

        df = pl.read_parquet(str(f))
        if len(df) >= 2:
            df = df.sort('trade_date', descending=True)
            latest = df.row(0)
            prev = df.row(1)

            close_t = float(latest[3])
            close_y = float(prev[3])

            if close_t > close_y:
                rising += 1
                high_t = float(latest[4])
                if close_t >= close_y * 1.099 and abs(close_t - high_t) < 0.01:
                    limit_up += 1
            elif close_t < close_y:
                falling += 1
                low_t = float(latest[5])
                if close_t <= close_y * 0.901 and abs(close_t - low_t) < 0.01:
                    limit_down += 1
            else:
                unchanged += 1
    except:
        continue

total = rising + falling + unchanged
print('=== 2026-04-03 主板+创业板统计 ===')
print(f'排除北交所: {excluded}只')
print(f'有效统计: {total}只')
print(f'上涨: {rising} ({rising*100//total if total else 0}%)')
print(f'下跌: {falling} ({falling*100//total if total else 0}%)')
print(f'平盘: {unchanged}')
print(f'涨停: {limit_up}')
print(f'跌停: {limit_down}')