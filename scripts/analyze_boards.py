import polars as pl
from pathlib import Path
kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))

# 按板块分类统计
boards = {
    '上海主板': [],
    '深圳主板': [],
    '创业板': [],
    '北交所': [],
    'ST': []
}

for f in files:
    code = f.stem
    if code.startswith('688'):
        boards['北交所'].append(f)
    elif code.startswith('000') or code.startswith('001'):
        boards['上海主板'].append(f)
    elif code.startswith('002') or code.startswith('003'):
        boards['深圳主板'].append(f)
    elif code.startswith('300'):
        boards['创业板'].append(f)
    else:
        boards['ST'].append(f)

print('各板块统计:')
print('-' * 60)
print(f'{"板块":>8} | {"总数":>5} | {"上涨":>5} | {"下跌":>5} | {"平盘":>5} | {"有效":>5}')
print('-' * 60)

for board, board_files in boards.items():
    rising, falling, unchanged, valid = 0, 0, 0, 0
    for f in board_files:
        try:
            df = pl.read_parquet(str(f))
            if len(df) >= 2:
                df = df.sort('trade_date', descending=True)
                latest = df.row(0)
                prev = df.row(1)
                if str(latest[1]) == '2026-04-03':
                    valid += 1
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
    print(f'{board:>8} | {len(board_files):>5} | {rising:>5} | {falling:>5} | {unchanged:>5} | {valid:>5}')