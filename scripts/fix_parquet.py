import requests
import polars as pl
import sys

code = sys.argv[1] if len(sys.argv) > 1 else '300688'

url = f'http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.{code}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=1&beg=20200101&end=20260402'

try:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data['data'] and data['data']['klines']:
        rows = [k.split(',') for k in data['data']['klines']]
        df = pl.DataFrame({
            'code': [code] * len(rows),
            'trade_date': [r[0] for r in rows],
            'open': [float(r[1]) for r in rows],
            'close': [float(r[2]) for r in rows],
            'high': [float(r[3]) for r in rows],
            'low': [float(r[4]) for r in rows],
            'volume': [float(r[5]) for r in rows],
        })
        df.write_parquet(f'/app/data/kline/{code}.parquet')
        print(f'OK {code} {len(df)} {df["trade_date"][-1]}')
    else:
        print(f'EMPTY {code}')
except Exception as e:
    print(f'ERROR {code}: {e}')