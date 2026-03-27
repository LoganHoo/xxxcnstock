"""获取并保存大盘指数历史数据"""
import akshare as ak
import polars as pl
from pathlib import Path
from datetime import datetime

print('=== 获取大盘指数历史数据 ===')
print()

indices = [
    ('sh000001', '上证指数'),
    ('sz399001', '深证成指'),
    ('sz399006', '创业板指'),
    ('sh000300', '沪深300'),
    ('sh000016', '上证50'),
    ('sh000905', '中证500'),
    ('sh000852', '中证1000'),
    ('sz399005', '中小板指'),
]

save_dir = Path('data/index')
save_dir.mkdir(parents=True, exist_ok=True)

for code, name in indices:
    try:
        print(f'获取 {name} ({code})...')
        
        df = ak.stock_zh_index_daily(symbol=code)
        
        df = df.rename(columns={
            'date': 'trade_date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        df['code'] = code
        df['name'] = name
        
        pl_df = pl.from_pandas(df)
        
        save_code = code.replace('sh', '').replace('sz', '')
        save_path = save_dir / f'{save_code}.parquet'
        pl_df.write_parquet(save_path)
        
        rows = len(df)
        start_date = df['trade_date'].iloc[0]
        end_date = df['trade_date'].iloc[-1]
        
        print(f'  保存: {save_path}')
        print(f'  数据: {rows} 条, {start_date} ~ {end_date}')
        print()
        
    except Exception as e:
        print(f'  失败: {e}')
        print()

print('=== 完成 ===')
