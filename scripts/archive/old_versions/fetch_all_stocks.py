"""采集所有A股股票行情数据"""
import requests
import pandas as pd
import os
from datetime import datetime

def fetch_all_stocks():
    """使用新浪财经API获取A股行情"""
    print('=== 采集A股全部行情数据 ===')
    
    # 读取股票代码列表
    stock_list = pd.read_parquet('data/stock_list_20260316.parquet')
    all_codes = stock_list['code'].tolist()
    
    # 转换为新浪格式
    def to_sina_code(code):
        if code.startswith('6'):
            return f'sh{code}'
        else:
            return f'sz{code}'
    
    sina_codes = [to_sina_code(c) for c in all_codes]
    print(f'股票总数: {len(sina_codes)}')
    
    # 分批获取
    batch_size = 100
    all_data = []
    
    for i in range(0, len(sina_codes), batch_size):
        batch = sina_codes[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(sina_codes) + batch_size - 1) // batch_size
        
        try:
            url = 'https://hq.sinajs.cn/list=' + ','.join(batch)
            headers = {'Referer': 'https://finance.sina.com.cn/'}
            resp = requests.get(url, headers=headers, timeout=30)
            text = resp.text
            lines = text.strip().split('\n')
            
            for line in lines:
                if 'var hq_str_' in line:
                    # 解析 var hq_str_sh600000="...";
                    eq_pos = line.find('="')
                    if eq_pos > 0:
                        code_part = line[11:eq_pos]  # 去掉 "var hq_str_"
                        data_part = line[eq_pos+2:-3]  # 去掉 '="...' 和 '";'
                        
                        if data_part:
                            fields = data_part.split(',')
                            if len(fields) >= 32:
                                try:
                                    all_data.append({
                                        'code': code_part[2:],  # 去掉 sh/sz
                                        'name': fields[0],
                                        'open': float(fields[1]) if fields[1] else 0,
                                        'pre_close': float(fields[2]) if fields[2] else 0,
                                        'price': float(fields[3]) if fields[3] else 0,
                                        'high': float(fields[4]) if fields[4] else 0,
                                        'low': float(fields[5]) if fields[5] else 0,
                                        'volume': int(float(fields[8])) if fields[8] else 0,
                                        'amount': float(fields[9]) if fields[9] else 0,
                                    })
                                except (ValueError, IndexError):
                                    pass
            
            print(f'批次 {batch_num}/{total_batches}: 已获取 {len(all_data)} 只', end='\r')
            
        except Exception as e:
            print(f'\n批次 {batch_num} 失败: {e}')
    
    print(f'\n获取完成: {len(all_data)} 只')
    
    if all_data:
        df = pd.DataFrame(all_data)
        
        # 计算涨跌幅
        df['change_pct'] = df.apply(
            lambda x: round((x['price'] - x['pre_close']) / x['pre_close'] * 100, 2) 
            if x['pre_close'] > 0 else 0, axis=1
        )
        
        # 保存
        date_str = datetime.now().strftime('%Y%m%d')
        filepath = f'data/realtime/{date_str}.parquet'
        os.makedirs('data/realtime', exist_ok=True)
        df.to_parquet(filepath, index=False)
        
        print(f'\n保存至: {filepath}')
        print(f'文件大小: {os.path.getsize(filepath)/1024:.1f} KB')
        
        # 统计
        up = len(df[df['change_pct'] > 0])
        down = len(df[df['change_pct'] < 0])
        flat = len(df[df['change_pct'] == 0])
        
        print(f'\n=== 市场统计 ===')
        print(f'上涨: {up} 只 ({up/len(df)*100:.1f}%)')
        print(f'下跌: {down} 只 ({down/len(df)*100:.1f}%)')
        print(f'平盘/停牌: {flat} 只')
        
        # 涨停股
        limit_up = df[df['change_pct'] >= 9.9]
        print(f'\n涨停股 (>=9.9%): {len(limit_up)} 只')
        if len(limit_up) > 0:
            print(limit_up[['code', 'name', 'price', 'change_pct', 'volume']].head(15).to_string(index=False))
        
        # 跌停股
        limit_down = df[df['change_pct'] <= -9.9]
        print(f'\n跌停股 (<=-9.9%): {len(limit_down)} 只')
        
        return df
    
    return None


if __name__ == '__main__':
    fetch_all_stocks()
