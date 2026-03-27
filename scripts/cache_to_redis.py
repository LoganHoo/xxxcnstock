"""缓存分析结果到Redis"""
import sys
sys.path.insert(0, 'D:/workstation/xcnstock')

import polars as pl
from datetime import datetime
from services.data_service.storage.enhanced_storage import get_storage

def main():
    storage = get_storage()
    print('Redis状态:', '已连接' if storage.redis_client else '未连接')
    
    if not storage.redis_client:
        print('Redis未连接，退出')
        return
    
    # 读取分析结果
    df = pl.read_parquet('data/enhanced_scores_full.parquet')
    print(f'读取数据: {len(df)} 只股票')
    
    # 缓存统计数据
    stats = {
        'total': len(df),
        's_count': len(df.filter(pl.col('grade') == 'S')),
        'a_count': len(df.filter(pl.col('grade') == 'A')),
        'b_count': len(df.filter(pl.col('grade') == 'B')),
        'c_count': len(df.filter(pl.col('grade') == 'C')),
        'update_time': datetime.now().isoformat()
    }
    storage.cache_set('statistics:all', stats, ttl=3600)
    print('统计已缓存:', stats)
    
    # 缓存S级全部
    s_stocks = df.filter(pl.col('grade') == 'S').sort('enhanced_score', descending=True)
    s_list = s_stocks.select(['code', 'name', 'price', 'change_pct', 'enhanced_score', 'rsi', 'momentum_10d', 'reasons']).to_dicts()
    storage.cache_set('stocks:s_grade_all', s_list, ttl=300)
    print(f'S级已缓存: {len(s_list)} 只')
    
    # 缓存A级全部
    a_stocks = df.filter(pl.col('grade') == 'A').sort('enhanced_score', descending=True)
    a_list = a_stocks.select(['code', 'name', 'price', 'change_pct', 'enhanced_score', 'rsi', 'momentum_10d', 'reasons']).to_dicts()
    storage.cache_set('stocks:a_grade_all', a_list, ttl=300)
    print(f'A级已缓存: {len(a_list)} 只')
    
    # 缓存S级Top 50和A级Top 100
    storage.cache_set('stocks:s_grade_top50', s_list[:50], ttl=300)
    storage.cache_set('stocks:a_grade_top100', a_list[:100], ttl=300)
    
    print('\n全部数据已缓存到Redis!')
    print('缓存键:')
    print('  - xcnstock:statistics:all')
    print('  - xcnstock:stocks:s_grade_all')
    print('  - xcnstock:stocks:a_grade_all')
    print('  - xcnstock:stocks:s_grade_top50')
    print('  - xcnstock:stocks:a_grade_top100')

if __name__ == '__main__':
    main()
