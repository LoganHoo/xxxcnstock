#!/usr/bin/env python3
"""
数据采集验证测试
"""
import asyncio
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.data_service.fetchers.unified_fetcher import UnifiedFetcher
from datetime import datetime, timedelta


async def test_fetch():
    """测试采集单只股票"""
    print("=" * 60)
    print("数据采集验证测试")
    print("=" * 60)
    print()
    
    # 初始化fetcher
    fetcher = UnifiedFetcher()
    await fetcher.initialize()
    
    # 测试股票代码
    test_code = "000001"  # 平安银行
    
    print(f"测试股票: {test_code}")
    print(f"数据源: UnifiedFetcher (自动主备切换)")
    print()
    
    # 计算日期范围（最近5天）
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    print(f"日期范围: {start_date} ~ {end_date}")
    print()
    
    try:
        # 采集数据
        df = await fetcher.fetch_kline(test_code, start_date, end_date)
        
        if df.empty:
            print("❌ 未获取到数据")
            return False
        
        print(f"✅ 成功获取 {len(df)} 条数据")
        print()
        print("最新5条数据:")
        print("-" * 60)
        
        # 显示最新5条
        recent = df.tail(5)
        for idx, row in recent.iterrows():
            date = row.get('trade_date', row.get('date', 'N/A'))
            open_p = row.get('open', 0)
            high = row.get('high', 0)
            low = row.get('low', 0)
            close = row.get('close', 0)
            volume = row.get('volume', 0)
            
            print(f"{date}: 开¥{open_p:.2f} 高¥{high:.2f} 低¥{low:.2f} 收¥{close:.2f} 量{volume:,.0f}")
        
        print()
        print("=" * 60)
        print("✅ 数据采集验证通过")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"❌ 采集失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_fetch())
    sys.exit(0 if success else 1)
