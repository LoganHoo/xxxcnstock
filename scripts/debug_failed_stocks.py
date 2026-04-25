#!/usr/bin/env python3
"""
调试失败股票的具体原因
"""
import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# 失败股票样本（000041-000099范围内的部分股票）
FAILED_SAMPLES = [
    '000041', '000044', '000051', '000052', '000053',
    '000054', '000057', '000064', '000067', '000071'
]


async def test_single_stock(code: str):
    """测试单只股票数据采集"""
    from services.data_service.datasource.manager import DataSourceManager
    
    print(f"\n{'='*60}")
    print(f"测试股票: {code}")
    print('='*60)
    
    try:
        manager = DataSourceManager()
        
        # 尝试获取K线数据
        df = await manager.fetch_kline(
            code=code,
            start_date='2024-01-01',
            end_date='2026-04-23',
            frequency='d'
        )
        
        if df is not None and len(df) > 0:
            print(f"✅ 成功获取数据: {len(df)} 条记录")
            print(f"   日期范围: {df.index[0]} ~ {df.index[-1]}")
            return True
        else:
            print(f"❌ 无数据返回")
            return False
            
    except Exception as e:
        print(f"❌ 采集失败: {e}")
        return False


def check_stock_status():
    """检查这些股票的状态（是否退市、停牌等）"""
    print("\n" + "="*60)
    print("检查股票状态")
    print("="*60)
    
    # 尝试从Baostock获取股票列表
    try:
        import baostock as bs
        import pandas as pd
        from datetime import datetime, timedelta
        
        # 登录
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ Baostock登录失败: {lg.error_msg}")
            return
        
        # 获取股票列表
        query_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        rs = bs.query_all_stock(day=query_date)
        
        stocks = []
        while rs.next():
            row = rs.get_row_data()
            stocks.append({
                'code': row[0],
                'trade_status': row[1] if len(row) > 1 else '0',
                'name': row[2] if len(row) > 2 else ''
            })
        
        bs.logout()
        
        # 检查失败样本
        df = pd.DataFrame(stocks)
        
        print(f"\n📊 Baostock股票列表总数: {len(df)}")
        
        for code in FAILED_SAMPLES:
            # 深市代码格式: sz.000xxx
            full_code = f"sz.{code}"
            stock_info = df[df['code'] == full_code]
            
            if stock_info.empty:
                print(f"\n   {code}: ❌ 不在Baostock列表中")
            else:
                status = stock_info.iloc[0]['trade_status']
                name = stock_info.iloc[0]['name']
                status_str = "✅ 正常交易" if status == '1' else f"⚠️  状态异常({status})"
                print(f"\n   {code}: {status_str} - {name}")
                
    except Exception as e:
        print(f"❌ 检查失败: {e}")


def analyze_code_pattern():
    """分析失败股票代码模式"""
    print("\n" + "="*60)
    print("失败股票代码模式分析")
    print("="*60)
    
    # 000041-000099 这个区间的股票特点
    print("\n📋 000041-000099 区间股票特点:")
    print("   这个区间主要是早期深市主板股票")
    print("   可能存在的问题:")
    print("   1. 部分股票已退市或更名")
    print("   2. 部分股票已迁移到其他板块")
    print("   3. 数据格式可能与其他股票不同")
    
    # 检查是否有连续的空缺
    all_codes = set(int(c) for c in FAILED_SAMPLES)
    missing_in_range = []
    
    for i in range(41, 100):
        code = f"000{i:03d}"
        if code not in FAILED_SAMPLES:
            missing_in_range.append(code)
    
    if missing_in_range:
        print(f"\n   000041-000099范围内未失败的代码: {len(missing_in_range)} 只")
        print(f"   示例: {', '.join(missing_in_range[:10])}")


async def main():
    """主函数"""
    print("\n" + "="*70)
    print("失败股票深度分析")
    print("="*70)
    
    # 1. 分析代码模式
    analyze_code_pattern()
    
    # 2. 检查股票状态
    check_stock_status()
    
    # 3. 测试几只样本股票
    print("\n" + "="*60)
    print("测试样本股票数据采集")
    print("="*60)
    
    for code in FAILED_SAMPLES[:3]:  # 只测试前3只
        await test_single_stock(code)
    
    print("\n" + "="*70)
    print("分析完成")
    print("="*70)


if __name__ == '__main__':
    asyncio.run(main())
