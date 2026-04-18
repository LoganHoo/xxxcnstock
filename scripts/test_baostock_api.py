#!/usr/bin/env python3
"""
测试Baostock API各个接口
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def test_baostock_apis():
    """测试Baostock各个API接口"""
    try:
        import baostock as bs
    except ImportError:
        print("安装 baostock...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "baostock", "-q"])
        import baostock as bs
    
    print("=" * 80)
    print("测试Baostock API")
    print("=" * 80)
    
    # 登录
    print("\n1. 登录...")
    lg = bs.login()
    print(f"   结果: {lg.error_code} - {lg.error_msg}")
    
    if lg.error_code != '0':
        print("登录失败!")
        return
    
    # 测试股票代码
    test_codes = ['sh.600000', 'sz.000001', 'sh.688001']
    
    # 2. 测试query_history_k_data_plus (K线数据)
    print("\n2. 测试query_history_k_data_plus (K线数据)...")
    for code in test_codes:
        rs = bs.query_history_k_data_plus(code,
            "date,code,open,high,low,close,volume",
            start_date='2025-04-01',
            end_date='2025-04-17',
            frequency="d")
        print(f"   {code}: {rs.error_code} - {rs.error_msg}")
        if rs.error_code == '0':
            count = 0
            while rs.next():
                count += 1
            print(f"      获取到 {count} 条数据")
    
    # 3. 测试query_history_k_data_plus (估值数据)
    print("\n3. 测试query_history_k_data_plus (估值数据: peTTM,pbMRQ)...")
    for code in test_codes:
        rs = bs.query_history_k_data_plus(code,
            "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM",
            start_date='2025-04-01',
            end_date='2025-04-17',
            frequency="d")
        print(f"   {code}: {rs.error_code} - {rs.error_msg}")
        if rs.error_code == '0':
            count = 0
            data_list = []
            while rs.next():
                row = rs.get_row_data()
                data_list.append(row)
                count += 1
            print(f"      获取到 {count} 条数据")
            if data_list:
                print(f"      示例: {data_list[-1]}")
    
    # 4. 测试query_profit_data (盈利能力)
    print("\n4. 测试query_profit_data (盈利能力)...")
    for code in test_codes:
        rs = bs.query_profit_data(code=code, year=2024, quarter=3)
        print(f"   {code}: {rs.error_code} - {rs.error_msg}")
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            print(f"      数据: {row}")
    
    # 5. 测试query_growth_data (成长能力)
    print("\n5. 测试query_growth_data (成长能力)...")
    for code in test_codes:
        rs = bs.query_growth_data(code=code, year=2024, quarter=3)
        print(f"   {code}: {rs.error_code} - {rs.error_msg}")
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            print(f"      数据: {row}")
    
    # 6. 测试query_stock_industry (行业数据)
    print("\n6. 测试query_stock_industry (行业数据)...")
    rs = bs.query_stock_industry()
    print(f"   结果: {rs.error_code} - {rs.error_msg}")
    if rs.error_code == '0':
        count = 0
        while rs.next() and count < 5:
            row = rs.get_row_data()
            print(f"      {row}")
            count += 1
    
    # 7. 测试query_all_stock (股票列表)
    print("\n7. 测试query_all_stock (股票列表)...")
    rs = bs.query_all_stock(day='2025-04-17')
    print(f"   结果: {rs.error_code} - {rs.error_msg}")
    if rs.error_code == '0':
        count = 0
        while rs.next() and count < 5:
            row = rs.get_row_data()
            print(f"      {row}")
            count += 1
    
    # 登出
    bs.logout()
    
    print("\n" + "=" * 80)
    print("测试完成")
    print("=" * 80)


if __name__ == "__main__":
    test_baostock_apis()
