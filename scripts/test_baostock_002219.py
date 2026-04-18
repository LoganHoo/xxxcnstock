#!/usr/bin/env python3
"""
通过 baostock 测试 002219 一个交易日的价格
"""
import sys
sys.path.insert(0, '.')

import baostock as bs
from datetime import datetime, timedelta

def main():
    # 登录
    print("登录 baostock...")
    lg = bs.login()
    print(f"登录结果: {lg.error_msg}")
    
    # 测试股票
    code = "002219"
    code_bs = f"sz.{code}"
    
    # 获取最近几个交易日
    today = datetime.now()
    dates_to_try = []
    for i in range(10):
        d = today - timedelta(days=i)
        if d.weekday() < 5:  # 周一到周五
            dates_to_try.append(d.strftime('%Y-%m-%d'))
    
    print(f"\n测试股票: {code}")
    print(f"尝试日期: {dates_to_try[:5]}")
    print()
    
    # 获取日线数据
    for test_date in dates_to_try[:5]:
        print(f"\n查询日期: {test_date}")
        print("-" * 60)
        
        rs = bs.query_history_k_data_plus(
            code_bs,
            "date,code,open,high,low,close,volume,amount",
            start_date=test_date,
            end_date=test_date,
            frequency="d"
        )
        
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            print(f"  日期: {row[0]}")
            print(f"  代码: {row[1]}")
            print(f"  开盘: ¥{row[2]}")
            print(f"  最高: ¥{row[3]}")
            print(f"  最低: ¥{row[4]}")
            print(f"  收盘: ¥{row[5]}")
            print(f"  成交量: {row[6]}")
            print(f"  成交额: {row[7]}")
            break
        else:
            print(f"  无数据 ({rs.error_msg})")
    
    # 登出
    bs.logout()
    print("\n测试完成")

if __name__ == "__main__":
    main()
