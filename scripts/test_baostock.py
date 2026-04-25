#!/usr/bin/env python3
"""
测试Baostock数据源连接和数据获取
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import baostock as bs
import pandas as pd
from datetime import datetime

print("=" * 60)
print("Baostock 连接测试")
print("=" * 60)

# 1. 登录测试
print("\n1. 登录测试...")
lg = bs.login()
print(f"登录结果: {lg.error_code} - {lg.error_msg}")

if lg.error_code != '0':
    print("❌ 登录失败!")
    sys.exit(1)

print("✅ 登录成功!")

# 2. 测试不同代码格式
print("\n2. 测试代码格式...")
test_codes = [
    ("sh.600000", "浦发银行-标准格式"),
    ("600000", "纯数字-需要转换"),
    ("sh600000", "sh前缀-无点"),
    ("sz.000001", "平安银行-标准格式"),
    ("000001", "纯数字-需要转换"),
]

start_date = (datetime.now() - pd.Timedelta(days=10)).strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')

for code, desc in test_codes:
    print(f"\n测试: {desc} ({code})")
    try:
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        
        if rs.error_code != '0':
            print(f"  ❌ 查询失败: {rs.error_msg}")
            continue
        
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        
        if data:
            print(f"  ✅ 成功! 获取到 {len(data)} 条数据")
            df = pd.DataFrame(data, columns=rs.fields)
            print(f"  数据预览:")
            print(df.head(3).to_string())
        else:
            print(f"  ⚠️  查询成功但无数据")
            
    except Exception as e:
        print(f"  ❌ 异常: {e}")

# 3. 测试股票代码转换
print("\n3. 测试代码转换逻辑...")

def convert_code(code: str) -> str:
    """转换代码格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    return code

test_raw_codes = ['600000', '000001', '300001', '688001']
for raw_code in test_raw_codes:
    converted = convert_code(raw_code)
    print(f"  {raw_code} -> {converted}")
    
    # 验证转换后的代码
    try:
        rs = bs.query_history_k_data_plus(
            converted,
            "date,code,close",
            start_date=start_date,
            end_date=end_date,
            frequency="d"
        )
        
        if rs.error_code == '0':
            count = 0
            while rs.next():
                count += 1
            print(f"    ✅ 可获取数据: {count} 条")
        else:
            print(f"    ❌ 查询失败: {rs.error_msg}")
    except Exception as e:
        print(f"    ❌ 异常: {e}")

# 4. 测试日期格式
print("\n4. 测试日期格式...")
date_formats = [
    (datetime.now().strftime('%Y-%m-%d'), "标准格式 YYYY-MM-DD"),
    (datetime.now().strftime('%Y%m%d'), "无分隔符 YYYYMMDD"),
]

for date_str, desc in date_formats:
    print(f"\n测试日期格式: {desc} ({date_str})")
    try:
        rs = bs.query_history_k_data_plus(
            "sh.600000",
            "date,close",
            start_date=date_str,
            end_date=date_str,
            frequency="d"
        )
        
        if rs.error_code == '0':
            count = 0
            while rs.next():
                count += 1
            print(f"  ✅ 成功: {count} 条数据")
        else:
            print(f"  ❌ 失败: {rs.error_msg}")
    except Exception as e:
        print(f"  ❌ 异常: {e}")

# 5. 登出
bs.logout()
print("\n" + "=" * 60)
print("测试完成!")
print("=" * 60)
