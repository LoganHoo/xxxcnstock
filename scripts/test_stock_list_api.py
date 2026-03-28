"""
测试新浪财经股票列表API
"""
import requests
import json

def test_sina_api():
    """测试新浪财经API"""
    print("=" * 70)
    print("测试新浪财经股票列表API")
    print("=" * 70)
    
    # 方法1: 原API
    print("\n方法1: 原API")
    url1 = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData'
    params1 = {'page': 1, 'num': 8000, 'sort': 'symbol', 'asc': 1, 'node': 'hs_a', 'symbol': '', '_s_r_a': 'page'}
    headers1 = {'Referer': 'http://finance.sina.com.cn/'}
    
    try:
        resp1 = requests.get(url1, params=params1, headers=headers1, timeout=30)
        print(f"状态码: {resp1.status_code}")
        print(f"响应长度: {len(resp1.text)}")
        print(f"响应前200字符: {resp1.text[:200]}")
        
        if resp1.status_code == 200:
            try:
                data1 = json.loads(resp1.text)
                print(f"解析成功，数据类型: {type(data1)}")
                if isinstance(data1, list):
                    print(f"数据条数: {len(data1)}")
                    if len(data1) > 0:
                        print(f"第一条数据: {data1[0]}")
            except json.JSONDecodeError as e:
                print(f"JSON解析失败: {e}")
    except Exception as e:
        print(f"请求失败: {e}")
    
    # 方法2: 新API
    print("\n方法2: 新API")
    url2 = 'https://hq.sinajs.cn/list=s_sh000001,s_sz399001'
    
    try:
        resp2 = requests.get(url2, timeout=30)
        print(f"状态码: {resp2.status_code}")
        print(f"响应长度: {len(resp2.text)}")
        print(f"响应内容:\n{resp2.text}")
    except Exception as e:
        print(f"请求失败: {e}")
    
    # 方法3: 东方财富API
    print("\n方法3: 东方财富API")
    url3 = 'http://80.push2.eastmoney.com/api/qt/clist/get'
    params3 = {
        'pn': 1,
        'pz': 5000,
        'po': 1,
        'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2,
        'invt': 2,
        'fid': 'f3',
        'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',
        'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152'
    }
    
    try:
        resp3 = requests.get(url3, params=params3, timeout=30)
        print(f"状态码: {resp3.status_code}")
        print(f"响应长度: {len(resp3.text)}")
        
        if resp3.status_code == 200:
            try:
                data3 = resp3.json()
                print(f"解析成功，数据类型: {type(data3)}")
                if isinstance(data3, dict) and 'data' in data3:
                    diff_list = data3['data'].get('diff', [])
                    print(f"数据条数: {len(diff_list)}")
                    if len(diff_list) > 0:
                        print(f"第一条数据: {diff_list[0]}")
            except json.JSONDecodeError as e:
                print(f"JSON解析失败: {e}")
    except Exception as e:
        print(f"请求失败: {e}")


if __name__ == "__main__":
    test_sina_api()
