"""
修复股票列表获取脚本 - 修复版本
使用东方财富API获取完整的A股列表
"""
import requests
import pandas as pd
import json
from pathlib import Path
from datetime import datetime


def safe_float(value, default=0):
    """安全转换为浮点数"""
    try:
        if value is None or value == '-' or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """安全转换为整数"""
    try:
        if value is None or value == '-' or value == '':
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def fetch_stock_list_eastmoney():
    """使用东方财富API获取A股列表"""
    print("=" * 70)
    print("使用东方财富API获取A股列表")
    print("=" * 70)
    
    all_stocks = []
    page = 1
    page_size = 5000
    
    while True:
        print(f"获取第 {page} 页...")
        
        url = 'http://80.push2.eastmoney.com/api/qt/clist/get'
        params = {
            'pn': page,
            'pz': page_size,
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
            resp = requests.get(url, params=params, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                
                if isinstance(data, dict) and 'data' in data:
                    diff_list = data['data'].get('diff', [])
                    
                    if not diff_list:
                        print(f"第 {page} 页没有数据，停止获取")
                        break
                    
                    print(f"  获取到 {len(diff_list)} 条数据")
                    
                    for stock in diff_list:
                        code = stock.get('f12', '')
                        name = stock.get('f14', '')
                        
                        if code and name:
                            all_stocks.append({
                                'code': code,
                                'name': name,
                                'price': safe_float(stock.get('f2')),
                                'change_pct': safe_float(stock.get('f3')),
                                'volume': safe_int(stock.get('f5')),
                                'amount': safe_float(stock.get('f6')),
                                'market_cap': safe_float(stock.get('f20')),
                            })
                    
                    page += 1
                    
                else:
                    print(f"第 {page} 页数据格式错误，停止获取")
                    break
            else:
                print(f"第 {page} 页请求失败，状态码: {resp.status_code}")
                break
        
        except Exception as e:
            print(f"第 {page} 页请求异常: {e}")
            break
    
    print(f"\n总共获取到 {len(all_stocks)} 只股票")
    
    if all_stocks:
        df = pd.DataFrame(all_stocks)
        
        # 保存
        output_file = Path('data/stock_list.parquet')
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False)
        
        print(f"股票列表已保存: {output_file}")
        print(f"文件大小: {output_file.stat().st_size / 1024:.1f} KB")
        
        # 统计
        print(f"\n股票统计:")
        print(f"  总数: {len(df)} 只")
        print(f"  沪市: {len(df[df['code'].str.startswith('6')])} 只")
        print(f"  深市: {len(df[~df['code'].str.startswith('6')])} 只")
        
        # 显示前10只
        print(f"\n前10只股票:")
        print(df.head(10).to_string(index=False))
        
        return True
    else:
        print("❌ 没有获取到股票数据")
        return False


def fetch_stock_list_sina():
    """使用新浪财经API获取A股列表（备用方案）"""
    print("=" * 70)
    print("使用新浪财经API获取A股列表")
    print("=" * 70)
    
    all_stocks = []
    
    # 获取所有A股
    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData'
    params = {
        'page': 1,
        'num': 10000,
        'sort': 'symbol',
        'asc': 1,
        'node': 'hs_a',
        'symbol': '',
        '_s_r_a': 'page'
    }
    headers = {'Referer': 'http://finance.sina.com.cn/'}
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            data = json.loads(resp.text)
            
            if isinstance(data, list):
                for stock in data:
                    code = stock.get('code', '')
                    name = stock.get('name', '')
                    
                    if code and name and code.startswith(('6', '0', '3')):
                        all_stocks.append({
                            'code': code,
                            'name': name,
                            'price': safe_float(stock.get('trade')),
                            'change_pct': safe_float(stock.get('changepercent')),
                            'volume': safe_int(stock.get('volume')),
                            'amount': safe_float(stock.get('amount')),
                            'market_cap': safe_float(stock.get('mktcap')),
                        })
                
                print(f"获取到 {len(all_stocks)} 只股票")
                
                if all_stocks:
                    df = pd.DataFrame(all_stocks)
                    
                    # 保存
                    output_file = Path('data/stock_list.parquet')
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    df.to_parquet(output_file, index=False)
                    
                    print(f"股票列表已保存: {output_file}")
                    print(f"文件大小: {output_file.stat().st_size / 1024:.1f} KB")
                    
                    return True
            else:
                print(f"数据格式错误: {type(data)}")
                return False
        else:
            print(f"请求失败，状态码: {resp.status_code}")
            return False
    
    except Exception as e:
        print(f"请求异常: {e}")
        return False


def main():
    """主函数"""
    print(f"\n开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # 优先使用东方财富API
    success = fetch_stock_list_eastmoney()
    
    if not success:
        print("\n东方财富API失败，尝试新浪财经API...")
        success = fetch_stock_list_sina()
    
    if success:
        print("\n✅ 股票列表获取成功")
    else:
        print("\n❌ 股票列表获取失败")
    
    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
