#!/usr/bin/env python3
"""
市场行为数据采集测试 - 使用 AKShare

测试内容:
1. 龙虎榜数据采集
2. 资金流向数据采集
3. 板块资金流向采集
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from services.data_service.fetchers.market_behavior.akshare_market_behavior_fetcher import (
    fetch_dragon_tiger_list,
    fetch_dragon_tiger_history,
    fetch_money_flow,
    fetch_sector_money_flow
)
from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_market_behavior", log_file="system/test_market_behavior.log")

# 测试日期范围
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=30)


def test_dragon_tiger_list() -> dict:
    """测试龙虎榜数据采集"""
    print("\n" + "="*70)
    print("测试1: 龙虎榜数据采集")
    print("="*70)
    
    try:
        # 使用最近的工作日（2025-04-15 是周二）
        test_date = '2025-04-15'
        
        print(f"测试日期: {test_date}（工作日）")
        
        df = fetch_dragon_tiger_list(test_date)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取龙虎榜数据: {len(df)} 条")
            print(f"   数据列: {list(df.columns)}")
            print(f"   日期: {df['date'].iloc[0]}")
            
            if 'code' in df.columns:
                unique_codes = df['code'].nunique()
                print(f"   上榜股票数: {unique_codes}")
            
            return {
                'type': 'dragon_tiger',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无龙虎榜数据返回")
            return {
                'type': 'dragon_tiger',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试龙虎榜采集失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'dragon_tiger',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_money_flow() -> dict:
    """测试资金流向数据采集"""
    print("\n" + "="*70)
    print("测试2: 资金流向数据采集")
    print("="*70)
    
    test_codes = ['600000', '000001', '300001']
    
    try:
        results = []
        
        for code in test_codes:
            print(f"\n测试股票: {code}")
            
            try:
                data = fetch_money_flow(code)
                
                if data:
                    print(f"  ✅ 成功获取资金流向")
                    print(f"     主力净流入: {data.get('main_inflow', 'N/A')}")
                    results.append(True)
                else:
                    print(f"  ⚠️ 无数据")
                    results.append(False)
                    
            except Exception as e:
                print(f"  ❌ 失败: {e}")
                results.append(False)
        
        success_count = sum(results)
        
        return {
            'type': 'money_flow',
            'success': success_count > 0,
            'records': success_count,
            'total': len(test_codes),
            'error': None if success_count > 0 else '全部失败'
        }
        
    except Exception as e:
        logger.error(f"测试资金流向采集失败: {e}")
        return {
            'type': 'money_flow',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_sector_flow() -> dict:
    """测试板块资金流向"""
    print("\n" + "="*70)
    print("测试3: 板块资金流向")
    print("="*70)
    
    try:
        df = fetch_sector_money_flow(sector_type="industry")
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取板块资金流向: {len(df)} 条")
            print(f"   数据列: {list(df.columns)[:5]}...")
            
            if 'sector' in df.columns:
                print(f"   板块数: {df['sector'].nunique()}")
                print(f"   前5板块: {df['sector'].head().tolist()}")
            
            return {
                'type': 'sector_flow',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无板块资金流向数据")
            return {
                'type': 'sector_flow',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试板块资金流向失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'sector_flow',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def run_all_tests():
    """运行所有测试"""
    print("=" * 70)
    print("市场行为数据采集测试 - AKShare")
    print("=" * 70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据源: AKShare")
    
    results = []
    
    # 测试1: 龙虎榜
    results.append(test_dragon_tiger_list())
    
    # 测试2: 资金流向
    results.append(test_money_flow())
    
    # 测试3: 板块资金流向
    results.append(test_sector_flow())
    
    # 汇总
    print("\n" + "="*70)
    print("测试汇总")
    print("="*70)
    
    for result in results:
        status = "✅" if result['success'] else "❌"
        data_type = {
            'dragon_tiger': '龙虎榜',
            'money_flow': '资金流向',
            'sector_flow': '板块资金流向'
        }.get(result['type'], result['type'])
        
        print(f"{status} {data_type}", end="")
        if 'records' in result and 'total' in result:
            print(f": {result['records']}/{result['total']} 成功")
        elif 'records' in result:
            print(f": {result['records']} 条")
        else:
            print()
        
        if not result['success'] and result.get('error'):
            print(f"   错误: {result['error']}")
    
    total_tests = len(results)
    total_success = sum(1 for r in results if r['success'])
    
    print(f"\n总计: {total_success}/{total_tests} 通过 ({total_success/total_tests*100:.1f}%)")
    print("="*70)
    
    return results


if __name__ == "__main__":
    results = run_all_tests()
