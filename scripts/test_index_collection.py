#!/usr/bin/env python3
"""
大盘指数数据采集测试 - 使用 Baostock

测试内容:
1. 上证指数采集
2. 深证成指采集
3. 创业板指采集
4. 沪深300采集
5. 中证500采集
6. 科创50采集
7. 上证50采集
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from services.data_service.fetchers.index.baostock_index_fetcher import (
    fetch_sh_index,
    fetch_sz_index,
    fetch_cy_index,
    fetch_hs300,
    fetch_zz500,
    fetch_all_major_indices
)
from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_index", log_file="system/test_index.log")

# 测试日期范围
START_DATE = '2025-04-01'
END_DATE = '2025-04-15'


def test_sh_index() -> dict:
    """测试上证指数"""
    print("\n" + "="*70)
    print("测试1: 上证指数 (sh.000001)")
    print("="*70)
    
    try:
        print(f"日期范围: {START_DATE} 至 {END_DATE}")
        
        df = fetch_sh_index(START_DATE, END_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取上证指数: {len(df)} 条")
            print(f"   最新收盘价: {df['close'].iloc[-1]:.2f}")
            print(f"   涨跌幅: {df['pctChg'].iloc[-1]:.2f}%")
            
            return {
                'type': 'sh_index',
                'name': '上证指数',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无上证指数数据")
            return {
                'type': 'sh_index',
                'name': '上证指数',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试上证指数失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'sh_index',
            'name': '上证指数',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_sz_index() -> dict:
    """测试深证成指"""
    print("\n" + "="*70)
    print("测试2: 深证成指 (sz.399001)")
    print("="*70)
    
    try:
        print(f"日期范围: {START_DATE} 至 {END_DATE}")
        
        df = fetch_sz_index(START_DATE, END_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取深证成指: {len(df)} 条")
            print(f"   最新收盘价: {df['close'].iloc[-1]:.2f}")
            
            return {
                'type': 'sz_index',
                'name': '深证成指',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无深证成指数据")
            return {
                'type': 'sz_index',
                'name': '深证成指',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试深证成指失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'sz_index',
            'name': '深证成指',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_cy_index() -> dict:
    """测试创业板指"""
    print("\n" + "="*70)
    print("测试3: 创业板指 (sz.399006)")
    print("="*70)
    
    try:
        print(f"日期范围: {START_DATE} 至 {END_DATE}")
        
        df = fetch_cy_index(START_DATE, END_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取创业板指: {len(df)} 条")
            print(f"   最新收盘价: {df['close'].iloc[-1]:.2f}")
            
            return {
                'type': 'cy_index',
                'name': '创业板指',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无创业板指数据")
            return {
                'type': 'cy_index',
                'name': '创业板指',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试创业板指失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'cy_index',
            'name': '创业板指',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_hs300() -> dict:
    """测试沪深300"""
    print("\n" + "="*70)
    print("测试4: 沪深300 (sh.000300)")
    print("="*70)
    
    try:
        print(f"日期范围: {START_DATE} 至 {END_DATE}")
        
        df = fetch_hs300(START_DATE, END_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取沪深300: {len(df)} 条")
            print(f"   最新收盘价: {df['close'].iloc[-1]:.2f}")
            
            return {
                'type': 'hs300',
                'name': '沪深300',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无沪深300数据")
            return {
                'type': 'hs300',
                'name': '沪深300',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试沪深300失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'hs300',
            'name': '沪深300',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_zz500() -> dict:
    """测试中证500"""
    print("\n" + "="*70)
    print("测试5: 中证500 (sh.000905)")
    print("="*70)
    
    try:
        print(f"日期范围: {START_DATE} 至 {END_DATE}")
        
        df = fetch_zz500(START_DATE, END_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取中证500: {len(df)} 条")
            print(f"   最新收盘价: {df['close'].iloc[-1]:.2f}")
            
            return {
                'type': 'zz500',
                'name': '中证500',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无中证500数据")
            return {
                'type': 'zz500',
                'name': '中证500',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试中证500失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'zz500',
            'name': '中证500',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_all_indices() -> dict:
    """测试批量获取所有主要指数"""
    print("\n" + "="*70)
    print("测试6: 批量获取所有主要指数")
    print("="*70)
    
    try:
        print(f"日期范围: {START_DATE} 至 {END_DATE}")
        
        indices = fetch_all_major_indices(START_DATE, END_DATE)
        
        if indices:
            print(f"✅ 成功获取 {len(indices)} 个指数")
            
            for name, df in indices.items():
                if not df.empty:
                    print(f"   {name}: {len(df)} 条, 最新: {df['close'].iloc[-1]:.2f}")
            
            return {
                'type': 'all_indices',
                'name': '全部主要指数',
                'success': True,
                'records': len(indices),
                'error': None
            }
        else:
            print("⚠️ 无指数数据")
            return {
                'type': 'all_indices',
                'name': '全部主要指数',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试批量获取指数失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'all_indices',
            'name': '全部主要指数',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def run_all_tests():
    """运行所有测试"""
    print("=" * 70)
    print("大盘指数数据采集测试 - Baostock")
    print("=" * 70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据源: Baostock")
    
    results = []
    
    # 测试1: 上证指数
    results.append(test_sh_index())
    
    # 测试2: 深证成指
    results.append(test_sz_index())
    
    # 测试3: 创业板指
    results.append(test_cy_index())
    
    # 测试4: 沪深300
    results.append(test_hs300())
    
    # 测试5: 中证500
    results.append(test_zz500())
    
    # 测试6: 批量获取
    results.append(test_all_indices())
    
    # 汇总
    print("\n" + "="*70)
    print("测试汇总")
    print("="*70)
    
    for result in results:
        status = "✅" if result['success'] else "❌"
        print(f"{status} {result['name']}: {result['records']} 条")
        
        if not result['success'] and result.get('error'):
            print(f"   错误: {result['error']}")
    
    total_tests = len(results)
    total_success = sum(1 for r in results if r['success'])
    
    print(f"\n总计: {total_success}/{total_tests} 通过 ({total_success/total_tests*100:.1f}%)")
    print("="*70)
    
    return results


if __name__ == "__main__":
    results = run_all_tests()
