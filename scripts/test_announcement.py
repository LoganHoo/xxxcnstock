#!/usr/bin/env python3
"""
公告数据采集测试 - 使用 AKShare

测试内容:
1. 公司公告采集
2. 定期报告采集
3. 重大事项采集
4. 公告类型分类
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from services.data_service.fetchers.announcement.akshare_announcement_fetcher import (
    fetch_announcements,
    fetch_company_announcement,
    fetch_periodic_reports,
    fetch_major_events,
    fetch_announcement_history
)
from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_announcement", log_file="system/test_announcement.log")

# 测试日期范围
TEST_DATE = '2025-04-15'  # 工作日
TEST_CODES = ['600000', '000001', '300001']


def test_all_announcements() -> dict:
    """测试全部公告采集"""
    print("\n" + "="*70)
    print("测试1: 全部公告采集")
    print("="*70)
    
    try:
        print(f"测试日期: {TEST_DATE}")
        
        df = fetch_announcements(TEST_DATE, symbol="全部")
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取公告数据: {len(df)} 条")
            print(f"   数据列: {list(df.columns)}")
            
            if 'type' in df.columns:
                type_counts = df['type'].value_counts()
                print(f"   公告类型分布:")
                for t, c in type_counts.head(5).items():
                    print(f"     - {t}: {c} 条")
            
            return {
                'type': 'all_announcements',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无公告数据返回")
            return {
                'type': 'all_announcements',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试全部公告采集失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'all_announcements',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_company_announcement() -> dict:
    """测试指定公司公告采集"""
    print("\n" + "="*70)
    print("测试2: 指定公司公告采集")
    print("="*70)
    
    results = []
    
    for code in TEST_CODES:
        print(f"\n测试股票: {code}")
        
        try:
            # 获取最近30天的公告
            end_date = TEST_DATE
            start_date = (datetime.strptime(TEST_DATE, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
            
            df = fetch_company_announcement(code, start_date, end_date)
            
            if df is not None and not df.empty:
                print(f"  ✅ 成功: {len(df)} 条公告")
                print(f"     日期范围: {df['date'].min()} 至 {df['date'].max()}")
                results.append(True)
            else:
                print(f"  ⚠️ 无公告数据")
                results.append(False)
                
        except Exception as e:
            print(f"  ❌ 失败: {e}")
            results.append(False)
    
    success_count = sum(results)
    
    return {
        'type': 'company_announcement',
        'success': success_count > 0,
        'records': success_count,
        'total': len(TEST_CODES),
        'error': None if success_count > 0 else '全部失败'
    }


def test_periodic_reports() -> dict:
    """测试定期报告采集"""
    print("\n" + "="*70)
    print("测试3: 定期报告采集")
    print("="*70)
    
    try:
        print(f"测试日期: {TEST_DATE}")
        
        df = fetch_periodic_reports(TEST_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取定期报告: {len(df)} 条")
            
            if 'code' in df.columns:
                unique_codes = df['code'].nunique()
                print(f"   涉及公司数: {unique_codes}")
            
            return {
                'type': 'periodic_reports',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无定期报告数据")
            return {
                'type': 'periodic_reports',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试定期报告采集失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'periodic_reports',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def test_major_events() -> dict:
    """测试重大事项采集"""
    print("\n" + "="*70)
    print("测试4: 重大事项采集")
    print("="*70)
    
    try:
        print(f"测试日期: {TEST_DATE}")
        
        df = fetch_major_events(TEST_DATE)
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取重大事项: {len(df)} 条")
            
            if 'title' in df.columns:
                print(f"   示例标题: {df['title'].iloc[0]}")
            
            return {
                'type': 'major_events',
                'success': True,
                'records': len(df),
                'error': None
            }
        else:
            print("⚠️ 无重大事项数据")
            return {
                'type': 'major_events',
                'success': False,
                'records': 0,
                'error': '无数据'
            }
    except Exception as e:
        logger.error(f"测试重大事项采集失败: {e}")
        print(f"❌ 测试失败: {e}")
        return {
            'type': 'major_events',
            'success': False,
            'records': 0,
            'error': str(e)
        }


def run_all_tests():
    """运行所有测试"""
    print("=" * 70)
    print("公告数据采集测试 - AKShare")
    print("=" * 70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据源: AKShare")
    
    results = []
    
    # 测试1: 全部公告
    results.append(test_all_announcements())
    
    # 测试2: 指定公司公告
    results.append(test_company_announcement())
    
    # 测试3: 定期报告
    results.append(test_periodic_reports())
    
    # 测试4: 重大事项
    results.append(test_major_events())
    
    # 汇总
    print("\n" + "="*70)
    print("测试汇总")
    print("="*70)
    
    for result in results:
        status = "✅" if result['success'] else "❌"
        data_type = {
            'all_announcements': '全部公告',
            'company_announcement': '公司公告',
            'periodic_reports': '定期报告',
            'major_events': '重大事项'
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
