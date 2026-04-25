#!/usr/bin/env python3
"""
财务数据采集测试 - 使用 Baostock

测试内容:
1. 资产负债表采集
2. 利润表采集
3. 现金流量表采集
4. 财务指标采集
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
from datetime import datetime
from pathlib import Path

from services.data_service.fetchers.financial.baostock_financial_fetcher import (
    fetch_balance_sheet_baostock,
    fetch_income_statement_baostock,
    fetch_cash_flow_baostock,
    fetch_financial_indicators_baostock
)
from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_financial", log_file="system/test_financial.log")

# 测试股票
TEST_CODES = ['600000', '000001', '300001', '000002']


def test_balance_sheet(code: str) -> dict:
    """测试资产负债表采集"""
    logger.info(f"测试 {code} 资产负债表")
    
    try:
        df = fetch_balance_sheet_baostock(code, years=2)
        
        if df is not None and not df.empty:
            return {
                'code': code,
                'type': 'balance_sheet',
                'success': True,
                'records': len(df),
                'columns': len(df.columns),
                'error': None
            }
        else:
            return {
                'code': code,
                'type': 'balance_sheet',
                'success': False,
                'records': 0,
                'columns': 0,
                'error': '无数据返回'
            }
    except Exception as e:
        logger.error(f"测试 {code} 资产负债表失败: {e}")
        return {
            'code': code,
            'type': 'balance_sheet',
            'success': False,
            'records': 0,
            'columns': 0,
            'error': str(e)
        }


def test_income_statement(code: str) -> dict:
    """测试利润表采集"""
    logger.info(f"测试 {code} 利润表")
    
    try:
        df = fetch_income_statement_baostock(code, years=2)
        
        if df is not None and not df.empty:
            return {
                'code': code,
                'type': 'income_statement',
                'success': True,
                'records': len(df),
                'columns': len(df.columns),
                'error': None
            }
        else:
            return {
                'code': code,
                'type': 'income_statement',
                'success': False,
                'records': 0,
                'columns': 0,
                'error': '无数据返回'
            }
    except Exception as e:
        logger.error(f"测试 {code} 利润表失败: {e}")
        return {
            'code': code,
            'type': 'income_statement',
            'success': False,
            'records': 0,
            'columns': 0,
            'error': str(e)
        }


def test_cash_flow(code: str) -> dict:
    """测试现金流量表采集"""
    logger.info(f"测试 {code} 现金流量表")
    
    try:
        df = fetch_cash_flow_baostock(code, years=2)
        
        if df is not None and not df.empty:
            return {
                'code': code,
                'type': 'cash_flow',
                'success': True,
                'records': len(df),
                'columns': len(df.columns),
                'error': None
            }
        else:
            return {
                'code': code,
                'type': 'cash_flow',
                'success': False,
                'records': 0,
                'columns': 0,
                'error': '无数据返回'
            }
    except Exception as e:
        logger.error(f"测试 {code} 现金流量表失败: {e}")
        return {
            'code': code,
            'type': 'cash_flow',
            'success': False,
            'records': 0,
            'columns': 0,
            'error': str(e)
        }


def test_financial_indicators(code: str) -> dict:
    """测试财务指标采集"""
    logger.info(f"测试 {code} 财务指标")
    
    try:
        df = fetch_financial_indicators_baostock(code, years=2)
        
        if df is not None and not df.empty:
            return {
                'code': code,
                'type': 'financial_indicators',
                'success': True,
                'records': len(df),
                'columns': len(df.columns),
                'error': None
            }
        else:
            return {
                'code': code,
                'type': 'financial_indicators',
                'success': False,
                'records': 0,
                'columns': 0,
                'error': '无数据返回'
            }
    except Exception as e:
        logger.error(f"测试 {code} 财务指标失败: {e}")
        return {
            'code': code,
            'type': 'financial_indicators',
            'success': False,
            'records': 0,
            'columns': 0,
            'error': str(e)
        }


def run_all_tests():
    """运行所有测试"""
    print("=" * 70)
    print("财务数据采集测试 - Baostock")
    print("=" * 70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {', '.join(TEST_CODES)}")
    print(f"数据源: Baostock")
    
    all_results = []
    
    # 测试每只股票的各类财务数据
    for code in TEST_CODES:
        print(f"\n{'='*70}")
        print(f"测试股票: {code}")
        print(f"{'='*70}")
        
        # 资产负债表
        result = test_balance_sheet(code)
        all_results.append(result)
        status = "✅" if result['success'] else "❌"
        print(f"{status} 资产负债表: {result['records']} 条记录, {result['columns']} 个字段")
        if not result['success']:
            print(f"   错误: {result['error']}")
        
        # 利润表
        result = test_income_statement(code)
        all_results.append(result)
        status = "✅" if result['success'] else "❌"
        print(f"{status} 利润表: {result['records']} 条记录, {result['columns']} 个字段")
        if not result['success']:
            print(f"   错误: {result['error']}")
        
        # 现金流量表
        result = test_cash_flow(code)
        all_results.append(result)
        status = "✅" if result['success'] else "❌"
        print(f"{status} 现金流量表: {result['records']} 条记录, {result['columns']} 个字段")
        if not result['success']:
            print(f"   错误: {result['error']}")
        
        # 财务指标
        result = test_financial_indicators(code)
        all_results.append(result)
        status = "✅" if result['success'] else "❌"
        print(f"{status} 财务指标: {result['records']} 条记录, {result['columns']} 个字段")
        if not result['success']:
            print(f"   错误: {result['error']}")
    
    # 总体汇总
    print(f"\n{'='*70}")
    print("总体测试汇总")
    print(f"{'='*70}")
    
    total_tests = len(all_results)
    total_success = sum(1 for r in all_results if r['success'])
    
    print(f"\n总测试数: {total_tests}")
    print(f"成功数: {total_success}")
    print(f"失败数: {total_tests - total_success}")
    print(f"成功率: {total_success/total_tests*100:.1f}%")
    
    # 按类型统计
    print(f"\n按类型统计:")
    for data_type in ['balance_sheet', 'income_statement', 'cash_flow', 'financial_indicators']:
        type_results = [r for r in all_results if r['type'] == data_type]
        type_success = sum(1 for r in type_results if r['success'])
        type_name = {
            'balance_sheet': '资产负债表',
            'income_statement': '利润表',
            'cash_flow': '现金流量表',
            'financial_indicators': '财务指标'
        }.get(data_type, data_type)
        print(f"  {type_name}: {type_success}/{len(type_results)} 成功")
    
    print(f"\n{'='*70}")
    
    return all_results


if __name__ == "__main__":
    results = run_all_tests()
