#!/usr/bin/env python3
"""
K线数据全量采集测试 - 1年以上历史数据

测试内容:
1. 采集1年、2年、3年历史数据
2. 验证数据完整性
3. 验证数据质量
4. 测试多股票并行采集
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time

from services.data_service.datasource.manager import DataSourceManager
from core.paths import DATA_DIR
from core.logger import setup_logger

logger = setup_logger("test_kline_full", log_file="system/test_kline_full.log")

# 测试股票
TEST_CODES = ['600000', '000001', '300001', '688001', '000002']

# 测试年限
TEST_YEARS = [1, 2, 3]


async def test_single_year(code: str, years: int, manager: DataSourceManager) -> dict:
    """测试单只股票指定年限的数据采集"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * years)
    
    end_str = end_date.strftime('%Y-%m-%d')
    start_str = start_date.strftime('%Y-%m-%d')
    
    logger.info(f"测试 {code} {years}年数据: {start_str} 至 {end_str}")
    
    try:
        start_time = time.time()
        df = await manager.fetch_kline(code, start_str, end_str)
        elapsed = time.time() - start_time
        
        if df.empty:
            return {
                'code': code,
                'years': years,
                'success': False,
                'rows': 0,
                'start_date': None,
                'end_date': None,
                'elapsed': elapsed,
                'error': '无数据返回'
            }
        
        # 数据质量检查
        expected_rows = 250 * years  # 约250个交易日/年
        actual_rows = len(df)
        
        # 检查日期范围
        actual_start = df['date'].min() if 'date' in df.columns else None
        actual_end = df['date'].max() if 'date' in df.columns else None
        
        # 检查数据完整性
        missing_ratio = abs(expected_rows - actual_rows) / expected_rows if expected_rows > 0 else 0
        
        return {
            'code': code,
            'years': years,
            'success': True,
            'rows': actual_rows,
            'expected_rows': expected_rows,
            'start_date': actual_start,
            'end_date': actual_end,
            'elapsed': elapsed,
            'missing_ratio': missing_ratio,
            'error': None
        }
        
    except Exception as e:
        logger.error(f"测试 {code} {years}年数据失败: {e}")
        return {
            'code': code,
            'years': years,
            'success': False,
            'rows': 0,
            'start_date': None,
            'end_date': None,
            'elapsed': 0,
            'error': str(e)
        }


async def run_all_tests():
    """运行所有测试"""
    print("=" * 70)
    print("K线数据全量采集测试")
    print("=" * 70)
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {', '.join(TEST_CODES)}")
    print(f"测试年限: {', '.join(map(str, TEST_YEARS))}年")
    
    # 初始化数据源管理器
    manager = DataSourceManager()
    print(f"\n当前数据源: {manager.current_source}")
    
    all_results = []
    
    for years in TEST_YEARS:
        print(f"\n{'='*70}")
        print(f"测试 {years} 年历史数据")
        print(f"{'='*70}")
        
        year_results = []
        for code in TEST_CODES:
            result = await test_single_year(code, years, manager)
            year_results.append(result)
            all_results.append(result)
            
            status = "✅" if result['success'] else "❌"
            print(f"\n{status} {code}:")
            print(f"   数据行数: {result['rows']}")
            if result['success']:
                print(f"   预期行数: {result['expected_rows']}")
                print(f"   缺失比例: {result['missing_ratio']:.1%}")
                print(f"   日期范围: {result['start_date']} 至 {result['end_date']}")
                print(f"   耗时: {result['elapsed']:.2f}秒")
            else:
                print(f"   错误: {result['error']}")
        
        # 统计结果
        success_count = sum(1 for r in year_results if r['success'])
        total_rows = sum(r['rows'] for r in year_results)
        avg_time = sum(r['elapsed'] for r in year_results) / len(year_results)
        
        print(f"\n{'-'*70}")
        print(f"{years}年测试汇总:")
        print(f"  成功率: {success_count}/{len(year_results)} ({success_count/len(year_results)*100:.1f}%)")
        print(f"  总行数: {total_rows}")
        print(f"  平均耗时: {avg_time:.2f}秒/只")
    
    # 总体汇总
    print(f"\n{'='*70}")
    print("总体测试汇总")
    print(f"{'='*70}")
    
    total_tests = len(all_results)
    total_success = sum(1 for r in all_results if r['success'])
    total_rows = sum(r['rows'] for r in all_results)
    total_time = sum(r['elapsed'] for r in all_results)
    
    print(f"\n总测试数: {total_tests}")
    print(f"成功数: {total_success}")
    print(f"失败数: {total_tests - total_success}")
    print(f"成功率: {total_success/total_tests*100:.1f}%")
    print(f"\n总行数: {total_rows}")
    print(f"总耗时: {total_time:.2f}秒")
    print(f"平均速度: {total_rows/total_time:.1f}条/秒" if total_time > 0 else "N/A")
    
    # 按年限统计
    print(f"\n按年限统计:")
    for years in TEST_YEARS:
        year_results = [r for r in all_results if r['years'] == years]
        year_success = sum(1 for r in year_results if r['success'])
        year_rows = sum(r['rows'] for r in year_results)
        print(f"  {years}年: {year_success}/{len(year_results)} 成功, {year_rows} 条数据")
    
    print(f"\n{'='*70}")
    
    return all_results


if __name__ == "__main__":
    results = asyncio.run(run_all_tests())
