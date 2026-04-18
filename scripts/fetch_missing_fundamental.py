#!/usr/bin/env python3
"""
补充采集缺失的基本面数据
================================================================================
针对192只缺失估值数据的普通股票进行补充采集
================================================================================
"""
import sys
import time
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
import baostock as bs

from core.logger import setup_logger

logger = setup_logger(
    name="fetch_missing_fundamental",
    level="INFO",
    log_file="system/fetch_missing_fundamental.log"
)


def get_missing_stocks() -> List[Dict]:
    """获取缺失估值数据的普通股票列表"""
    # 读取数据
    stock_list = pl.read_parquet('data/stock_list.parquet')
    valuation = pl.read_parquet('data/fundamental/valuation_realistic.parquet')
    
    # 找出缺失估值数据的股票
    stock_codes = set(stock_list['code'].to_list())
    valuation_codes = set(valuation['code'].to_list())
    missing_codes = stock_codes - valuation_codes
    
    # 过滤掉ETF、基金、指数
    etf_pattern = re.compile(r'ETF|指数|货币|债券|分级|联接')
    
    missing_list = list(missing_codes)
    missing_df = stock_list.filter(pl.col('code').is_in(missing_list))
    
    # 筛选普通股票
    normal_stocks = []
    for row in missing_df.iter_rows(named=True):
        name = row['name']
        if not etf_pattern.search(name):
            normal_stocks.append({
                'code': row['code'],
                'name': name
            })
    
    return normal_stocks


def fetch_stock_valuation(code: str) -> Optional[Dict]:
    """获取单只股票的估值数据"""
    # 添加市场前缀
    if code.startswith('6'):
        code_with_prefix = f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        code_with_prefix = f"sz.{code}"
    else:
        code_with_prefix = code
    
    try:
        # 查询历史K线数据获取估值指标
        rs = bs.query_history_k_data_plus(
            code_with_prefix,
            "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM",
            start_date='2026-04-01',
            end_date='2026-04-17',
            frequency="d"
        )
        
        if rs.error_code != '0':
            logger.warning(f"{code} 查询失败: {rs.error_msg}")
            return None
        
        # 获取最新数据
        data = None
        while rs.next():
            row = rs.get_row_data()
            data = {
                'code': code,
                'pe_ttm': float(row[2]) if row[2] else None,
                'pb': float(row[3]) if row[3] else None,
                'ps_ttm': float(row[4]) if row[4] else None,
                'pcf': float(row[5]) if row[5] else None,
            }
        
        return data
        
    except Exception as e:
        logger.error(f"{code} 采集异常: {e}")
        return None


def fetch_all_missing(missing_stocks: List[Dict]) -> List[Dict]:
    """批量采集缺失的估值数据"""
    logger.info(f"开始采集 {len(missing_stocks)} 只股票的估值数据...")
    
    # 登录Baostock
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"Baostock登录失败: {lg.error_msg}")
        return []
    
    results = []
    failed_codes = []
    
    try:
        for i, stock in enumerate(missing_stocks):
            code = stock['code']
            name = stock['name']
            
            try:
                data = fetch_stock_valuation(code)
                if data:
                    results.append(data)
                    logger.info(f"✅ {code} {name}: PE={data.get('pe_ttm')}, PB={data.get('pb')}")
                else:
                    failed_codes.append(code)
                    logger.warning(f"❌ {code} {name}: 无数据")
                
                if (i + 1) % 10 == 0:
                    logger.info(f"进度: {i + 1}/{len(missing_stocks)}")
                
                # 添加延迟
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"{code} 处理失败: {e}")
                failed_codes.append(code)
    
    finally:
        bs.logout()
    
    logger.info(f"采集完成: 成功 {len(results)} 只, 失败 {len(failed_codes)} 只")
    return results


def merge_and_save(new_data: List[Dict]):
    """合并新数据并保存"""
    if not new_data:
        logger.warning("无新数据可合并")
        return
    
    # 读取现有估值数据
    existing = pl.read_parquet('data/fundamental/valuation_realistic.parquet')
    logger.info(f"现有数据: {len(existing)} 条")
    
    # 创建新数据DataFrame
    new_df = pl.DataFrame(new_data)
    logger.info(f"新数据: {len(new_df)} 条")
    
    # 合并数据
    merged = pl.concat([existing, new_df], how="diagonal")
    logger.info(f"合并后: {len(merged)} 条")
    
    # 保存
    output_file = 'data/fundamental/valuation_realistic.parquet'
    merged.write_parquet(output_file)
    logger.info(f"✅ 数据已保存: {output_file}")


def main():
    logger.info("=" * 70)
    logger.info("补充采集缺失的基本面数据")
    logger.info("=" * 70)
    
    # 获取缺失的股票列表
    missing_stocks = get_missing_stocks()
    logger.info(f"发现 {len(missing_stocks)} 只缺失估值数据的普通股票")
    
    if not missing_stocks:
        logger.info("✅ 所有普通股票已有估值数据")
        return
    
    # 显示前10只
    logger.info("缺失估值数据的股票示例:")
    for stock in missing_stocks[:10]:
        logger.info(f"  - {stock['code']} {stock['name']}")
    
    # 采集数据
    new_data = fetch_all_missing(missing_stocks)
    
    # 合并保存
    if new_data:
        merge_and_save(new_data)
        
        # 验证结果
        final_df = pl.read_parquet('data/fundamental/valuation_realistic.parquet')
        logger.info(f"\n最终数据量: {len(final_df)} 条")
        
        # 计算覆盖率
        stock_list = pl.read_parquet('data/stock_list.parquet')
        etf_pattern = re.compile(r'ETF|指数|货币|债券|分级|联接')
        non_fund = [s for s in stock_list.iter_rows(named=True) 
                   if not etf_pattern.search(s['name'])]
        non_fund_codes = {s['code'] for s in non_fund}
        
        final_codes = set(final_df['code'].to_list())
        covered = len(non_fund_codes & final_codes)
        coverage = covered / len(non_fund_codes) * 100
        
        logger.info(f"普通股票覆盖率: {coverage:.1f}%")


if __name__ == "__main__":
    main()
