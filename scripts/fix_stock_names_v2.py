#!/usr/bin/env python3
"""
修复股票名称数据 - 使用 stock_list_enhanced.parquet
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import polars as pl
from pathlib import Path
from core.logger import setup_logger

logger = setup_logger("fix_stock_names")

def fix_stock_list_names():
    """修复股票列表中的名称"""
    data_dir = Path('data')
    stock_list_path = data_dir / 'stock_list.parquet'
    enhanced_path = data_dir / 'stock_list_enhanced.parquet'
    
    # 读取股票列表
    stock_list = pl.read_parquet(stock_list_path)
    logger.info(f"股票列表: {len(stock_list)} 只")
    
    # 读取增强版股票列表
    enhanced = pl.read_parquet(enhanced_path)
    logger.info(f"增强版股票列表: {len(enhanced)} 只")
    
    # 创建代码到名称的映射
    name_map = {}
    for row in enhanced.iter_rows(named=True):
        name_map[row['code']] = row.get('name', '')
    
    logger.info(f"名称映射: {len(name_map)} 个")
    
    # 更新股票列表
    def get_name(code):
        return name_map.get(code, '')
    
    stock_list = stock_list.with_columns([
        pl.col('code').map_elements(get_name, return_dtype=pl.Utf8).alias('name')
    ])
    
    # 保存
    stock_list.write_parquet(stock_list_path)
    
    # 验证
    empty_after = stock_list.filter(pl.col('name') == '').shape[0]
    logger.info(f"修复后空名称数量: {empty_after}")
    
    # 显示样本
    logger.info("样本数据:")
    for row in stock_list.head(5).to_dicts():
        logger.info(f"  {row['code']}: {row['name']}")


def fix_scores_names():
    """修复评分数据中的名称"""
    data_dir = Path('data')
    scores_path = data_dir / 'enhanced_scores_full.parquet'
    enhanced_path = data_dir / 'stock_list_enhanced.parquet'
    
    if not scores_path.exists():
        logger.warning("评分数据不存在")
        return
    
    # 读取数据
    scores = pl.read_parquet(scores_path)
    enhanced = pl.read_parquet(enhanced_path)
    
    logger.info(f"评分数据: {len(scores)} 条")
    
    # 创建代码到名称的映射
    name_map = {}
    for row in enhanced.iter_rows(named=True):
        name_map[row['code']] = row.get('name', '')
    
    # 更新评分数据
    def get_name(code):
        return name_map.get(code, '')
    
    scores = scores.with_columns([
        pl.col('code').map_elements(get_name, return_dtype=pl.Utf8).alias('name')
    ])
    
    # 保存
    scores.write_parquet(scores_path)
    
    # 验证
    empty_after = scores.filter(pl.col('name') == '').shape[0]
    logger.info(f"修复后空名称数量: {empty_after}")
    
    # 显示样本
    logger.info("样本数据:")
    for row in scores.head(5).to_dicts():
        logger.info(f"  {row['code']}: {row['name']} (评分: {row['enhanced_score']})")


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("开始修复股票名称")
    logger.info("=" * 60)
    
    fix_stock_list_names()
    fix_scores_names()
    
    logger.info("=" * 60)
    logger.info("修复完成")
    logger.info("=" * 60)
