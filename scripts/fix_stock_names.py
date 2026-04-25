#!/usr/bin/env python3
"""
修复股票名称数据
从K线数据或akshare获取股票名称
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
    
    # 读取股票列表
    stock_list = pl.read_parquet(stock_list_path)
    logger.info(f"股票列表: {len(stock_list)} 只")
    
    # 检查当前名称情况
    empty_count = stock_list.filter(pl.col('name') == '').shape[0]
    logger.info(f"空名称数量: {empty_count}")
    
    if empty_count == 0:
        logger.info("名称数据完整，无需修复")
        return
    
    # 尝试从akshare获取名称
    try:
        import akshare as ak
        logger.info("从akshare获取股票名称...")
        spot_df = ak.stock_zh_a_spot_em()
        
        # 创建代码到名称的映射
        name_map = {}
        for _, row in spot_df.iterrows():
            code = row['代码']
            name = row['名称']
            name_map[code] = name
        
        logger.info(f"从akshare获取到 {len(name_map)} 个名称")
        
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
        
    except Exception as e:
        logger.error(f"从akshare获取失败: {e}")
        logger.info("尝试从本地CSV获取...")
        
        # 尝试从stock_list.csv获取
        csv_path = data_dir / 'stock_list.csv'
        if csv_path.exists():
            csv_df = pl.read_csv(csv_path)
            if 'name' in csv_df.columns:
                # 合并名称
                stock_list = stock_list.drop('name').join(
                    csv_df.select(['code', 'name']),
                    on='code',
                    how='left'
                )
                stock_list.write_parquet(stock_list_path)
                
                empty_after = stock_list.filter(pl.col('name') == '').shape[0]
                logger.info(f"从CSV修复后空名称数量: {empty_after}")


def fix_scores_names():
    """修复评分数据中的名称"""
    data_dir = Path('data')
    scores_path = data_dir / 'enhanced_scores_full.parquet'
    stock_list_path = data_dir / 'stock_list.parquet'
    
    if not scores_path.exists():
        logger.warning("评分数据不存在")
        return
    
    # 读取数据
    scores = pl.read_parquet(scores_path)
    stock_list = pl.read_parquet(stock_list_path)
    
    logger.info(f"评分数据: {len(scores)} 条")
    logger.info(f"股票列表: {len(stock_list)} 条")
    
    # 检查当前名称情况
    empty_count = scores.filter(pl.col('name') == '').shape[0]
    logger.info(f"评分数据空名称数量: {empty_count}")
    
    if empty_count == 0:
        logger.info("评分数据名称完整，无需修复")
        return
    
    # 从股票列表获取名称
    name_map = {}
    for row in stock_list.iter_rows(named=True):
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


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("开始修复股票名称")
    logger.info("=" * 60)
    
    fix_stock_list_names()
    fix_scores_names()
    
    logger.info("=" * 60)
    logger.info("修复完成")
    logger.info("=" * 60)
