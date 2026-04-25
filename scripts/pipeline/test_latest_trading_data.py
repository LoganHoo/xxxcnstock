#!/usr/bin/env python3
"""
测试最新交易数据脚本

功能：
1. 验证数据源连接
2. 获取最新交易日数据
3. 对比本地数据与数据源
4. 生成测试报告

使用方式:
    python scripts/pipeline/test_latest_trading_data.py
    python scripts/pipeline/test_latest_trading_data.py --code 000001
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_latest_data")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import pandas as pd
import asyncio


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='测试最新交易数据')
    parser.add_argument(
        '--code',
        type=str,
        default='000001',
        help='测试股票代码（默认000001）'
    )
    parser.add_argument(
        '--source',
        type=str,
        default='baostock',
        choices=['baostock', 'tencent', 'sina'],
        help='数据源（默认baostock）'
    )
    return parser.parse_args()


async def test_datasource_connection(source: str) -> Dict:
    """测试数据源连接"""
    logger.info("=" * 60)
    logger.info(f"🔌 测试数据源连接: {source}")
    logger.info("=" * 60)
    
    try:
        if source == 'baostock':
            import baostock as bs
            lg = bs.login()
            if lg.error_code == '0':
                logger.info(f"✅ Baostock 连接成功")
                bs.logout()
                return {'success': True, 'message': '连接成功'}
            else:
                return {'success': False, 'message': f"登录失败: {lg.error_msg}"}
                
        elif source == 'tencent':
            # 腾讯数据源通过HTTP接口，无需登录
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            if not df.empty:
                logger.info(f"✅ 腾讯数据源连接成功")
                return {'success': True, 'message': '连接成功'}
            else:
                return {'success': False, 'message': '获取数据失败'}
                
        elif source == 'sina':
            import akshare as ak
            df = ak.stock_zh_a_spot()
            if not df.empty:
                logger.info(f"✅ 新浪数据源连接成功")
                return {'success': True, 'message': '连接成功'}
            else:
                return {'success': False, 'message': '获取数据失败'}
        
    except Exception as e:
        logger.error(f"❌ 数据源连接失败: {e}")
        return {'success': False, 'message': str(e)}


async def fetch_latest_from_source(code: str, source: str) -> Optional[pd.DataFrame]:
    """从数据源获取最新数据"""
    logger.info(f"\n📥 从 {source} 获取 {code} 的最新数据...")
    
    try:
        if source == 'baostock':
            import baostock as bs
            bs.login()
            
            # 获取最近5个交易日数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
            
            rs = bs.query_history_k_data_plus(
                f"sz.{code}" if code.startswith('0') or code.startswith('3') else f"sh.{code}",
                "date,code,open,high,low,close,volume",
                start_date=start_date,
                end_date=end_date,
                frequency="d"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            bs.logout()
            
            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                logger.info(f"✅ 获取到 {len(df)} 条数据")
                return df
            else:
                logger.warning("⚠️ 未获取到数据")
                return None
                
        elif source in ['tencent', 'sina']:
            import akshare as ak
            
            # 使用akshare获取历史数据
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=(datetime.now() - timedelta(days=10)).strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d'),
                adjust=""
            )
            
            if not df.empty:
                logger.info(f"✅ 获取到 {len(df)} 条数据")
                return df
            else:
                logger.warning("⚠️ 未获取到数据")
                return None
        
    except Exception as e:
        logger.error(f"❌ 获取数据失败: {e}")
        return None


def read_local_data(code: str) -> Optional[pl.DataFrame]:
    """读取本地数据"""
    logger.info(f"\n📂 读取本地数据: {code}")
    
    kline_file = Path(f"data/kline/{code}.parquet")
    if not kline_file.exists():
        logger.warning(f"⚠️ 本地文件不存在: {kline_file}")
        return None
    
    try:
        df = pl.read_parquet(kline_file)
        logger.info(f"✅ 本地数据: {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"❌ 读取本地数据失败: {e}")
        return None


def compare_data(source_df: pd.DataFrame, local_df: pl.DataFrame) -> Dict:
    """对比数据源和本地数据"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 数据对比")
    logger.info("=" * 60)
    
    # 获取最新日期
    source_latest = source_df['date'].iloc[-1] if 'date' in source_df.columns else source_df.iloc[-1, 0]
    local_latest = str(local_df['date'].max()) if 'date' in local_df.columns else str(local_df['trade_date'].max())
    
    logger.info(f"数据源最新日期: {source_latest}")
    logger.info(f"本地数据最新日期: {local_latest}")
    
    # 检查是否同步
    is_synced = str(source_latest) == str(local_latest)
    
    if is_synced:
        logger.info("✅ 数据已同步")
    else:
        logger.warning("⚠️ 数据不同步")
    
    return {
        'source_latest': str(source_latest),
        'local_latest': str(local_latest),
        'is_synced': is_synced
    }


async def main():
    """主函数"""
    args = parse_args()
    
    logger.info("=" * 60)
    logger.info("🧪 最新交易数据测试")
    logger.info("=" * 60)
    logger.info(f"测试股票: {args.code}")
    logger.info(f"数据源: {args.source}")
    
    # 1. 测试数据源连接
    connection_result = await test_datasource_connection(args.source)
    if not connection_result['success']:
        logger.error(f"❌ 数据源连接失败: {connection_result['message']}")
        return 1
    
    # 2. 从数据源获取数据
    source_df = await fetch_latest_from_source(args.code, args.source)
    if source_df is None:
        logger.error("❌ 无法从数据源获取数据")
        return 1
    
    # 显示最新数据
    logger.info("\n📈 数据源最新数据:")
    print(source_df.tail(3).to_string())
    
    # 3. 读取本地数据
    local_df = read_local_data(args.code)
    if local_df is not None:
        logger.info("\n📈 本地最新数据:")
        print(local_df.tail(3).to_pandas().to_string())
        
        # 4. 对比数据
        comparison = compare_data(source_df, local_df)
        
        # 5. 生成报告
        logger.info("\n" + "=" * 60)
        logger.info("📋 测试报告")
        logger.info("=" * 60)
        logger.info(f"数据源: {args.source}")
        logger.info(f"测试股票: {args.code}")
        logger.info(f"数据源最新: {comparison['source_latest']}")
        logger.info(f"本地最新: {comparison['local_latest']}")
        logger.info(f"数据同步: {'✅ 是' if comparison['is_synced'] else '⚠️ 否'}")
        
        if comparison['is_synced']:
            logger.info("\n✅ 测试通过：数据已同步")
            return 0
        else:
            logger.warning("\n⚠️ 测试警告：数据不同步")
            return 1
    else:
        logger.info("\n⚠️ 本地数据不存在，建议执行数据采集")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
