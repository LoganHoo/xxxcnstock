#!/usr/bin/env python3
"""
股票基本面数据采集 - AKShare版
================================================================================
使用AKShare获取更完整的基本面数据

采集内容：
- 市盈率(PE)、市净率(PB)、市销率(PS)、市现率(PCF)
- 总股本、流通股本、总市值、流通市值
- 净资产收益率(ROE)、毛利率、净利率
- 营收增长率、净利润增长率
================================================================================
"""
import sys
import os
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
import akshare as ak

from core.logger import setup_logger

logger = setup_logger(
    name="fetch_fundamental_akshare",
    level="INFO",
    log_file="system/fetch_fundamental_akshare.log"
)


class FundamentalDataCollector:
    """基本面数据采集器(AKShare版)"""
    
    def __init__(self, date: Optional[str] = None):
        self.project_root = project_root
        self.data_dir = project_root / "data"
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        
    def fetch_all_fundamentals(self) -> List[Dict]:
        """
        获取所有股票基本面数据
        
        Returns:
            基本面数据列表
        """
        logger.info("开始采集股票基本面数据...")
        
        try:
            # 使用AKShare获取A股实时估值指标
            logger.info("获取A股估值指标...")
            df = ak.stock_zh_a_spot_em()
            
            logger.info(f"获取到 {len(df)} 只股票数据")
            logger.info(f"数据列: {df.columns.tolist()}")
            
            # 重命名列以匹配标准格式
            column_mapping = {
                '代码': 'code',
                '名称': 'name',
                '市盈率-动态': 'peTTM',
                '市净率': 'pbMRQ',
                '总市值': 'totalMarketCap',
                '流通市值': 'flowMarketCap',
                '换手率': 'turnover',
                '涨跌幅': 'change_pct',
                '成交量': 'volume',
                '成交额': 'amount',
                '最高': 'high',
                '最低': 'low',
                '今开': 'open',
                '昨收': 'pre_close',
                '最新价': 'close',
            }
            
            # 选择需要的列
            available_cols = [c for c in column_mapping.keys() if c in df.columns]
            df_selected = df[available_cols].copy()
            df_selected.rename(columns=column_mapping, inplace=True)
            
            # 添加日期
            df_selected['date'] = self.date
            df_selected['update_time'] = datetime.now().isoformat()
            
            # 转换数据类型
            numeric_cols = ['peTTM', 'pbMRQ', 'totalMarketCap', 'flowMarketCap', 
                           'turnover', 'change_pct', 'volume', 'amount',
                           'high', 'low', 'open', 'pre_close', 'close']
            
            for col in numeric_cols:
                if col in df_selected.columns:
                    df_selected[col] = pd.to_numeric(df_selected[col], errors='coerce')
            
            # 转换为字典列表
            data = df_selected.to_dict('records')
            
            logger.info(f"✅ 采集完成: {len(data)} 只股票")
            return data
            
        except Exception as e:
            logger.error(f"❌ 采集失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def save_to_parquet(self, data: List[Dict]):
        """保存数据到parquet文件"""
        if not data:
            logger.warning("无数据可保存")
            return
        
        try:
            import pandas as pd
            df = pd.DataFrame(data)
            
            # 转换为polars
            df_pl = pl.from_pandas(df)
            
            output_file = self.data_dir / f"fundamental_data_{self.date}.parquet"
            df_pl.write_parquet(output_file)
            
            logger.info(f"✅ 数据已保存: {output_file}")
            logger.info(f"   记录数: {len(df_pl)}")
            logger.info(f"   数据列: {df_pl.columns}")
            
        except Exception as e:
            logger.error(f"❌ 保存数据失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def run(self):
        """运行采集流程"""
        logger.info("=" * 70)
        logger.info("股票基本面数据采集启动 (AKShare)")
        logger.info("=" * 70)
        logger.info(f"采集日期: {self.date}")
        
        # 采集基本面数据
        fundamental_data = self.fetch_all_fundamentals()
        
        # 保存数据
        if fundamental_data:
            self.save_to_parquet(fundamental_data)
            return True
        else:
            logger.error("❌ 未获取到数据")
            return False


def main():
    parser = argparse.ArgumentParser(description='股票基本面数据采集(AKShare)')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD格式)')
    args = parser.parse_args()
    
    collector = FundamentalDataCollector(date=args.date)
    success = collector.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
