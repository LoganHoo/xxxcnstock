#!/usr/bin/env python3
"""
股票基本面数据采集 - Baostock版
================================================================================
采集内容：
- 市盈率(PE)、市净率(PB)、市销率(PS)、市现率(PCF)
- 总股本、流通股本
- 总市值、流通市值
- 每股收益、每股净资产
- 净资产收益率(ROE)、毛利率、净利率
- 营收增长率、净利润增长率

使用方法：
    python scripts/fetch_fundamental_data.py [--date YYYY-MM-DD]
================================================================================
"""
import sys
import os
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
import baostock as bs

from core.logger import setup_logger

logger = setup_logger(
    name="fetch_fundamental",
    level="INFO",
    log_file="system/fetch_fundamental.log"
)

# 重试装饰器
def retry_on_error(max_retries=3, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"{func.__name__} 失败，{delay}s后重试 ({attempt+1}/{max_retries}): {e}")
                        time.sleep(delay * (attempt + 1))
                    else:
                        logger.error(f"{func.__name__} 最终失败: {e}")
                        raise
            return None
        return wrapper
    return decorator


class FundamentalDataCollector:
    """基本面数据采集器"""
    
    def __init__(self, date: Optional[str] = None):
        self.project_root = project_root
        self.data_dir = project_root / "data"
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        self.lg = None
        
    def login(self) -> bool:
        """登录Baostock"""
        try:
            self.lg = bs.login()
            if self.lg.error_code == '0':
                logger.info("✅ Baostock登录成功")
                return True
            else:
                logger.error(f"❌ Baostock登录失败: {self.lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"❌ Baostock登录异常: {e}")
            return False
    
    def logout(self):
        """登出Baostock"""
        try:
            bs.logout()
            logger.info("Baostock已登出")
        except:
            pass
    
    @retry_on_error(max_retries=3, delay=1)
    def fetch_stock_fundamental(self, code: str) -> Optional[Dict]:
        """
        获取单只股票基本面数据
        
        Args:
            code: 股票代码 (如 '000001')
            
        Returns:
            基本面数据字典
        """
        # 添加市场前缀
        if code.startswith('6'):
            code_with_prefix = f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            code_with_prefix = f"sz.{code}"
        else:
            code_with_prefix = code
        
        # 查询证券基本资料
        rs = bs.query_stock_basic(code=code_with_prefix)
        if rs.error_code != '0':
            logger.warning(f"{code} 基本资料查询失败: {rs.error_msg}")
            return None
        
        # 查询季频估值指标
        rs_pe = bs.query_history_k_data_plus(
            code_with_prefix,
            "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM",
            start_date=self.date,
            end_date=self.date,
            frequency="d"
        )
        
        # 查询季频盈利能力
        rs_profit = bs.query_profit_data(code=code_with_prefix, year=int(self.date[:4]), quarter=1)
        
        # 查询季频营运能力
        rs_operation = bs.query_operation_data(code=code_with_prefix, year=int(self.date[:4]), quarter=1)
        
        # 查询季频成长能力
        rs_growth = bs.query_growth_data(code=code_with_prefix, year=int(self.date[:4]), quarter=1)
        
        # 查询季频偿债能力
        rs_balance = bs.query_balance_data(code=code_with_prefix, year=int(self.date[:4]), quarter=1)
        
        # 查询季频现金流量
        rs_cash_flow = bs.query_cash_flow_data(code=code_with_prefix, year=int(self.date[:4]), quarter=1)
        
        # 查询季频杜邦分析
        rs_dupont = bs.query_dupont_data(code=code_with_prefix, year=int(self.date[:4]), quarter=1)
        
        # 整合数据
        data = {
            'code': code,
            'date': self.date,
            'update_time': datetime.now().isoformat()
        }
        
        # 估值指标
        if rs_pe.error_code == '0' and rs_pe.next():
            row = rs_pe.get_row_data()
            data['peTTM'] = float(row[2]) if row[2] else None
            data['pbMRQ'] = float(row[3]) if row[3] else None
            data['psTTM'] = float(row[4]) if row[4] else None
            data['pcfNcfTTM'] = float(row[5]) if row[5] else None
        
        # 盈利能力
        if rs_profit.error_code == '0' and rs_profit.next():
            row = rs_profit.get_row_data()
            data['roeAvg'] = float(row[3]) if len(row) > 3 and row[3] else None  # 净资产收益率
            data['npMargin'] = float(row[4]) if len(row) > 4 and row[4] else None  # 净利率
            data['gpMargin'] = float(row[5]) if len(row) > 5 and row[5] else None  # 毛利率
        
        # 成长能力
        if rs_growth.error_code == '0' and rs_growth.next():
            row = rs_growth.get_row_data()
            data['yoyEquity'] = float(row[3]) if len(row) > 3 and row[3] else None  # 净资产同比增长率
            data['yoyAsset'] = float(row[4]) if len(row) > 4 and row[4] else None  # 总资产同比增长率
            data['yoyNpp'] = float(row[7]) if len(row) > 7 and row[7] else None  # 净利润同比增长率
            data['yoySale'] = float(row[10]) if len(row) > 10 and row[10] else None  # 营收同比增长率
        
        # 偿债能力
        if rs_balance.error_code == '0' and rs_balance.next():
            row = rs_balance.get_row_data()
            data['currentRatio'] = float(row[3]) if len(row) > 3 and row[3] else None  # 流动比率
            data['quickRatio'] = float(row[4]) if len(row) > 4 and row[4] else None  # 速动比率
            data['cashRatio'] = float(row[5]) if len(row) > 5 and row[5] else None  # 现金比率
            data['yoyLiability'] = float(row[10]) if len(row) > 10 and row[10] else None  # 总负债同比增长率
        
        # 股本数据（从K线数据中获取）
        try:
            kline_file = self.data_dir / "kline" / f"{code}.parquet"
            if kline_file.exists():
                kline_df = pl.read_parquet(kline_file)
                if len(kline_df) > 0:
                    latest = kline_df.filter(pl.col('trade_date') == self.date)
                    if len(latest) > 0:
                        # 计算市值（需要股价和股本数据）
                        close = latest['close'].to_list()[0]
                        # 从证券基本资料获取股本
                        rs_basic = bs.query_stock_basic(code=code_with_prefix)
                        if rs_basic.error_code == '0' and rs_basic.next():
                            basic_row = rs_basic.get_row_data()
                            # 这里需要解析股本数据
        except:
            pass
        
        return data
    
    def fetch_all_fundamentals(self, stock_codes: List[str], max_workers: int = 4) -> List[Dict]:
        """
        批量获取所有股票基本面数据
        
        Args:
            stock_codes: 股票代码列表
            max_workers: 并发数
            
        Returns:
            基本面数据列表
        """
        logger.info(f"开始采集 {len(stock_codes)} 只股票的基本面数据...")
        
        results = []
        failed_codes = []
        
        # 使用单线程逐个获取（Baostock可能不支持并发）
        for i, code in enumerate(stock_codes):
            try:
                data = self.fetch_stock_fundamental(code)
                if data:
                    results.append(data)
                else:
                    failed_codes.append(code)
                
                if (i + 1) % 100 == 0:
                    logger.info(f"  已采集 {i + 1}/{len(stock_codes)} 只...")
                
                # 添加延迟避免请求过快
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"{code} 采集失败: {e}")
                failed_codes.append(code)
        
        logger.info(f"✅ 采集完成: 成功 {len(results)} 只, 失败 {len(failed_codes)} 只")
        if failed_codes:
            logger.warning(f"失败股票: {failed_codes[:10]}...")
        
        return results
    
    def save_to_parquet(self, data: List[Dict]):
        """保存数据到parquet文件"""
        if not data:
            logger.warning("无数据可保存")
            return
        
        try:
            df = pl.DataFrame(data)
            output_file = self.data_dir / f"fundamental_data_{self.date}.parquet"
            df.write_parquet(output_file)
            logger.info(f"✅ 数据已保存: {output_file}")
            logger.info(f"   记录数: {len(df)}")
            logger.info(f"   数据列: {df.columns}")
        except Exception as e:
            logger.error(f"❌ 保存数据失败: {e}")
    
    def run(self):
        """运行采集流程"""
        logger.info("=" * 70)
        logger.info("股票基本面数据采集启动")
        logger.info("=" * 70)
        logger.info(f"采集日期: {self.date}")
        
        # 登录Baostock
        if not self.login():
            return False
        
        try:
            # 读取股票列表
            stock_list_file = self.data_dir / "stock_list.parquet"
            if not stock_list_file.exists():
                logger.error(f"❌ 股票列表不存在: {stock_list_file}")
                return False
            
            stock_df = pl.read_parquet(stock_list_file)
            stock_codes = stock_df['code'].to_list()
            
            logger.info(f"股票列表: {len(stock_codes)} 只")
            
            # 采集基本面数据
            fundamental_data = self.fetch_all_fundamentals(stock_codes)
            
            # 保存数据
            self.save_to_parquet(fundamental_data)
            
            return len(fundamental_data) > 0
            
        finally:
            self.logout()


def main():
    parser = argparse.ArgumentParser(description='股票基本面数据采集')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD格式)')
    args = parser.parse_args()
    
    collector = FundamentalDataCollector(date=args.date)
    success = collector.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
