#!/usr/bin/env python3
"""
股票列表获取器
使用Baostock获取A股所有股票列表
"""
import importlib
import pandas as pd
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from core.logger import setup_logger

logger = setup_logger("stock_list_fetcher", log_file="system/stock_list.log")


def _get_baostock_client():
    """延迟加载baostock"""
    return importlib.import_module("baostock")


@dataclass
class StockInfo:
    """股票信息"""
    code: str
    name: str
    industry: str
    exchange: str  # sh/sz
    status: str    # 1-正常, 2-停牌, 3-退市


class StockListFetcher:
    """股票列表获取器"""
    
    def __init__(self):
        self._bs = None
        self._logged_in = False
    
    def _login(self) -> bool:
        """登录Baostock"""
        if self._logged_in:
            return True
        
        try:
            bs = _get_baostock_client()
            lg = bs.login()
            if lg.error_code == '0':
                self._bs = bs
                self._logged_in = True
                logger.info("Baostock登录成功")
                return True
            else:
                logger.error(f"Baostock登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"Baostock登录异常: {e}")
            return False
    
    def _logout(self):
        """登出Baostock"""
        if self._logged_in and self._bs:
            try:
                self._bs.logout()
                logger.info("Baostock登出成功")
            except:
                pass
            finally:
                self._logged_in = False
                self._bs = None
    
    async def fetch_all_stocks(self) -> List[StockInfo]:
        """
        获取所有A股股票列表
        
        Returns:
            StockInfo列表
        """
        if not self._login():
            return []
        
        try:
            logger.info("开始获取股票列表...")
            
            # 自动查找最近的有效交易日（最多回溯10天）
            stocks = []
            rs_sh = None
            query_date = None
            max_backtrack_days = 10
            
            for days_ago in range(max_backtrack_days + 1):
                test_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
                
                rs_test = self._bs.query_all_stock(day=test_date)
                
                # 只检查第一条记录，避免遍历整个结果集
                if rs_test.next():
                    query_date = test_date
                    rs_sh = rs_test  # 直接使用已打开的结果集
                    first_row = rs_test.get_row_data()
                    logger.info(f"找到有效交易日: {test_date} (示例: {first_row[0]} - {first_row[1]})")
                    break
                else:
                    if days_ago < 3:
                        logger.debug(f"日期 {test_date} 无数据，继续回溯...")
            
            if not query_date or not rs_sh:
                logger.error(f"最近{max_backtrack_days}天均无有效交易数据")
                return []
            
            # 解析股票数据（继续使用已打开的结果集）
            # 先添加第一条记录（已经在next()中读取了）
            row = rs_sh.get_row_data()
            code = row[0].split('.')[-1]
            stocks.append(StockInfo(
                code=code,
                name=row[1],
                industry=row[2] if len(row) > 2 else '',
                exchange='sh',
                status=row[3] if len(row) > 3 else '1'
            ))
            
            # 继续读取剩余记录
            while rs_sh.next():
                row = rs_sh.get_row_data()
                code = row[0].split('.')[-1]
                stocks.append(StockInfo(
                    code=code,
                    name=row[1],
                    industry=row[2] if len(row) > 2 else '',
                    exchange='sh',
                    status=row[3] if len(row) > 3 else '1'
                ))
            
            logger.info(f"获取到 {len(stocks)} 只股票 (数据日期: {query_date})")
            return stocks
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        finally:
            self._logout()
    
    async def fetch_to_parquet(self, output_path: str) -> bool:
        """
        获取股票列表并保存为parquet
        
        Args:
            output_path: 输出文件路径
        Returns:
            是否成功
        """
        try:
            stocks = await self.fetch_all_stocks()
            if not stocks:
                return False
            
            # 转换为DataFrame
            df = pd.DataFrame([s.__dict__ for s in stocks])
            
            # 保存
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)
            
            logger.info(f"股票列表已保存: {output_path}, 共 {len(df)} 只")
            return True
            
        except Exception as e:
            logger.error(f"保存股票列表失败: {e}")
            return False
    
    async def update_stock_list(self, data_dir: str = "data") -> bool:
        """
        更新股票列表（标准路径）
        
        Args:
            data_dir: 数据目录
        Returns:
            是否成功
        """
        from core.config import get_settings
        
        settings = get_settings()
        output_path = Path(settings.DATA_DIR) / "stock_list.parquet"
        
        return await self.fetch_to_parquet(str(output_path))
