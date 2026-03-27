import akshare as ak
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime
from dataclasses import dataclass
import logging

from core.logger import setup_logger

logger = setup_logger("limitup_fetcher", log_file="system/limitup.log")


@dataclass
class LimitUpStock:
    """涨停股票数据"""
    code: str
    name: str
    change_pct: float
    limit_time: str
    seal_amount: float  # 封单金额(万)
    open_count: int  # 开板次数
    continuous_limit: int  # 连板数
    sector: str = ""


class LimitUpFetcher:
    """涨停数据获取器"""
    
    async def fetch_limit_up_pool(self, date: str = None) -> List[LimitUpStock]:
        """
        获取涨停池数据
        
        Args:
            date: 日期 YYYYMMDD，默认今天
        """
        try:
            if not date:
                date = datetime.now().strftime("%Y%m%d")
            
            logger.info(f"获取涨停池数据: {date}")
            df = ak.stock_zt_pool_em(date=date)
            
            if df is None or df.empty:
                logger.warning(f"涨停池数据为空: {date}")
                return []
            
            stocks = []
            for _, row in df.iterrows():
                try:
                    stock = LimitUpStock(
                        code=str(row.get("代码", "")),
                        name=str(row.get("名称", "")),
                        change_pct=float(row.get("涨跌幅", 0) or 0),
                        limit_time=str(row.get("涨停时间", "")),
                        seal_amount=float(row.get("封单资金", 0) or 0) / 10000,  # 转万
                        open_count=int(row.get("开板次数", 0) or 0),
                        continuous_limit=int(row.get("连板数", 1) or 1),
                        sector=str(row.get("所属行业", ""))
                    )
                    stocks.append(stock)
                except Exception as e:
                    logger.warning(f"解析涨停数据失败: {row.get('代码')}, {e}")
                    continue
            
            logger.info(f"获取涨停池完成: {len(stocks)} 只")
            return stocks
            
        except Exception as e:
            logger.error(f"获取涨停池失败: {e}")
            return []
    
    async def fetch_limit_up_strong(self, date: str = None) -> List[LimitUpStock]:
        """获取强势涨停股（首板封板早、封单大）"""
        stocks = await self.fetch_limit_up_pool(date)
        
        strong_stocks = [
            s for s in stocks
            if s.open_count == 0 
            and s.limit_time < "10:00:00"
            and s.seal_amount > 5000  # 封单>5000万
        ]
        
        logger.info(f"强势涨停股: {len(strong_stocks)} 只")
        return strong_stocks
    
    async def fetch_continuous_limit_up(self, min_boards: int = 2, date: str = None) -> List[LimitUpStock]:
        """获取连板股"""
        stocks = await self.fetch_limit_up_pool(date)
        
        continuous = [s for s in stocks if s.continuous_limit >= min_boards]
        logger.info(f"{min_boards}连板以上: {len(continuous)} 只")
        return continuous
