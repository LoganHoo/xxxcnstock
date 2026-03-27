import akshare as ak
import pandas as pd
from typing import List, Optional
from datetime import datetime
import logging

from core.models import StockQuote
from core.logger import setup_logger

logger = setup_logger("quote_fetcher", log_file="system/quote.log")


class QuoteFetcher:
    """行情数据获取器"""
    
    def __init__(self):
        self._cache = {}
    
    async def fetch_realtime_quotes(self) -> List[StockQuote]:
        """
        获取A股实时行情
        使用 akshare.stock_zh_a_spot_em 接口
        """
        try:
            logger.info("开始获取实时行情数据")
            df = ak.stock_zh_a_spot_em()
            
            if df is None or df.empty:
                logger.warning("实时行情数据为空")
                return []
            
            quotes = []
            for _, row in df.iterrows():
                try:
                    quote = StockQuote(
                        code=str(row.get("代码", "")),
                        name=str(row.get("名称", "")),
                        price=float(row.get("最新价", 0) or 0),
                        change_pct=float(row.get("涨跌幅", 0) or 0),
                        volume=int(row.get("成交量", 0) or 0),
                        turnover_rate=float(row.get("换手率", 0) or 0),
                        amount=float(row.get("成交额", 0) or 0),
                        high=float(row.get("最高", 0) or 0),
                        low=float(row.get("最低", 0) or 0),
                        open=float(row.get("今开", 0) or 0),
                        pre_close=float(row.get("昨收", 0) or 0),
                    )
                    quotes.append(quote)
                except Exception as e:
                    logger.warning(f"解析行情数据失败: {row.get('代码')}, {e}")
                    continue
            
            logger.info(f"获取实时行情完成，共 {len(quotes)} 条")
            return quotes
            
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            return []
    
    async def fetch_kline(
        self, 
        code: str, 
        period: str = "daily",
        start_date: str = None,
        end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            period: 周期 daily/weekly/monthly
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        try:
            logger.info(f"获取K线数据: {code}")
            
            if not start_date:
                start_date = "20200101"
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            if df is not None and not df.empty:
                logger.info(f"获取K线数据成功: {code}, {len(df)} 条")
            
            return df
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {code}, {e}")
            return None
    
    async def fetch_quote_by_code(self, code: str) -> Optional[StockQuote]:
        """获取单只股票行情"""
        quotes = await self.fetch_realtime_quotes()
        for quote in quotes:
            if quote.code == code:
                return quote
        return None
