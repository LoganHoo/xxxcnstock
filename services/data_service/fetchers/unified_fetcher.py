#!/usr/bin/env python3
"""
统一数据获取器
使用主备数据源架构，自动故障转移
"""
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime

from core.logger import setup_logger
from services.data_service.datasource import get_datasource_manager

logger = setup_logger("unified_fetcher", log_file="system/unified_fetcher.log")


@dataclass
class StockFundamental:
    """股票基本面数据"""
    code: str
    name: str
    pe_ttm: Optional[float]
    pb: Optional[float]
    ps_ttm: Optional[float]
    pcf: Optional[float]
    total_mv: Optional[float]
    float_mv: Optional[float]
    turnover: Optional[float]
    date: str


@dataclass
class StockInfo:
    """股票信息"""
    code: str
    name: str
    industry: str
    exchange: str
    status: str


class UnifiedFetcher:
    """
    统一数据获取器
    
    特性：
    1. 自动选择可用数据源
    2. 主备故障转移
    3. 统一数据格式
    """
    
    def __init__(self):
        self.ds_manager = get_datasource_manager()
    
    async def initialize(self):
        """初始化"""
        # DataSourceManager.initialize() 不是协程，直接调用
        if hasattr(self.ds_manager, 'initialize'):
            result = self.ds_manager.initialize()
            if hasattr(result, '__await__'):
                await result
    
    async def fetch_stock_list(self) -> pd.DataFrame:
        """
        获取股票列表
        自动选择可用数据源
        """
        logger.info("获取股票列表...")
        df = await self.ds_manager.fetch_stock_list()
        
        if not df.empty:
            # 标准化列名
            required_cols = ['code', 'name', 'industry', 'exchange']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = ''
            
            logger.info(f"✅ 获取到 {len(df)} 只股票")
        else:
            logger.error("❌ 获取股票列表失败")
        
        return df
    
    async def fetch_kline(
        self,
        code: str,
        start_date: str = "2020-01-01",
        end_date: str = None
    ) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
        """
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.debug(f"获取 {code} K线数据: {start_date} ~ {end_date}")
        
        df = await self.ds_manager.fetch_kline(code, start_date, end_date)
        
        if not df.empty:
            # 标准化列名
            column_mapping = {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount'
            }
            df = df.rename(columns=column_mapping)
            
            # 确保code列存在
            if 'code' not in df.columns:
                df['code'] = code
            
            logger.debug(f"✅ {code} 获取到 {len(df)} 条K线")
        
        return df
    
    async def fetch_fundamental(self, code: str) -> Optional[StockFundamental]:
        """
        获取基本面数据
        
        Args:
            code: 股票代码
        Returns:
            StockFundamental或None
        """
        logger.debug(f"获取 {code} 基本面数据...")
        
        data = await self.ds_manager.fetch_fundamental(code)
        
        if data:
            return StockFundamental(
                code=data.get('code', code),
                name=data.get('name', ''),
                pe_ttm=data.get('pe_ttm'),
                pb=data.get('pb'),
                ps_ttm=data.get('ps_ttm'),
                pcf=data.get('pcf'),
                total_mv=data.get('total_mv'),
                float_mv=data.get('float_mv'),
                turnover=data.get('turnover'),
                date=data.get('date', datetime.now().strftime('%Y-%m-%d'))
            )
        
        return None
    
    async def fetch_all_fundamentals(self, stock_list: List[Dict]) -> List[StockFundamental]:
        """
        批量获取基本面数据
        
        Args:
            stock_list: 股票列表 [{'code': '000001', 'name': '平安银行'}, ...]
        Returns:
            StockFundamental列表
        """
        results = []
        total = len(stock_list)
        
        logger.info(f"批量获取 {total} 只股票的基本面数据...")
        
        for i, stock in enumerate(stock_list, 1):
            try:
                fundamental = await self.fetch_fundamental(stock['code'])
                if fundamental:
                    fundamental.name = stock.get('name', '')
                    results.append(fundamental)
                
                if i % 100 == 0:
                    logger.info(f"进度: {i}/{total} ({i/total*100:.1f}%)")
                    
            except Exception as e:
                logger.warning(f"{stock['code']} 处理失败: {e}")
                continue
        
        logger.info(f"批量获取完成: 成功 {len(results)}/{total}")
        return results
    
    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """
        获取实时行情
        """
        logger.info("获取实时行情...")
        df = await self.ds_manager.fetch_realtime_quotes()
        
        if not df.empty:
            logger.info(f"✅ 获取到 {len(df)} 条实时行情")
        else:
            logger.error("❌ 获取实时行情失败")
        
        return df
    
    async def batch_fetch_kline(
        self,
        stock_codes: List[str],
        start_date: str = "2020-01-01",
        end_date: str = None
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取K线数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        Returns:
            {code: DataFrame} 字典
        """
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        results = {}
        total = len(stock_codes)
        
        logger.info(f"批量获取 {total} 只股票的K线数据...")
        
        for i, code in enumerate(stock_codes, 1):
            try:
                df = await self.fetch_kline(code, start_date, end_date)
                if not df.empty:
                    results[code] = df
                
                if i % 100 == 0:
                    logger.info(f"进度: {i}/{total} ({i/total*100:.1f}%), 成功: {len(results)}")
                    
            except Exception as e:
                logger.warning(f"{code} 获取失败: {e}")
                continue
        
        logger.info(f"批量获取完成: 成功 {len(results)}/{total}")
        return results
    
    def get_datasource_status(self) -> Dict:
        """获取数据源状态"""
        return self.ds_manager.get_status()


# 便捷函数
_unified_fetcher: Optional[UnifiedFetcher] = None


async def get_unified_fetcher() -> UnifiedFetcher:
    """获取统一获取器实例"""
    global _unified_fetcher
    if _unified_fetcher is None:
        _unified_fetcher = UnifiedFetcher()
        await _unified_fetcher.initialize()
    return _unified_fetcher
