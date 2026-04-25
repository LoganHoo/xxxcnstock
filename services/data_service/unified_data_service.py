#!/usr/bin/env python3
"""
统一数据服务入口

整合所有数据获取、处理、存储功能,提供统一API:
- 财务数据服务
- 市场行为数据服务
- 公告数据服务
- K线数据服务

使用示例:
    from services.data_service import UnifiedDataService
    
    service = UnifiedDataService()
    
    # 获取财务数据
    financial_data = service.get_financial_data("000001")
    
    # 获取龙虎榜
    dragon_tiger = service.get_dragon_tiger("20240419")
    
    # 获取公告
    announcements = service.get_announcements("000001")
"""
import pandas as pd
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, date
from pathlib import Path

from core.logger import setup_logger
from core.paths import get_data_path

# 财务数据模块
from services.data_service.fetchers.financial import (
    BalanceSheetFetcher,
    IncomeStatementFetcher,
    CashFlowFetcher,
)
from services.data_service.processors.financial import (
    FinancialIndicatorEngine,
    calculate_all_indicators,
)
from services.data_service.quality.financial import (
    FinancialDataValidator,
)
from services.data_service.storage.financial_storage import (
    FinancialStorageManager,
    save_financial_data,
    load_financial_data,
)

# 市场行为数据模块
from services.data_service.fetchers.market_behavior import (
    DragonTigerFetcher,
    MoneyFlowFetcher,
)

# 公告数据模块
from services.data_service.fetchers.announcement import (
    AnnouncementFetcher,
    AnnouncementType,
)

# K线数据
from services.data_service.fetchers.kline_fetcher import (
    fetch_kline_data_parallel,
    fetch_kline_for_stock
)

logger = setup_logger("unified_data_service", log_file="system/unified_data_service.log")


class UnifiedDataService:
    """统一数据服务"""
    
    def __init__(self, tushare_token: Optional[str] = None):
        self.logger = logger
        
        # 初始化获取器
        self.balance_sheet_fetcher = BalanceSheetFetcher(tushare_token)
        self.income_statement_fetcher = IncomeStatementFetcher(tushare_token)
        self.cash_flow_fetcher = CashFlowFetcher(tushare_token)
        self.dragon_tiger_fetcher = DragonTigerFetcher()
        self.money_flow_fetcher = MoneyFlowFetcher()
        self.announcement_fetcher = AnnouncementFetcher()
        
        # 初始化处理器
        self.indicator_engine = FinancialIndicatorEngine()
        self.financial_validator = FinancialDataValidator()
        
        # 初始化存储
        self.financial_storage = FinancialStorageManager()
        
        # 数据源管理器（用于获取股票列表）
        from services.data_service.datasource import get_datasource_manager
        self.datasource_manager = get_datasource_manager()
        
        self.logger.info("统一数据服务初始化完成")
    
    # ==================== 股票列表 API ====================
    
    async def get_stock_list(self) -> pd.DataFrame:
        """
        获取股票列表 (异步)
        
        Returns:
            股票列表DataFrame
        """
        return await self.datasource_manager.fetch_stock_list()
    
    def get_stock_list_sync(self) -> pd.DataFrame:
        """
        获取股票列表 (同步)
        
        Returns:
            股票列表DataFrame
        """
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.datasource_manager.fetch_stock_list())
    
    # ==================== 财务数据 API ====================
    
    def get_balance_sheet(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        获取资产负债表
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            use_cache: 是否使用缓存
        
        Returns:
            资产负债表DataFrame
        """
        if use_cache:
            cached = self.financial_storage.load_balance_sheet(code, start_date, end_date)
            if not cached.empty:
                self.logger.info(f"{code} 使用缓存的资产负债表数据")
                return cached
        
        # 从数据源获取
        df = self.balance_sheet_fetcher.fetch(code, start_date, end_date)
        
        if not df.empty and use_cache:
            self.financial_storage.save_balance_sheet(code, df)
        
        return df
    
    def get_income_statement(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """获取利润表"""
        if use_cache:
            cached = self.financial_storage.load_income_statement(code, start_date, end_date)
            if not cached.empty:
                return cached
        
        df = self.income_statement_fetcher.fetch(code, start_date, end_date)
        
        if not df.empty and use_cache:
            self.financial_storage.save_income_statement(code, df)
        
        return df
    
    def get_cash_flow(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """获取现金流量表"""
        if use_cache:
            cached = self.financial_storage.load_cash_flow(code, start_date, end_date)
            if not cached.empty:
                return cached
        
        df = self.cash_flow_fetcher.fetch(code, start_date, end_date)
        
        if not df.empty and use_cache:
            self.financial_storage.save_cash_flow(code, df)
        
        return df
    
    def get_financial_indicators(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        获取财务指标
        
        自动获取三大报表并计算指标
        """
        if use_cache:
            cached = self.financial_storage.load_indicators(code, start_date, end_date)
            if not cached.empty:
                return cached
        
        # 获取三大报表
        bs = self.get_balance_sheet(code, start_date, end_date, use_cache)
        inc = self.get_income_statement(code, start_date, end_date, use_cache)
        cf = self.get_cash_flow(code, start_date, end_date, use_cache)
        
        if bs.empty or inc.empty:
            self.logger.warning(f"{code} 缺少财务数据,无法计算指标")
            return pd.DataFrame()
        
        # 计算指标
        indicators_list = []
        for idx in range(min(len(bs), len(inc))):
            report_date = bs.iloc[idx].get('report_date', '')
            
            # 获取对应期的数据
            bs_slice = bs.iloc[idx:idx+2] if idx+1 < len(bs) else bs.iloc[idx:idx+1]
            inc_slice = inc.iloc[idx:idx+2] if idx+1 < len(inc) else inc.iloc[idx:idx+1]
            cf_slice = cf.iloc[idx:idx+1] if not cf.empty else pd.DataFrame()
            
            indicators = self.indicator_engine.calculate_all(
                bs_slice, inc_slice, cf_slice, code, report_date
            )
            indicators_list.append(indicators.__dict__)
        
        df = pd.DataFrame(indicators_list)
        
        if not df.empty and use_cache:
            self.financial_storage.save_indicators(code, df)
        
        return df
    
    def get_all_financial_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        获取所有财务数据
        
        Returns:
            {
                'balance_sheet': DataFrame,
                'income_statement': DataFrame,
                'cash_flow': DataFrame,
                'indicators': DataFrame
            }
        """
        return {
            'balance_sheet': self.get_balance_sheet(code, start_date, end_date),
            'income_statement': self.get_income_statement(code, start_date, end_date),
            'cash_flow': self.get_cash_flow(code, start_date, end_date),
            'indicators': self.get_financial_indicators(code, start_date, end_date),
        }
    
    def validate_financial_data(
        self,
        code: str,
        report_date: str
    ) -> Dict[str, Any]:
        """
        验证财务数据质量
        
        Returns:
            {
                'is_valid': bool,
                'errors': List[str],
                'warnings': List[str]
            }
        """
        # 获取数据
        bs = self.financial_storage.load_balance_sheet(code)
        inc = self.financial_storage.load_income_statement(code)
        cf = self.financial_storage.load_cash_flow(code)
        
        # 筛选指定报告期
        bs_row = bs[bs['report_date'] == report_date]
        inc_row = inc[inc['report_date'] == report_date]
        cf_row = cf[cf['report_date'] == report_date]
        
        if bs_row.empty:
            return {'is_valid': False, 'errors': ['资产负债表数据不存在'], 'warnings': []}
        
        # 执行验证
        results = []
        if not bs_row.empty:
            results.extend(self.financial_validator.validate_balance_sheet(
                bs_row, code, report_date
            ))
        if not inc_row.empty:
            results.extend(self.financial_validator.validate_income_statement(
                inc_row, code, report_date
            ))
        if not cf_row.empty:
            results.extend(self.financial_validator.validate_cash_flow(
                cf_row, code, report_date
            ))
        
        # 汇总结果
        errors = [r.message for r in results if r.level.name == 'ERROR' and not r.is_passed]
        warnings = [r.message for r in results if r.level.name == 'WARNING' and not r.is_passed]
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    # ==================== 市场行为数据 API ====================
    
    def get_dragon_tiger(
        self,
        trade_date: Optional[str] = None,
        code: Optional[str] = None
    ) -> Union[pd.DataFrame, Any]:
        """
        获取龙虎榜数据
        
        Args:
            trade_date: 交易日期 (YYYYMMDD), None表示最新
            code: 股票代码, None表示获取全部
        
        Returns:
            DataFrame或DragonTigerData对象
        """
        if code:
            # 获取单只股票详情
            if not trade_date:
                # 获取最新日期
                df = self.dragon_tiger_fetcher.fetch_daily_list()
                if not df.empty:
                    trade_date = df.iloc[0]['trade_date']
            return self.dragon_tiger_fetcher.fetch_stock_detail(code, trade_date)
        else:
            # 获取全部列表
            return self.dragon_tiger_fetcher.fetch_daily_list(trade_date)
    
    def get_dragon_tiger_history(
        self,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """获取历史龙虎榜数据"""
        return self.dragon_tiger_fetcher.fetch_date_range(start_date, end_date)
    
    def get_money_flow(
        self,
        code: str,
        market: str = "sh"
    ) -> Any:
        """获取个股资金流向"""
        return self.money_flow_fetcher.fetch_stock_money_flow(code, market)
    
    def get_money_flow_history(
        self,
        code: str,
        market: str = "sh",
        days: int = 30
    ) -> pd.DataFrame:
        """获取个股历史资金流向"""
        return self.money_flow_fetcher.fetch_stock_money_flow_hist(code, market, days)
    
    def get_sector_money_flow(self, sector_type: str = "industry") -> pd.DataFrame:
        """获取板块资金流向"""
        return self.money_flow_fetcher.fetch_sector_money_flow(sector_type)
    
    def get_northbound_money_flow(self) -> pd.DataFrame:
        """获取北向资金流向"""
        return self.money_flow_fetcher.fetch_northbound_money_flow()
    
    # ==================== 公告数据 API ====================
    
    def get_announcements(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取个股公告"""
        return self.announcement_fetcher.fetch_stock_announcements(code, start_date, end_date)
    
    def get_major_events(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取市场重大事项"""
        return self.announcement_fetcher.fetch_major_events(start_date, end_date)
    
    def get_performance_forecasts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取业绩预告"""
        return self.announcement_fetcher.fetch_performance_forecasts(start_date, end_date)
    
    def get_trading_hints(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取交易提示(停牌复牌)"""
        return self.announcement_fetcher.fetch_trading_hints(trade_date)
    
    # ==================== K线数据 API ====================
    
    def get_kline(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = "day"
    ) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            freq: 频率 (day/week/month)
        """
        from datetime import datetime
        
        # 设置默认日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - pd.Timedelta(days=365)).strftime('%Y-%m-%d')
        
        # 调用kline_fetcher函数
        result = fetch_kline_for_stock(code, start_date, end_date)
        if result is not None:
            return result
        return pd.DataFrame()
    
    # ==================== 批量操作 API ====================
    
    def batch_update_financial_data(
        self,
        codes: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Dict[str, bool]]:
        """
        批量更新财务数据
        
        Returns:
            {
                'code': {
                    'balance_sheet': True/False,
                    'income_statement': True/False,
                    'cash_flow': True/False,
                    'indicators': True/False
                }
            }
        """
        results = {}
        
        for code in codes:
            self.logger.info(f"批量更新 {code} 财务数据")
            
            code_results = {
                'balance_sheet': False,
                'income_statement': False,
                'cash_flow': False,
                'indicators': False
            }
            
            try:
                # 更新资产负债表
                bs = self.balance_sheet_fetcher.fetch(code, start_date, end_date)
                if not bs.empty:
                    code_results['balance_sheet'] = self.financial_storage.save_balance_sheet(code, bs)
                
                # 更新利润表
                inc = self.income_statement_fetcher.fetch(code, start_date, end_date)
                if not inc.empty:
                    code_results['income_statement'] = self.financial_storage.save_income_statement(code, inc)
                
                # 更新现金流量表
                cf = self.cash_flow_fetcher.fetch(code, start_date, end_date)
                if not cf.empty:
                    code_results['cash_flow'] = self.financial_storage.save_cash_flow(code, cf)
                
                # 更新指标
                if not bs.empty and not inc.empty:
                    indicators_df = self.get_financial_indicators(code, start_date, end_date, use_cache=False)
                    if not indicators_df.empty:
                        code_results['indicators'] = self.financial_storage.save_indicators(code, indicators_df)
                
            except Exception as e:
                self.logger.error(f"{code} 批量更新失败: {e}")
            
            results[code] = code_results
        
        return results
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        return self.financial_storage.get_storage_stats()


# ==================== 便捷函数 ====================

def get_data_service(tushare_token: Optional[str] = None) -> UnifiedDataService:
    """获取数据服务实例 (单例模式)"""
    return UnifiedDataService(tushare_token)


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 统一数据服务")
    print("=" * 60)
    
    service = UnifiedDataService()
    
    # 测试财务数据
    print("\n1. 测试财务数据获取:")
    indicators = service.get_financial_indicators("000001")
    if not indicators.empty:
        print(f"获取到 {len(indicators)} 期财务指标")
        print(indicators[['code', 'report_date', 'roe', 'roa', 'gross_margin']].head())
    
    # 测试龙虎榜
    print("\n2. 测试龙虎榜获取:")
    dragon_tiger = service.get_dragon_tiger()
    if not isinstance(dragon_tiger, pd.DataFrame):
        dragon_tiger = pd.DataFrame()
    if not dragon_tiger.empty:
        print(f"获取到 {len(dragon_tiger)} 条龙虎榜记录")
    
    # 测试公告
    print("\n3. 测试公告获取:")
    announcements = service.get_announcements("000001")
    if not announcements.empty:
        print(f"获取到 {len(announcements)} 条公告")
    
    # 测试存储统计
    print("\n4. 存储统计:")
    stats = service.get_storage_stats()
    print(stats)
