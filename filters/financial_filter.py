#!/usr/bin/env python3
"""
财务指标选股过滤器

基于三大财务报表计算的财务指标进行选股过滤:
- 盈利能力: ROE、ROA、毛利率、净利率
- 偿债能力: 资产负债率、流动比率
- 运营能力: 存货周转率、应收账款周转率
- 成长能力: 营收增长率、净利润增长率
- 现金流: 经营现金流/净利润

使用示例:
    filter = ROEFilter(params={"min_roe": 15.0})
    filtered_stocks = filter.filter(stock_list)
"""
import polars as pl
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime

from filters.base_filter import BaseFilter, FilterRegistry
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.storage.financial_storage import FinancialStorageManager

from core.logger import get_logger

logger = get_logger(__name__)


def register_filter(name: str):
    """过滤器注册装饰器"""
    def decorator(cls):
        FilterRegistry.register(name, cls)
        return cls
    return decorator


class FinancialFilterBase(BaseFilter):
    """财务过滤器基类"""
    
    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = None,
        description: str = ""
    ):
        super().__init__(name, params, description)
        self.financial_storage = FinancialStorageManager()
        self.data_service = UnifiedDataService()
    
    def _get_financial_indicators(
        self,
        code: str,
        max_age_days: int = 120
    ) -> Optional[Dict[str, Any]]:
        """
        获取股票财务指标
        
        Args:
            code: 股票代码
            max_age_days: 最大数据年龄(天)
        
        Returns:
            财务指标字典
        """
        try:
            # 从存储加载
            indicators_df = self.financial_storage.load_indicators(code)
            
            if indicators_df.empty:
                return None
            
            # 检查数据新鲜度
            latest_date = indicators_df.iloc[0]['report_date']
            date_diff = (datetime.now() - datetime.strptime(latest_date, '%Y-%m-%d')).days
            
            if date_diff > max_age_days:
                self.logger.debug(f"{code} 财务数据过旧({date_diff}天),跳过")
                return None
            
            # 返回最新一期指标
            return indicators_df.iloc[0].to_dict()
            
        except Exception as e:
            self.logger.error(f"{code} 获取财务指标失败: {e}")
            return None
    
    def _batch_get_indicators(
        self,
        codes: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量获取财务指标
        
        Returns:
            {code: indicators_dict}
        """
        results = {}
        for code in codes:
            indicators = self._get_financial_indicators(code)
            if indicators:
                results[code] = indicators
        return results


@register_filter("roe_filter")
class ROEFilter(FinancialFilterBase):
    """
    ROE过滤器 - 筛选高ROE股票
    
    ROE(净资产收益率)是衡量公司盈利能力的核心指标
    - ROE > 15%: 优秀
    - ROE > 20%: 杰出
    """
    
    def __init__(self, name: str = "roe_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "ROE过滤器 - 筛选高ROE股票")
        self.min_roe = self.params.get("min_roe", 15.0)  # 默认15%
        self.max_roe = self.params.get("max_roe", 100.0)  # 上限100%
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤ROE不达标的股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 获取所有股票代码
        codes = stock_list['code'].to_list()
        
        # 批量获取财务指标
        indicators_map = self._batch_get_indicators(codes)
        
        # 筛选符合条件的股票
        filtered_codes = []
        for code in codes:
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            roe = indicators.get('roe')
            if roe is None:
                continue
            
            if self.min_roe <= roe <= self.max_roe:
                filtered_codes.append(code)
        
        # 过滤DataFrame
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"ROE过滤器: 移除 {removed_count} 只ROE不在[{self.min_roe}%, {self.max_roe}%]的股票")
        
        return filtered


@register_filter("profitability_filter")
class ProfitabilityFilter(FinancialFilterBase):
    """
    盈利能力综合过滤器
    
    综合评估:
    - ROE >= 阈值
    - 毛利率 >= 阈值
    - 净利率 >= 阈值
    """
    
    def __init__(self, name: str = "profitability_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "盈利能力综合过滤器")
        self.min_roe = self.params.get("min_roe", 10.0)
        self.min_gross_margin = self.params.get("min_gross_margin", 20.0)
        self.min_net_margin = self.params.get("min_net_margin", 5.0)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤盈利能力不达标的股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        indicators_map = self._batch_get_indicators(codes)
        
        filtered_codes = []
        for code in codes:
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            roe = indicators.get('roe') or 0
            gross_margin = indicators.get('gross_margin') or 0
            net_margin = indicators.get('net_margin') or 0
            
            # 所有指标都必须达标
            if (roe >= self.min_roe and 
                gross_margin >= self.min_gross_margin and 
                net_margin >= self.min_net_margin):
                filtered_codes.append(code)
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"盈利能力过滤器: 移除 {removed_count} 只股票 "
                f"(ROE>={self.min_roe}%, 毛利率>={self.min_gross_margin}%, 净利率>={self.min_net_margin}%)"
            )
        
        return filtered


@register_filter("solvency_filter")
class SolvencyFilter(FinancialFilterBase):
    """
    偿债能力过滤器
    
    筛选财务稳健的股票:
    - 资产负债率 <= 阈值
    - 流动比率 >= 阈值
    """
    
    def __init__(self, name: str = "solvency_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "偿债能力过滤器")
        self.max_debt_to_asset = self.params.get("max_debt_to_asset", 70.0)  # 资产负债率上限
        self.min_current_ratio = self.params.get("min_current_ratio", 1.0)   # 流动比率下限
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤偿债能力不足的股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        indicators_map = self._batch_get_indicators(codes)
        
        filtered_codes = []
        for code in codes:
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            debt_to_asset = indicators.get('debt_to_asset') or 0
            current_ratio = indicators.get('current_ratio') or 0
            
            # 资产负债率不能太高,流动比率不能太低
            if debt_to_asset <= self.max_debt_to_asset and current_ratio >= self.min_current_ratio:
                filtered_codes.append(code)
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"偿债能力过滤器: 移除 {removed_count} 只股票 "
                f"(资产负债率<={self.max_debt_to_asset}%, 流动比率>={self.min_current_ratio})"
            )
        
        return filtered


@register_filter("growth_filter")
class GrowthFilter(FinancialFilterBase):
    """
    成长能力过滤器
    
    筛选高成长股票:
    - 营收增长率 >= 阈值
    - 净利润增长率 >= 阈值
    """
    
    def __init__(self, name: str = "growth_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "成长能力过滤器")
        self.min_revenue_growth = self.params.get("min_revenue_growth", 10.0)
        self.min_profit_growth = self.params.get("min_profit_growth", 10.0)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤成长能力不足的股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        indicators_map = self._batch_get_indicators(codes)
        
        filtered_codes = []
        for code in codes:
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            revenue_growth = indicators.get('revenue_growth')
            profit_growth = indicators.get('profit_growth')
            
            # 允许其中一个指标不达标,但不能都为负
            growth_score = 0
            if revenue_growth and revenue_growth >= self.min_revenue_growth:
                growth_score += 1
            if profit_growth and profit_growth >= self.min_profit_growth:
                growth_score += 1
            
            # 至少满足一个增长条件
            if growth_score >= 1:
                filtered_codes.append(code)
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"成长能力过滤器: 移除 {removed_count} 只股票 "
                f"(营收增长>={self.min_revenue_growth}% 或 净利润增长>={self.min_profit_growth}%)"
            )
        
        return filtered


@register_filter("cashflow_filter")
class CashFlowFilter(FinancialFilterBase):
    """
    现金流过滤器
    
    筛选现金流健康的股票:
    - 经营现金流/净利润 >= 阈值
    """
    
    def __init__(self, name: str = "cashflow_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "现金流过滤器")
        self.min_ocf_to_profit = self.params.get("min_ocf_to_profit", 0.5)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤现金流不佳的股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        indicators_map = self._batch_get_indicators(codes)
        
        filtered_codes = []
        for code in codes:
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            ocf_to_profit = indicators.get('ocf_to_profit')
            
            # 经营现金流/净利润应大于阈值
            if ocf_to_profit is not None and ocf_to_profit >= self.min_ocf_to_profit:
                filtered_codes.append(code)
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"现金流过滤器: 移除 {removed_count} 只股票 "
                f"(经营现金流/净利润>={self.min_ocf_to_profit})"
            )
        
        return filtered


@register_filter("financial_composite_filter")
class FinancialCompositeFilter(FinancialFilterBase):
    """
    财务综合评分过滤器
    
    综合多个财务指标进行评分,筛选高分股票:
    - 盈利能力 (40%): ROE、毛利率、净利率
    - 偿债能力 (20%): 资产负债率、流动比率
    - 成长能力 (25%): 营收增长、净利润增长
    - 现金流 (15%): 经营现金流/净利润
    """
    
    def __init__(self, name: str = "financial_composite_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "财务综合评分过滤器")
        self.min_score = self.params.get("min_score", 60.0)  # 最低综合评分
        self.top_n = self.params.get("top_n", None)  # 只保留前N名
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """根据综合评分过滤股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        indicators_map = self._batch_get_indicators(codes)
        
        # 计算评分
        scored_stocks = []
        for code in codes:
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            score = self._calculate_score(indicators)
            scored_stocks.append((code, score))
        
        # 排序
        scored_stocks.sort(key=lambda x: x[1], reverse=True)
        
        # 筛选
        if self.top_n:
            filtered_codes = [code for code, score in scored_stocks[:self.top_n]]
        else:
            filtered_codes = [code for code, score in scored_stocks if score >= self.min_score]
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"财务综合过滤器: 从 {len(stock_list)} 只中筛选出 {len(filtered)} 只高分股票"
            )
        
        return filtered
    
    def _calculate_score(self, indicators: Dict[str, Any]) -> float:
        """计算财务综合评分"""
        score = 0.0
        
        # 盈利能力 (40%)
        roe = indicators.get('roe') or 0
        gross_margin = indicators.get('gross_margin') or 0
        net_margin = indicators.get('net_margin') or 0
        
        profitability_score = (
            min(roe / 20, 1.0) * 0.5 +  # ROE 20%为满分
            min(gross_margin / 40, 1.0) * 0.3 +  # 毛利率 40%为满分
            min(net_margin / 20, 1.0) * 0.2  # 净利率 20%为满分
        ) * 40
        
        # 偿债能力 (20%)
        debt_to_asset = indicators.get('debt_to_asset') or 50
        current_ratio = indicators.get('current_ratio') or 1.5
        
        solvency_score = (
            max(0, 1 - debt_to_asset / 100) * 0.6 +  # 资产负债率越低越好
            min(current_ratio / 2, 1.0) * 0.4  # 流动比率 2为满分
        ) * 20
        
        # 成长能力 (25%)
        revenue_growth = indicators.get('revenue_growth') or 0
        profit_growth = indicators.get('profit_growth') or 0
        
        growth_score = (
            min(max(revenue_growth, 0) / 30, 1.0) * 0.5 +  # 营收增长 30%为满分
            min(max(profit_growth, 0) / 30, 1.0) * 0.5  # 净利润增长 30%为满分
        ) * 25
        
        # 现金流 (15%)
        ocf_to_profit = indicators.get('ocf_to_profit') or 0
        
        cashflow_score = min(max(ocf_to_profit, 0) / 1.0, 1.0) * 15
        
        score = profitability_score + solvency_score + growth_score + cashflow_score
        return score


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 财务指标选股过滤器")
    print("=" * 60)
    
    # 创建测试数据
    test_stocks = pl.DataFrame({
        'code': ['000001', '000002', '600000', '000858', '600519'],
        'name': ['平安银行', '万科A', '浦发银行', '五粮液', '贵州茅台'],
    })
    
    print(f"\n测试股票列表: {len(test_stocks)} 只")
    print(test_stocks)
    
    # 测试ROE过滤器
    print("\n1. 测试ROE过滤器 (ROE >= 15%):")
    roe_filter = ROEFilter(params={"min_roe": 15.0})
    filtered = roe_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
    
    # 测试盈利能力过滤器
    print("\n2. 测试盈利能力过滤器:")
    prof_filter = ProfitabilityFilter(params={
        "min_roe": 10.0,
        "min_gross_margin": 30.0,
        "min_net_margin": 10.0
    })
    filtered = prof_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
    
    # 测试综合过滤器
    print("\n3. 测试财务综合评分过滤器:")
    composite_filter = FinancialCompositeFilter(params={"min_score": 50.0})
    filtered = composite_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
