#!/usr/bin/env python3
"""
财务指标计算引擎

基于三大财务报表计算各类财务指标:
1. 盈利能力指标: ROE, ROA, 毛利率, 净利率
2. 偿债能力指标: 流动比率, 速动比率, 资产负债率
3. 运营能力指标: 存货周转率, 应收账款周转率, 总资产周转率
4. 成长能力指标: 营收增长率, 净利润增长率
5. 现金流指标: 经营现金流/净利润, 自由现金流

支持:
- 单季度指标计算
- TTM(滚动12个月)指标计算
- 同比增长率计算
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime
from functools import wraps

from core.logger import setup_logger

logger = setup_logger("indicator_engine", log_file="system/indicator_engine.log")


@dataclass
class FinancialIndicators:
    """财务指标数据类"""
    # 基本信息
    code: str
    report_date: str
    report_type: str
    
    # 盈利能力
    roe: Optional[float] = None                 # 净资产收益率 (%)
    roe_diluted: Optional[float] = None         # 摊薄ROE (%)
    roa: Optional[float] = None                 # 总资产收益率 (%)
    gross_margin: Optional[float] = None        # 销售毛利率 (%)
    net_margin: Optional[float] = None          # 销售净利率 (%)
    operating_margin: Optional[float] = None    # 营业利润率 (%)
    
    # 偿债能力
    current_ratio: Optional[float] = None       # 流动比率
    quick_ratio: Optional[float] = None         # 速动比率
    cash_ratio: Optional[float] = None          # 现金比率
    debt_to_asset: Optional[float] = None       # 资产负债率 (%)
    debt_to_equity: Optional[float] = None      # 产权比率 (%)
    equity_ratio: Optional[float] = None        # 权益乘数
    interest_coverage: Optional[float] = None   # 利息保障倍数
    
    # 运营能力
    inventory_turnover: Optional[float] = None      # 存货周转率
    receivable_turnover: Optional[float] = None     # 应收账款周转率
    total_asset_turnover: Optional[float] = None    # 总资产周转率
    current_asset_turnover: Optional[float] = None  # 流动资产周转率
    
    # 成长能力
    revenue_growth: Optional[float] = None      # 营业收入增长率 (%)
    profit_growth: Optional[float] = None       # 净利润增长率 (%)
    net_profit_growth: Optional[float] = None   # 归母净利润增长率 (%)
    asset_growth: Optional[float] = None        # 总资产增长率 (%)
    equity_growth: Optional[float] = None       # 净资产增长率 (%)
    
    # 现金流指标
    ocf_to_profit: Optional[float] = None       # 经营现金流/净利润
    ocf_to_revenue: Optional[float] = None      # 经营现金流/营业收入
    free_cash_flow: Optional[float] = None      # 自由现金流
    fcf_to_profit: Optional[float] = None       # 自由现金流/净利润
    
    # 每股指标
    eps: Optional[float] = None                 # 每股收益
    bps: Optional[float] = None                 # 每股净资产
    ocf_per_share: Optional[float] = None       # 每股经营现金流
    
    # 估值相关(需要股价数据)
    pe: Optional[float] = None                  # 市盈率
    pb: Optional[float] = None                  # 市净率
    ps: Optional[float] = None                  # 市销率
    
    # 元数据
    update_time: str = ""


def safe_divide(numerator: Optional[float], denominator: Optional[float], default: float = np.nan) -> float:
    """安全除法,处理None和0的情况"""
    if numerator is None or denominator is None:
        return default
    if denominator == 0:
        return default
    return numerator / denominator


def calculate_yoy(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    """计算同比增长率"""
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / abs(previous) * 100


class FinancialIndicatorEngine:
    """财务指标计算引擎"""
    
    def __init__(self):
        self.logger = logger
    
    def calculate_all(
        self,
        balance_sheet: pd.DataFrame,
        income_statement: pd.DataFrame,
        cash_flow: pd.DataFrame,
        code: str,
        report_date: str
    ) -> FinancialIndicators:
        """
        计算所有财务指标
        
        Args:
            balance_sheet: 资产负债表
            income_statement: 利润表
            cash_flow: 现金流量表
            code: 股票代码
            report_date: 报告期
        
        Returns:
            FinancialIndicators对象
        """
        # 获取当前期数据
        bs = balance_sheet.iloc[0] if not balance_sheet.empty else pd.Series()
        inc = income_statement.iloc[0] if not income_statement.empty else pd.Series()
        cf = cash_flow.iloc[0] if not cash_flow.empty else pd.Series()
        
        # 获取上期数据(用于计算增长率)
        bs_prev = balance_sheet.iloc[1] if len(balance_sheet) > 1 else pd.Series()
        inc_prev = income_statement.iloc[1] if len(income_statement) > 1 else pd.Series()
        
        # 报告类型
        report_type = bs.get('report_type', '') or inc.get('report_type', '')
        
        indicators = FinancialIndicators(
            code=code,
            report_date=report_date,
            report_type=report_type,
            update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        # 计算盈利能力指标
        self._calculate_profitability(indicators, bs, inc)
        
        # 计算偿债能力指标
        self._calculate_solvency(indicators, bs)
        
        # 计算运营能力指标
        self._calculate_efficiency(indicators, bs, inc)
        
        # 计算成长能力指标
        self._calculate_growth(indicators, bs, inc, bs_prev, inc_prev)
        
        # 计算现金流指标
        self._calculate_cashflow(indicators, inc, cf)
        
        # 计算每股指标
        self._calculate_per_share(indicators, bs, inc, cf)
        
        self.logger.info(f"{code} {report_date} 财务指标计算完成")
        return indicators
    
    def _calculate_profitability(
        self,
        indicators: FinancialIndicators,
        bs: pd.Series,
        inc: pd.Series
    ):
        """计算盈利能力指标"""
        # ROE = 归母净利润 / 净资产
        net_profit_parent = inc.get('net_profit_parent')
        total_equity = bs.get('total_equity')
        indicators.roe = safe_divide(net_profit_parent, total_equity, 0) * 100
        
        # ROA = 净利润 / 总资产
        net_profit = inc.get('net_profit')
        total_assets = bs.get('total_assets')
        indicators.roa = safe_divide(net_profit, total_assets, 0) * 100
        
        # 毛利率 = (营业收入 - 营业成本) / 营业收入
        revenue = inc.get('operating_revenue') or inc.get('total_revenue')
        cost = inc.get('operating_cost') or inc.get('total_cost')
        if revenue and cost:
            indicators.gross_margin = (revenue - cost) / revenue * 100
        
        # 净利率 = 净利润 / 营业收入
        indicators.net_margin = safe_divide(net_profit, revenue, 0) * 100
        
        # 营业利润率
        operating_profit = inc.get('operating_profit')
        indicators.operating_margin = safe_divide(operating_profit, revenue, 0) * 100
    
    def _calculate_solvency(
        self,
        indicators: FinancialIndicators,
        bs: pd.Series
    ):
        """计算偿债能力指标"""
        # 流动比率 = 流动资产 / 流动负债
        current_assets = bs.get('total_current_assets')
        current_liabilities = bs.get('total_current_liabilities')
        indicators.current_ratio = safe_divide(current_assets, current_liabilities)
        
        # 速动比率 = (流动资产 - 存货) / 流动负债
        inventory = bs.get('inventory')
        if current_assets and inventory and current_liabilities:
            indicators.quick_ratio = (current_assets - inventory) / current_liabilities
        
        # 现金比率 = 货币资金 / 流动负债
        cash = bs.get('cash_and_deposits')
        indicators.cash_ratio = safe_divide(cash, current_liabilities)
        
        # 资产负债率 = 总负债 / 总资产
        total_liabilities = bs.get('total_liabilities')
        total_assets = bs.get('total_assets')
        indicators.debt_to_asset = safe_divide(total_liabilities, total_assets, 0) * 100
        
        # 产权比率 = 总负债 / 所有者权益
        total_equity = bs.get('total_equity')
        indicators.debt_to_equity = safe_divide(total_liabilities, total_equity, 0) * 100
        
        # 权益乘数 = 总资产 / 所有者权益
        indicators.equity_ratio = safe_divide(total_assets, total_equity)
    
    def _calculate_efficiency(
        self,
        indicators: FinancialIndicators,
        bs: pd.Series,
        inc: pd.Series
    ):
        """计算运营能力指标"""
        # 需要计算平均余额,这里简化处理使用期末余额
        revenue = inc.get('operating_revenue') or inc.get('total_revenue')
        cost = inc.get('operating_cost') or inc.get('total_cost')
        
        # 存货周转率 = 营业成本 / 存货
        inventory = bs.get('inventory')
        indicators.inventory_turnover = safe_divide(cost, inventory)
        
        # 应收账款周转率 = 营业收入 / 应收账款
        receivables = bs.get('accounts_receivable')
        indicators.receivable_turnover = safe_divide(revenue, receivables)
        
        # 总资产周转率 = 营业收入 / 总资产
        total_assets = bs.get('total_assets')
        indicators.total_asset_turnover = safe_divide(revenue, total_assets)
        
        # 流动资产周转率 = 营业收入 / 流动资产
        current_assets = bs.get('total_current_assets')
        indicators.current_asset_turnover = safe_divide(revenue, current_assets)
    
    def _calculate_growth(
        self,
        indicators: FinancialIndicators,
        bs: pd.Series,
        inc: pd.Series,
        bs_prev: pd.Series,
        inc_prev: pd.Series
    ):
        """计算成长能力指标"""
        # 营收增长率
        revenue = inc.get('operating_revenue') or inc.get('total_revenue')
        revenue_prev = inc_prev.get('operating_revenue') or inc_prev.get('total_revenue')
        indicators.revenue_growth = calculate_yoy(revenue, revenue_prev)
        
        # 净利润增长率
        net_profit = inc.get('net_profit')
        net_profit_prev = inc_prev.get('net_profit')
        indicators.profit_growth = calculate_yoy(net_profit, net_profit_prev)
        
        # 归母净利润增长率
        net_profit_parent = inc.get('net_profit_parent')
        net_profit_parent_prev = inc_prev.get('net_profit_parent')
        indicators.net_profit_growth = calculate_yoy(net_profit_parent, net_profit_parent_prev)
        
        # 总资产增长率
        total_assets = bs.get('total_assets')
        total_assets_prev = bs_prev.get('total_assets')
        indicators.asset_growth = calculate_yoy(total_assets, total_assets_prev)
        
        # 净资产增长率
        total_equity = bs.get('total_equity')
        total_equity_prev = bs_prev.get('total_equity')
        indicators.equity_growth = calculate_yoy(total_equity, total_equity_prev)
    
    def _calculate_cashflow(
        self,
        indicators: FinancialIndicators,
        inc: pd.Series,
        cf: pd.Series
    ):
        """计算现金流指标"""
        # 经营现金流/净利润
        ocf = cf.get('operating_cash_flow')
        net_profit = inc.get('net_profit')
        indicators.ocf_to_profit = safe_divide(ocf, net_profit)
        
        # 经营现金流/营业收入
        revenue = inc.get('operating_revenue') or inc.get('total_revenue')
        indicators.ocf_to_revenue = safe_divide(ocf, revenue)
        
        # 自由现金流 = 经营现金流 - 资本支出
        capex = cf.get('cash_for_fixed_assets') or 0
        if ocf:
            indicators.free_cash_flow = ocf - abs(capex)
            indicators.fcf_to_profit = safe_divide(indicators.free_cash_flow, net_profit)
    
    def _calculate_per_share(
        self,
        indicators: FinancialIndicators,
        bs: pd.Series,
        inc: pd.Series,
        cf: pd.Series
    ):
        """计算每股指标"""
        # 需要总股本数据,这里简化处理
        # 实际应用中应该从股本数据获取
        
        # EPS
        indicators.eps = inc.get('basic_eps')
        
        # BPS = 净资产 / 总股本 (简化)
        # 实际应该使用准确的股本数
        total_equity = bs.get('total_equity')
        share_capital = bs.get('share_capital')
        if total_equity and share_capital and share_capital > 0:
            indicators.bps = total_equity / share_capital
        
        # 每股经营现金流
        ocf = cf.get('operating_cash_flow')
        if ocf and share_capital and share_capital > 0:
            indicators.ocf_per_share = ocf / share_capital
    
    def calculate_batch(
        self,
        financial_data: Dict[str, Dict[str, pd.DataFrame]]
    ) -> pd.DataFrame:
        """
        批量计算财务指标
        
        Args:
            financial_data: {
                'code': {
                    'balance_sheet': DataFrame,
                    'income_statement': DataFrame,
                    'cash_flow': DataFrame
                }
            }
        
        Returns:
            财务指标DataFrame
        """
        all_indicators = []
        
        for code, data in financial_data.items():
            bs = data.get('balance_sheet', pd.DataFrame())
            inc = data.get('income_statement', pd.DataFrame())
            cf = data.get('cash_flow', pd.DataFrame())
            
            if bs.empty or inc.empty:
                self.logger.warning(f"{code} 缺少财务数据,跳过指标计算")
                continue
            
            # 获取最新报告期
            report_date = bs.iloc[0].get('report_date', '') if not bs.empty else ''
            
            try:
                indicators = self.calculate_all(bs, inc, cf, code, report_date)
                all_indicators.append(asdict(indicators))
            except Exception as e:
                self.logger.error(f"{code} 财务指标计算失败: {e}")
        
        return pd.DataFrame(all_indicators)


# ==================== 便捷函数 ====================

def calculate_roe(net_profit: float, equity: float) -> Optional[float]:
    """计算ROE"""
    return safe_divide(net_profit, equity, 0) * 100


def calculate_roa(net_profit: float, total_assets: float) -> Optional[float]:
    """计算ROA"""
    return safe_divide(net_profit, total_assets, 0) * 100


def calculate_gross_margin(revenue: float, cost: float) -> Optional[float]:
    """计算毛利率"""
    if revenue == 0:
        return None
    return (revenue - cost) / revenue * 100


def calculate_net_margin(net_profit: float, revenue: float) -> Optional[float]:
    """计算净利率"""
    return safe_divide(net_profit, revenue, 0) * 100


def calculate_current_ratio(current_assets: float, current_liabilities: float) -> Optional[float]:
    """计算流动比率"""
    return safe_divide(current_assets, current_liabilities)


def calculate_debt_to_asset(total_liabilities: float, total_assets: float) -> Optional[float]:
    """计算资产负债率"""
    return safe_divide(total_liabilities, total_assets, 0) * 100


def calculate_all_indicators(
    balance_sheet: pd.DataFrame,
    income_statement: pd.DataFrame,
    cash_flow: pd.DataFrame,
    code: str,
    report_date: str
) -> FinancialIndicators:
    """计算所有财务指标 (便捷函数)"""
    engine = FinancialIndicatorEngine()
    return engine.calculate_all(balance_sheet, income_statement, cash_flow, code, report_date)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 财务指标计算引擎")
    print("=" * 50)
    
    # 构造测试数据
    balance_sheet = pd.DataFrame([{
        'code': '000001',
        'report_date': '2023-12-31',
        'report_type': '年报',
        'total_assets': 1000,
        'total_liabilities': 400,
        'total_equity': 600,
        'total_current_assets': 600,
        'total_current_liabilities': 300,
        'inventory': 100,
        'cash_and_deposits': 200,
        'accounts_receivable': 150,
        'share_capital': 100,
    }])
    
    income_statement = pd.DataFrame([{
        'code': '000001',
        'report_date': '2023-12-31',
        'operating_revenue': 500,
        'operating_cost': 300,
        'net_profit': 80,
        'net_profit_parent': 75,
        'operating_profit': 100,
        'basic_eps': 0.75,
    }])
    
    cash_flow = pd.DataFrame([{
        'code': '000001',
        'report_date': '2023-12-31',
        'operating_cash_flow': 90,
        'cash_for_fixed_assets': -30,
    }])
    
    engine = FinancialIndicatorEngine()
    indicators = engine.calculate_all(balance_sheet, income_statement, cash_flow, '000001', '2023-12-31')
    
    print("\n计算结果:")
    print(f"  ROE: {indicators.roe:.2f}%")
    print(f"  ROA: {indicators.roa:.2f}%")
    print(f"  毛利率: {indicators.gross_margin:.2f}%")
    print(f"  净利率: {indicators.net_margin:.2f}%")
    print(f"  流动比率: {indicators.current_ratio:.2f}")
    print(f"  资产负债率: {indicators.debt_to_asset:.2f}%")
    print(f"  经营现金流/净利润: {indicators.ocf_to_profit:.2f}")
