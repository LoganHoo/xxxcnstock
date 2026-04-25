#!/usr/bin/env python3
"""
资产负债表获取器

支持多数据源:
- Tushare Pro: 主要数据源,数据完整
- AKShare: 备用数据源
- Baostock: 备用数据源

数据字段覆盖:
- 资产: 流动资产、非流动资产、总资产
- 负债: 流动负债、非流动负债、总负债
- 所有者权益: 股本、资本公积、盈余公积、未分配利润
"""
import asyncio
import pandas as pd
import polars as pl
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime, date
from pathlib import Path
import time
import akshare as ak

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("balance_sheet_fetcher", log_file="system/balance_sheet_fetcher.log")


@dataclass
class BalanceSheetData:
    """资产负债表数据模型"""
    # 基本信息
    code: str                           # 股票代码
    report_date: str                    # 报告期 (YYYY-MM-DD)
    report_type: str                    # 报告类型 (年报/中报/一季报/三季报)
    
    # 资产 - 流动资产
    total_current_assets: Optional[float] = None      # 流动资产合计
    cash_and_deposits: Optional[float] = None         # 货币资金
    trading_financial_assets: Optional[float] = None  # 交易性金融资产
    notes_receivable: Optional[float] = None          # 应收票据
    accounts_receivable: Optional[float] = None       # 应收账款
    prepayments: Optional[float] = None               # 预付款项
    interest_receivable: Optional[float] = None       # 应收利息
    dividend_receivable: Optional[float] = None       # 应收股利
    other_receivables: Optional[float] = None         # 其他应收款
    inventory: Optional[float] = None                 # 存货
    non_current_assets_due: Optional[float] = None    # 一年内到期的非流动资产
    other_current_assets: Optional[float] = None      # 其他流动资产
    
    # 资产 - 非流动资产
    total_non_current_assets: Optional[float] = None  # 非流动资产合计
    available_for_sale_assets: Optional[float] = None # 可供出售金融资产
    held_to_maturity: Optional[float] = None          # 持有至到期投资
    long_term_receivables: Optional[float] = None     # 长期应收款
    long_term_equity: Optional[float] = None          # 长期股权投资
    investment_property: Optional[float] = None       # 投资性房地产
    fixed_assets: Optional[float] = None              # 固定资产
    construction_in_progress: Optional[float] = None  # 在建工程
    engineering_materials: Optional[float] = None     # 工程物资
    fixed_assets_disposal: Optional[float] = None     # 固定资产清理
    productive_biological: Optional[float] = None     # 生产性生物资产
    oil_gas_assets: Optional[float] = None            # 油气资产
    intangible_assets: Optional[float] = None         # 无形资产
    development_expenditure: Optional[float] = None   # 开发支出
    goodwill: Optional[float] = None                  # 商誉
    long_term_prepaid: Optional[float] = None         # 长期待摊费用
    deferred_tax_assets: Optional[float] = None       # 递延所得税资产
    other_non_current_assets: Optional[float] = None  # 其他非流动资产
    
    # 资产总计
    total_assets: Optional[float] = None              # 资产总计
    
    # 负债 - 流动负债
    total_current_liabilities: Optional[float] = None # 流动负债合计
    short_term_loans: Optional[float] = None          # 短期借款
    trading_financial_liabilities: Optional[float] = None  # 交易性金融负债
    notes_payable: Optional[float] = None             # 应付票据
    accounts_payable: Optional[float] = None          # 应付账款
    advance_receipts: Optional[float] = None          # 预收款项
    employee_payables: Optional[float] = None         # 应付职工薪酬
    tax_payables: Optional[float] = None              # 应交税费
    interest_payables: Optional[float] = None         # 应付利息
    dividend_payables: Optional[float] = None         # 应付股利
    other_payables: Optional[float] = None            # 其他应付款
    non_current_liabilities_due: Optional[float] = None  # 一年内到期的非流动负债
    other_current_liabilities: Optional[float] = None # 其他流动负债
    
    # 负债 - 非流动负债
    total_non_current_liabilities: Optional[float] = None  # 非流动负债合计
    long_term_loans: Optional[float] = None           # 长期借款
    bonds_payable: Optional[float] = None             # 应付债券
    long_term_payables: Optional[float] = None        # 长期应付款
    special_payables: Optional[float] = None          # 专项应付款
    estimated_liabilities: Optional[float] = None     # 预计负债
    deferred_tax_liabilities: Optional[float] = None  # 递延所得税负债
    other_non_current_liabilities: Optional[float] = None  # 其他非流动负债
    
    # 负债合计
    total_liabilities: Optional[float] = None         # 负债合计
    
    # 所有者权益
    total_equity: Optional[float] = None              # 所有者权益合计
    share_capital: Optional[float] = None             # 实收资本(或股本)
    capital_reserve: Optional[float] = None           # 资本公积
    treasury_stock: Optional[float] = None            # 减:库存股
    surplus_reserve: Optional[float] = None           # 盈余公积
    general_risk_reserve: Optional[float] = None      # 一般风险准备
    undistributed_profit: Optional[float] = None      # 未分配利润
    
    # 外币报表折算差额
    translation_reserve: Optional[float] = None       # 外币报表折算差额
    
    # 少数股东权益
    minority_interest: Optional[float] = None         # 少数股东权益
    
    # 负债和所有者权益总计
    total_liabilities_and_equity: Optional[float] = None  # 负债和所有者权益总计
    
    # 元数据
    source: str = ""                                  # 数据来源
    update_time: str = ""                             # 更新时间


class BalanceSheetFetcher:
    """资产负债表获取器"""
    
    def __init__(self, tushare_token: Optional[str] = None):
        self.tushare_token = tushare_token
        self._tushare_pro = None
        self.logger = logger
        
    def _get_tushare_pro(self):
        """获取Tushare Pro接口"""
        if self._tushare_pro is None:
            try:
                import tushare as ts
                if self.tushare_token:
                    self._tushare_pro = ts.pro_api(self.tushare_token)
                else:
                    # 尝试从环境变量获取
                    import os
                    token = os.getenv('TUSHARE_TOKEN')
                    if token:
                        self._tushare_pro = ts.pro_api(token)
                    else:
                        self.logger.warning("Tushare token未设置,将使用AKShare作为数据源")
                        return None
            except ImportError:
                self.logger.warning("tushare未安装,将使用AKShare作为数据源")
                return None
        return self._tushare_pro
    
    def fetch_from_tushare(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        从Tushare Pro获取资产负债表
        
        Args:
            code: 股票代码 (如: 000001.SZ)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            report_type: 报告类型 (1=年报, 2=中报, 3=一季报, 4=三季报)
        
        Returns:
            资产负债表DataFrame
        """
        pro = self._get_tushare_pro()
        if pro is None:
            return pd.DataFrame()
        
        try:
            # 转换代码格式
            if '.' not in code:
                code = self._format_code(code)
            
            params = {
                'ts_code': code,
            }
            if start_date:
                params['start_date'] = start_date.replace('-', '')
            if end_date:
                params['end_date'] = end_date.replace('-', '')
            if report_type:
                params['report_type'] = report_type
            
            df = pro.balancesheet(**params)
            
            if df.empty:
                self.logger.warning(f"{code} 未获取到资产负债表数据")
                return df
            
            # 标准化列名
            df = self._standardize_tushare_columns(df)
            df['source'] = 'tushare'
            
            self.logger.info(f"{code} 从Tushare获取到 {len(df)} 条资产负债表记录")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 从Tushare获取资产负债表失败: {e}")
            return pd.DataFrame()
    
    def fetch_from_akshare(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        从AKShare获取资产负债表
        
        Args:
            code: 股票代码 (如: 000001)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            资产负债表DataFrame
        """
        try:
            # 移除后缀
            code = code.split('.')[0] if '.' in code else code
            
            # 使用AKShare获取资产负债表
            df = ak.stock_balance_sheet_by_report_em(symbol=code)
            
            if df.empty:
                self.logger.warning(f"{code} 未从AKShare获取到资产负债表数据")
                return df
            
            # 标准化列名
            df = self._standardize_akshare_columns(df)
            df['source'] = 'akshare'
            
            # 日期过滤
            if start_date:
                df = df[df['report_date'] >= start_date]
            if end_date:
                df = df[df['report_date'] <= end_date]
            
            self.logger.info(f"{code} 从AKShare获取到 {len(df)} 条资产负债表记录")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 从AKShare获取资产负债表失败: {e}")
            return pd.DataFrame()
    
    def fetch(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None,
        priority: str = 'tushare'
    ) -> pd.DataFrame:
        """
        获取资产负债表 (自动选择数据源)
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型
            priority: 优先数据源 (tushare/akshare)
        
        Returns:
            资产负债表DataFrame
        """
        # 尝试优先数据源
        if priority == 'tushare':
            df = self.fetch_from_tushare(code, start_date, end_date, report_type)
            if df.empty:
                self.logger.info(f"{code} Tushare获取失败,尝试AKShare")
                df = self.fetch_from_akshare(code, start_date, end_date)
        else:
            df = self.fetch_from_akshare(code, start_date, end_date)
            if df.empty:
                self.logger.info(f"{code} AKShare获取失败,尝试Tushare")
                df = self.fetch_from_tushare(code, start_date, end_date, report_type)
        
        return df
    
    def fetch_batch(
        self,
        codes: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        delay: float = 0.5
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取资产负债表
        
        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            delay: 请求间隔(秒)
        
        Returns:
            {code: DataFrame} 字典
        """
        results = {}
        
        for i, code in enumerate(codes, 1):
            self.logger.info(f"[{i}/{len(codes)}] 获取 {code} 资产负债表...")
            
            df = self.fetch(code, start_date, end_date)
            if not df.empty:
                results[code] = df
            
            # 请求间隔,避免频率限制
            if i < len(codes):
                time.sleep(delay)
        
        self.logger.info(f"批量获取完成: 成功 {len(results)}/{len(codes)}")
        return results
    
    def _format_code(self, code: str) -> str:
        """格式化股票代码"""
        code = code.strip()
        if '.' in code:
            return code
        
        # 根据代码前缀判断交易所
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        elif code.startswith('8') or code.startswith('4'):
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"
    
    def _standardize_tushare_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化Tushare列名"""
        column_mapping = {
            'ts_code': 'code',
            'ann_date': 'announce_date',
            'f_ann_date': 'f_announce_date',
            'end_date': 'report_date',
            'comp_type': 'company_type',
            'total_assets': 'total_assets',
            'total_liab': 'total_liabilities',
            'total_hldr_eqy_exc_min_int': 'total_equity',
            'total_hldr_eqy_inc_min_int': 'total_liabilities_and_equity',
            'total_cur_assets': 'total_current_assets',
            'total_nca': 'total_non_current_assets',
            'total_cur_liab': 'total_current_liabilities',
            'total_ncl': 'total_non_current_liabilities',
            'money_cap': 'cash_and_deposits',
            'trad_asset': 'trading_financial_assets',
            'notes_receiv': 'notes_receivable',
            'accounts_receiv': 'accounts_receivable',
            'prepayment': 'prepayments',
            'int_receiv': 'interest_receivable',
            'div_receiv': 'dividend_receivable',
            'oth_receiv': 'other_receivables',
            'inventories': 'inventory',
            'amor_exp': 'non_current_assets_due',
            'oth_cur_assets': 'other_current_assets',
            'fix_assets': 'fixed_assets',
            'cip': 'construction_in_progress',
            'const_materials': 'engineering_materials',
            'intan_assets': 'intangible_assets',
            'goodwill': 'goodwill',
            'lt_amor_exp': 'long_term_prepaid',
            'defer_tax_assets': 'deferred_tax_assets',
            'st_borr': 'short_term_loans',
            'trad_fl': 'trading_financial_liabilities',
            'notes_payable': 'notes_payable',
            'acct_payable': 'accounts_payable',
            'adv_receipts': 'advance_receipts',
            'emp_payable': 'employee_payables',
            'taxes_payable': 'tax_payables',
            'int_payable': 'interest_payables',
            'div_payable': 'dividend_payables',
            'oth_payable': 'other_payables',
            'lt_borr': 'long_term_loans',
            'bond_payable': 'bonds_payable',
            'lt_payable': 'long_term_payables',
            'provisions': 'estimated_liabilities',
            'defer_tax_liab': 'deferred_tax_liabilities',
            'cap_rese': 'capital_reserve',
            'treasury_stock': 'treasury_stock',
            'surplus_rese': 'surplus_reserve',
            'undistributed_profit': 'undistributed_profit',
        }
        
        # 重命名存在的列
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        # 添加code列(如果没有)
        if 'code' not in df.columns and 'ts_code' in df.columns:
            df['code'] = df['ts_code']
        
        return df
    
    def _standardize_akshare_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化AKShare列名"""
        # AKShare列名已经是中文,需要映射到标准字段名
        column_mapping = {
            '股票代码': 'code',
            '报告期': 'report_date',
            '公告日期': 'announce_date',
            '流动资产合计': 'total_current_assets',
            '货币资金': 'cash_and_deposits',
            '交易性金融资产': 'trading_financial_assets',
            '应收票据': 'notes_receivable',
            '应收账款': 'accounts_receivable',
            '预付款项': 'prepayments',
            '应收利息': 'interest_receivable',
            '应收股利': 'dividend_receivable',
            '其他应收款': 'other_receivables',
            '存货': 'inventory',
            '一年内到期的非流动资产': 'non_current_assets_due',
            '其他流动资产': 'other_current_assets',
            '非流动资产合计': 'total_non_current_assets',
            '可供出售金融资产': 'available_for_sale_assets',
            '持有至到期投资': 'held_to_maturity',
            '长期应收款': 'long_term_receivables',
            '长期股权投资': 'long_term_equity',
            '投资性房地产': 'investment_property',
            '固定资产': 'fixed_assets',
            '在建工程': 'construction_in_progress',
            '工程物资': 'engineering_materials',
            '无形资产': 'intangible_assets',
            '商誉': 'goodwill',
            '长期待摊费用': 'long_term_prepaid',
            '递延所得税资产': 'deferred_tax_assets',
            '其他非流动资产': 'other_non_current_assets',
            '资产总计': 'total_assets',
            '流动负债合计': 'total_current_liabilities',
            '短期借款': 'short_term_loans',
            '交易性金融负债': 'trading_financial_liabilities',
            '应付票据': 'notes_payable',
            '应付账款': 'accounts_payable',
            '预收款项': 'advance_receipts',
            '应付职工薪酬': 'employee_payables',
            '应交税费': 'tax_payables',
            '应付利息': 'interest_payables',
            '应付股利': 'dividend_payables',
            '其他应付款': 'other_payables',
            '一年内到期的非流动负债': 'non_current_liabilities_due',
            '其他流动负债': 'other_current_liabilities',
            '非流动负债合计': 'total_non_current_liabilities',
            '长期借款': 'long_term_loans',
            '应付债券': 'bonds_payable',
            '长期应付款': 'long_term_payables',
            '预计负债': 'estimated_liabilities',
            '递延所得税负债': 'deferred_tax_liabilities',
            '负债合计': 'total_liabilities',
            '所有者权益合计': 'total_equity',
            '实收资本': 'share_capital',
            '资本公积': 'capital_reserve',
            '减:库存股': 'treasury_stock',
            '盈余公积': 'surplus_reserve',
            '未分配利润': 'undistributed_profit',
            '少数股东权益': 'minority_interest',
            '负债和所有者权益总计': 'total_liabilities_and_equity',
        }
        
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        return df


# ==================== 便捷函数 ====================

def fetch_balance_sheet(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    report_type: Optional[str] = None,
    tushare_token: Optional[str] = None
) -> pd.DataFrame:
    """
    获取单只股票资产负债表 (便捷函数)
    
    Args:
        code: 股票代码
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        report_type: 报告类型
        tushare_token: Tushare token
    
    Returns:
        资产负债表DataFrame
    """
    fetcher = BalanceSheetFetcher(tushare_token)
    return fetcher.fetch(code, start_date, end_date, report_type)


def fetch_balance_sheet_batch(
    codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    delay: float = 0.5,
    tushare_token: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    批量获取资产负债表 (便捷函数)
    
    Args:
        codes: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        delay: 请求间隔
        tushare_token: Tushare token
    
    Returns:
        {code: DataFrame} 字典
    """
    fetcher = BalanceSheetFetcher(tushare_token)
    return fetcher.fetch_batch(codes, start_date, end_date, delay)


# ==================== 测试代码 ====================

if __name__ == "__main__":
    # 测试单只股票获取
    print("=" * 50)
    print("测试: 获取单只股票资产负债表")
    print("=" * 50)
    
    code = "000001"  # 平安银行
    df = fetch_balance_sheet(code, start_date="2023-01-01")
    
    if not df.empty:
        print(f"\n成功获取 {code} 资产负债表:")
        print(f"记录数: {len(df)}")
        print(f"\n最新报告期数据:")
        print(df.head(1).to_string())
    else:
        print(f"获取 {code} 资产负债表失败")
    
    # 测试批量获取
    print("\n" + "=" * 50)
    print("测试: 批量获取资产负债表")
    print("=" * 50)
    
    codes = ["000001", "000002", "600000"]
    results = fetch_balance_sheet_batch(codes, delay=1.0)
    
    for code, df in results.items():
        print(f"{code}: {len(df)} 条记录")
