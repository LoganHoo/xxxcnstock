#!/usr/bin/env python3
"""
利润表获取器

支持多数据源:
- Tushare Pro: 主要数据源
- AKShare: 备用数据源

数据字段覆盖:
- 营业收入、营业成本
- 期间费用(销售/管理/研发/财务)
- 营业利润、利润总额、净利润
- 每股收益(EPS)
- 其他综合收益
"""
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import time
import akshare as ak

from core.logger import setup_logger

logger = setup_logger("income_statement_fetcher", log_file="system/income_statement_fetcher.log")


@dataclass
class IncomeStatementData:
    """利润表数据模型"""
    # 基本信息
    code: str                           # 股票代码
    report_date: str                    # 报告期
    report_type: str                    # 报告类型
    
    # 营业收入
    total_revenue: Optional[float] = None           # 营业总收入
    operating_revenue: Optional[float] = None       # 营业收入
    interest_income: Optional[float] = None         # 利息收入
    premium_income: Optional[float] = None          # 已赚保费
    fee_commission_income: Optional[float] = None   # 手续费及佣金收入
    
    # 营业成本
    total_cost: Optional[float] = None              # 营业总成本
    operating_cost: Optional[float] = None          # 营业成本
    interest_expense: Optional[float] = None        # 利息支出
    premium_expense: Optional[float] = None         # 手续费及佣金支出
    
    # 期间费用
    sales_expense: Optional[float] = None           # 销售费用
    admin_expense: Optional[float] = None           # 管理费用
    rd_expense: Optional[float] = None              # 研发费用
    financial_expense: Optional[float] = None       # 财务费用
    
    # 其他收益
    other_income: Optional[float] = None            # 其他收益
    investment_income: Optional[float] = None       # 投资收益
    fair_value_change: Optional[float] = None       # 公允价值变动收益
    credit_impairment: Optional[float] = None       # 信用减值损失
    asset_impairment: Optional[float] = None        # 资产减值损失
    asset_disposal_income: Optional[float] = None   # 资产处置收益
    
    # 营业利润
    operating_profit: Optional[float] = None        # 营业利润
    non_operating_income: Optional[float] = None    # 营业外收入
    non_operating_expense: Optional[float] = None   # 营业外支出
    
    # 利润总额
    total_profit: Optional[float] = None            # 利润总额
    income_tax: Optional[float] = None              # 所得税费用
    
    # 净利润
    net_profit: Optional[float] = None              # 净利润
    net_profit_parent: Optional[float] = None       # 归母净利润
    minority_interest: Optional[float] = None       # 少数股东损益
    
    # 每股收益
    basic_eps: Optional[float] = None               # 基本每股收益
    diluted_eps: Optional[float] = None             # 稀释每股收益
    
    # 其他综合收益
    other_comprehensive_income: Optional[float] = None  # 其他综合收益
    total_comprehensive_income: Optional[float] = None  # 综合收益总额
    
    # 元数据
    source: str = ""
    update_time: str = ""


class IncomeStatementFetcher:
    """利润表获取器"""
    
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
                    import os
                    token = os.getenv('TUSHARE_TOKEN')
                    if token:
                        self._tushare_pro = ts.pro_api(token)
                    else:
                        self.logger.warning("Tushare token未设置")
                        return None
            except ImportError:
                self.logger.warning("tushare未安装")
                return None
        return self._tushare_pro
    
    def fetch_from_tushare(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None
    ) -> pd.DataFrame:
        """从Tushare Pro获取利润表"""
        pro = self._get_tushare_pro()
        if pro is None:
            return pd.DataFrame()
        
        try:
            if '.' not in code:
                code = self._format_code(code)
            
            params = {'ts_code': code}
            if start_date:
                params['start_date'] = start_date.replace('-', '')
            if end_date:
                params['end_date'] = end_date.replace('-', '')
            if report_type:
                params['report_type'] = report_type
            
            df = pro.income(**params)
            
            if df.empty:
                self.logger.warning(f"{code} 未获取到利润表数据")
                return df
            
            df = self._standardize_tushare_columns(df)
            df['source'] = 'tushare'
            
            self.logger.info(f"{code} 从Tushare获取到 {len(df)} 条利润表记录")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 从Tushare获取利润表失败: {e}")
            return pd.DataFrame()
    
    def fetch_from_akshare(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """从AKShare获取利润表"""
        try:
            code = code.split('.')[0] if '.' in code else code
            
            # 使用AKShare获取利润表
            df = ak.stock_profit_sheet_by_report_em(symbol=code)
            
            if df.empty:
                self.logger.warning(f"{code} 未从AKShare获取到利润表数据")
                return df
            
            df = self._standardize_akshare_columns(df)
            df['source'] = 'akshare'
            
            if start_date:
                df = df[df['report_date'] >= start_date]
            if end_date:
                df = df[df['report_date'] <= end_date]
            
            self.logger.info(f"{code} 从AKShare获取到 {len(df)} 条利润表记录")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 从AKShare获取利润表失败: {e}")
            return pd.DataFrame()
    
    def fetch(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None,
        priority: str = 'tushare'
    ) -> pd.DataFrame:
        """获取利润表 (自动选择数据源)"""
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
        """批量获取利润表"""
        results = {}
        
        for i, code in enumerate(codes, 1):
            self.logger.info(f"[{i}/{len(codes)}] 获取 {code} 利润表...")
            
            df = self.fetch(code, start_date, end_date)
            if not df.empty:
                results[code] = df
            
            if i < len(codes):
                time.sleep(delay)
        
        self.logger.info(f"批量获取完成: 成功 {len(results)}/{len(codes)}")
        return results
    
    def _format_code(self, code: str) -> str:
        """格式化股票代码"""
        code = code.strip()
        if '.' in code:
            return code
        
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
            'total_revenue': 'total_revenue',
            'revenue': 'operating_revenue',
            'total_cogs': 'total_cost',
            'oper_cost': 'operating_cost',
            'int_exp': 'interest_expense',
            'sell_exp': 'sales_expense',
            'admin_exp': 'admin_expense',
            'rd_exp': 'rd_expense',
            'fin_exp': 'financial_expense',
            'invest_income': 'investment_income',
            'fair_value_chg': 'fair_value_change',
            'credit_impair_loss': 'credit_impairment',
            'assets_impair_loss': 'asset_impairment',
            'assets_disp_income': 'asset_disposal_income',
            'operate_profit': 'operating_profit',
            'non_oper_income': 'non_operating_income',
            'non_oper_exp': 'non_operating_expense',
            'total_profit': 'total_profit',
            'income_tax': 'income_tax',
            'n_income': 'net_profit',
            'n_income_attr_p': 'net_profit_parent',
            'minority_gain': 'minority_interest',
            'basic_eps': 'basic_eps',
            ' diluted_eps': 'diluted_eps',
            'oth_compr_income': 'other_comprehensive_income',
            'total_compr_income': 'total_comprehensive_income',
        }
        
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        if 'code' not in df.columns and 'ts_code' in df.columns:
            df['code'] = df['ts_code']
        
        return df
    
    def _standardize_akshare_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化AKShare列名"""
        column_mapping = {
            '股票代码': 'code',
            '报告期': 'report_date',
            '公告日期': 'announce_date',
            '营业总收入': 'total_revenue',
            '营业收入': 'operating_revenue',
            '利息收入': 'interest_income',
            '营业总成本': 'total_cost',
            '营业成本': 'operating_cost',
            '利息支出': 'interest_expense',
            '销售费用': 'sales_expense',
            '管理费用': 'admin_expense',
            '研发费用': 'rd_expense',
            '财务费用': 'financial_expense',
            '其他收益': 'other_income',
            '投资收益': 'investment_income',
            '公允价值变动收益': 'fair_value_change',
            '信用减值损失': 'credit_impairment',
            '资产减值损失': 'asset_impairment',
            '资产处置收益': 'asset_disposal_income',
            '营业利润': 'operating_profit',
            '营业外收入': 'non_operating_income',
            '营业外支出': 'non_operating_expense',
            '利润总额': 'total_profit',
            '所得税费用': 'income_tax',
            '净利润': 'net_profit',
            '归属于母公司股东的净利润': 'net_profit_parent',
            '少数股东损益': 'minority_interest',
            '基本每股收益': 'basic_eps',
            '稀释每股收益': 'diluted_eps',
            '其他综合收益': 'other_comprehensive_income',
            '综合收益总额': 'total_comprehensive_income',
        }
        
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        return df


# ==================== 便捷函数 ====================

def fetch_income_statement(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    report_type: Optional[str] = None,
    tushare_token: Optional[str] = None
) -> pd.DataFrame:
    """获取单只股票利润表 (便捷函数)"""
    fetcher = IncomeStatementFetcher(tushare_token)
    return fetcher.fetch(code, start_date, end_date, report_type)


def fetch_income_statement_batch(
    codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    delay: float = 0.5,
    tushare_token: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """批量获取利润表 (便捷函数)"""
    fetcher = IncomeStatementFetcher(tushare_token)
    return fetcher.fetch_batch(codes, start_date, end_date, delay)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 获取单只股票利润表")
    print("=" * 50)
    
    code = "000001"
    df = fetch_income_statement(code, start_date="2023-01-01")
    
    if not df.empty:
        print(f"\n成功获取 {code} 利润表:")
        print(f"记录数: {len(df)}")
        print(f"\n最新报告期数据:")
        print(df[['code', 'report_date', 'operating_revenue', 'net_profit', 'basic_eps']].head(1).to_string())
    else:
        print(f"获取 {code} 利润表失败")
