#!/usr/bin/env python3
"""
现金流量表获取器

支持多数据源:
- Tushare Pro: 主要数据源
- AKShare: 备用数据源

数据字段覆盖:
- 经营活动现金流
- 投资活动现金流
- 筹资活动现金流
- 现金及现金等价物净增加额
"""
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import time
import akshare as ak

from core.logger import setup_logger

logger = setup_logger("cash_flow_fetcher", log_file="system/cash_flow_fetcher.log")


@dataclass
class CashFlowData:
    """现金流量表数据模型"""
    # 基本信息
    code: str                           # 股票代码
    report_date: str                    # 报告期
    report_type: str                    # 报告类型
    
    # 经营活动现金流
    operating_cash_flow: Optional[float] = None       # 经营活动现金流净额
    cash_from_sales: Optional[float] = None           # 销售商品收到的现金
    cash_from_services: Optional[float] = None        # 提供劳务收到的现金
    cash_from_interest: Optional[float] = None        # 收取利息的现金
    cash_from_premiums: Optional[float] = None        # 收取保费的现金
    cash_to_suppliers: Optional[float] = None         # 购买商品支付的现金
    cash_to_employees: Optional[float] = None         # 支付给职工的现金
    cash_paid_for_taxes: Optional[float] = None       # 支付的各项税费
    other_operating_cash: Optional[float] = None      # 其他经营活动现金流
    
    # 投资活动现金流
    investing_cash_flow: Optional[float] = None       # 投资活动现金流净额
    cash_from_investments: Optional[float] = None     # 收回投资收到的现金
    cash_from_returns: Optional[float] = None         # 取得投资收益收到的现金
    cash_from_asset_sales: Optional[float] = None     # 处置资产收回的现金
    cash_for_investments: Optional[float] = None      # 投资支付的现金
    cash_for_fixed_assets: Optional[float] = None     # 购建固定资产支付的现金
    cash_for_intangible: Optional[float] = None       # 购建无形资产支付的现金
    
    # 筹资活动现金流
    financing_cash_flow: Optional[float] = None       # 筹资活动现金流净额
    cash_from_investors: Optional[float] = None       # 吸收投资收到的现金
    cash_from_borrowings: Optional[float] = None      # 取得借款收到的现金
    cash_from_bonds: Optional[float] = None           # 发行债券收到的现金
    cash_repayment: Optional[float] = None            # 偿还债务支付的现金
    cash_dividends: Optional[float] = None            # 分配股利支付的现金
    cash_for_interest: Optional[float] = None         # 支付利息的现金
    
    # 汇率变动影响
    exchange_rate_effect: Optional[float] = None      # 汇率变动对现金的影响
    
    # 现金净变动
    net_cash_increase: Optional[float] = None         # 现金及等价物净增加额
    beginning_cash: Optional[float] = None            # 期初现金余额
    ending_cash: Optional[float] = None               # 期末现金余额
    
    # 补充资料
    depreciation: Optional[float] = None              # 固定资产折旧
    amortization: Optional[float] = None              # 无形资产摊销
    
    # 元数据
    source: str = ""
    update_time: str = ""


class CashFlowFetcher:
    """现金流量表获取器"""
    
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
        """从Tushare Pro获取现金流量表"""
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
            
            df = pro.cashflow(**params)
            
            if df.empty:
                self.logger.warning(f"{code} 未获取到现金流量表数据")
                return df
            
            df = self._standardize_tushare_columns(df)
            df['source'] = 'tushare'
            
            self.logger.info(f"{code} 从Tushare获取到 {len(df)} 条现金流量表记录")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 从Tushare获取现金流量表失败: {e}")
            return pd.DataFrame()
    
    def fetch_from_akshare(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """从AKShare获取现金流量表"""
        try:
            code = code.split('.')[0] if '.' in code else code
            
            # 使用AKShare获取现金流量表
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
            
            if df.empty:
                self.logger.warning(f"{code} 未从AKShare获取到现金流量表数据")
                return df
            
            df = self._standardize_akshare_columns(df)
            df['source'] = 'akshare'
            
            if start_date:
                df = df[df['report_date'] >= start_date]
            if end_date:
                df = df[df['report_date'] <= end_date]
            
            self.logger.info(f"{code} 从AKShare获取到 {len(df)} 条现金流量表记录")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 从AKShare获取现金流量表失败: {e}")
            return pd.DataFrame()
    
    def fetch(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        report_type: Optional[str] = None,
        priority: str = 'tushare'
    ) -> pd.DataFrame:
        """获取现金流量表 (自动选择数据源)"""
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
        """批量获取现金流量表"""
        results = {}
        
        for i, code in enumerate(codes, 1):
            self.logger.info(f"[{i}/{len(codes)}] 获取 {code} 现金流量表...")
            
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
            'n_cashflow_act': 'operating_cash_flow',
            'c_inf_fr_operate_a': 'cash_from_sales',
            'c_paid_for_salaries': 'cash_to_employees',
            'c_paid_for_taxes': 'cash_paid_for_taxes',
            'n_cashflow_inv_act': 'investing_cash_flow',
            'c_inf_fr_invest_a': 'cash_from_investments',
            'c_paid_invest': 'cash_for_investments',
            'c_paid_acq_const': 'cash_for_fixed_assets',
            'n_cash_flows_fnc_act': 'financing_cash_flow',
            'c_inf_fr_finance_a': 'cash_from_borrowings',
            'c_paid_debt': 'cash_repayment',
            'c_paid_div_prof_int': 'cash_dividends',
            'forex_effects': 'exchange_rate_effect',
            'im_net_cashflow': 'net_cash_increase',
            'cash_cash_equ_beg_period': 'beginning_cash',
            'cash_cash_equ_end_period': 'ending_cash',
            'depr_fa_coga_dpba': 'depreciation',
            'amort_intang_assets': 'amortization',
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
            '经营活动产生的现金流量净额': 'operating_cash_flow',
            '销售商品、提供劳务收到的现金': 'cash_from_sales',
            '收到的税费返还': 'cash_from_interest',
            '收到其他与经营活动有关的现金': 'other_operating_cash',
            '购买商品、接受劳务支付的现金': 'cash_to_suppliers',
            '支付给职工以及为职工支付的现金': 'cash_to_employees',
            '支付的各项税费': 'cash_paid_for_taxes',
            '投资活动产生的现金流量净额': 'investing_cash_flow',
            '收回投资收到的现金': 'cash_from_investments',
            '取得投资收益收到的现金': 'cash_from_returns',
            '处置固定资产收回的现金': 'cash_from_asset_sales',
            '购建固定资产支付的现金': 'cash_for_fixed_assets',
            '投资支付的现金': 'cash_for_investments',
            '筹资活动产生的现金流量净额': 'financing_cash_flow',
            '吸收投资收到的现金': 'cash_from_investors',
            '取得借款收到的现金': 'cash_from_borrowings',
            '发行债券收到的现金': 'cash_from_bonds',
            '偿还债务支付的现金': 'cash_repayment',
            '分配股利、利润或偿付利息支付的现金': 'cash_dividends',
            '汇率变动对现金及现金等价物的影响': 'exchange_rate_effect',
            '现金及现金等价物净增加额': 'net_cash_increase',
            '期初现金及现金等价物余额': 'beginning_cash',
            '期末现金及现金等价物余额': 'ending_cash',
            '固定资产折旧': 'depreciation',
            '无形资产摊销': 'amortization',
        }
        
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        return df


# ==================== 便捷函数 ====================

def fetch_cash_flow(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    report_type: Optional[str] = None,
    tushare_token: Optional[str] = None
) -> pd.DataFrame:
    """获取单只股票现金流量表 (便捷函数)"""
    fetcher = CashFlowFetcher(tushare_token)
    return fetcher.fetch(code, start_date, end_date, report_type)


def fetch_cash_flow_batch(
    codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    delay: float = 0.5,
    tushare_token: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """批量获取现金流量表 (便捷函数)"""
    fetcher = CashFlowFetcher(tushare_token)
    return fetcher.fetch_batch(codes, start_date, end_date, delay)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 获取单只股票现金流量表")
    print("=" * 50)
    
    code = "000001"
    df = fetch_cash_flow(code, start_date="2023-01-01")
    
    if not df.empty:
        print(f"\n成功获取 {code} 现金流量表:")
        print(f"记录数: {len(df)}")
        print(f"\n最新报告期数据:")
        print(df[['code', 'report_date', 'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow']].head(1).to_string())
    else:
        print(f"获取 {code} 现金流量表失败")
