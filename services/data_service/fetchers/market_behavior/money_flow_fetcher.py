#!/usr/bin/env python3
"""
资金流向数据获取器

支持数据源:
- AKShare: 主要数据源,提供个股和行业资金流向
- Sina Finance: 备用数据源

数据内容:
- 主力资金流向(超大单+大单)
- 散户资金流向(中单+小单)
- 行业/板块资金流向
- 北向资金流向
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, date
import time
import akshare as ak

from core.logger import setup_logger

logger = setup_logger("money_flow_fetcher", log_file="system/money_flow_fetcher.log")


@dataclass
class MoneyFlowData:
    """资金流向数据模型"""
    # 基本信息
    code: str                           # 股票代码
    name: str                           # 股票名称
    trade_date: str                     # 交易日期
    
    # 价格数据
    close_price: Optional[float] = None # 收盘价
    change_pct: Optional[float] = None  # 涨跌幅(%)
    turnover_rate: Optional[float] = None  # 换手率(%)
    
    # 主力资金
    main_inflow: Optional[float] = None     # 主力流入(万元)
    main_outflow: Optional[float] = None    # 主力流出(万元)
    main_net_flow: Optional[float] = None   # 主力净流入(万元)
    main_net_ratio: Optional[float] = None  # 主力净流入占比(%)
    
    # 散户资金
    retail_inflow: Optional[float] = None   # 散户流入(万元)
    retail_outflow: Optional[float] = None  # 散户流出(万元)
    retail_net_flow: Optional[float] = None # 散户净流入(万元)
    retail_net_ratio: Optional[float] = None # 散户净流入占比(%)
    
    # 大单统计
    large_buy: Optional[float] = None       # 大单买入(万元)
    large_sell: Optional[float] = None      # 大单卖出(万元)
    large_net: Optional[float] = None       # 大单净额(万元)
    
    # 超大单统计
    super_large_buy: Optional[float] = None     # 超大单买入(万元)
    super_large_sell: Optional[float] = None    # 超大单卖出(万元)
    super_large_net: Optional[float] = None     # 超大单净额(万元)
    
    # 总成交额
    total_amount: Optional[float] = None    # 总成交额(万元)
    
    # 元数据
    source: str = ""
    update_time: str = ""


class MoneyFlowFetcher:
    """资金流向数据获取器"""
    
    def __init__(self):
        self.logger = logger
    
    def fetch_stock_money_flow(
        self,
        code: str,
        market: str = "sh"
    ) -> Optional[MoneyFlowData]:
        """
        获取单只股票资金流向
        
        Args:
            code: 股票代码
            market: 市场 (sh/sz)
        
        Returns:
            MoneyFlowData对象
        """
        try:
            # 移除代码后缀
            code_clean = code.split('.')[0] if '.' in code else code
            
            # 使用AKShare获取资金流向
            df = ak.stock_individual_fund_flow(stock=code_clean, market=market)
            
            if df.empty:
                self.logger.warning(f"{code} 资金流向数据为空")
                return None
            
            # 获取最新日期数据
            latest = df.iloc[0]
            trade_date = latest.get('日期', datetime.now().strftime('%Y-%m-%d'))
            
            data = MoneyFlowData(
                code=code_clean,
                name=latest.get('名称', ''),
                trade_date=trade_date,
                source='akshare',
                update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            
            # 解析资金流向数据
            data.close_price = self._parse_amount(latest.get('收盘价'))
            data.change_pct = self._parse_amount(latest.get('涨跌幅'))
            
            # 主力净流入
            data.main_net_flow = self._parse_amount(latest.get('主力净流入-净额'))
            data.main_net_ratio = self._parse_amount(latest.get('主力净流入-净占比'))
            
            # 超大单
            data.super_large_net = self._parse_amount(latest.get('超大单净流入-净额'))
            
            # 大单
            data.large_net = self._parse_amount(latest.get('大单净流入-净额'))
            
            # 中单(散户)
            data.retail_net_flow = self._parse_amount(latest.get('中单净流入-净额'))
            data.retail_net_ratio = self._parse_amount(latest.get('中单净流入-净占比'))
            
            # 小单(散户)
            small_net = self._parse_amount(latest.get('小单净流入-净额'))
            
            # 合并散户资金
            if data.retail_net_flow is not None and small_net is not None:
                data.retail_net_flow += small_net
            
            # 计算主力流入流出
            if data.main_net_flow is not None:
                # 估算: 净流入为正时,流入=净额+流出,流出为固定比例
                # 这里简化处理
                data.main_inflow = max(data.main_net_flow, 0)
                data.main_outflow = max(-data.main_net_flow, 0)
            
            self.logger.info(f"{code} 资金流向数据获取成功")
            return data
            
        except Exception as e:
            self.logger.error(f"{code} 资金流向获取失败: {e}")
            return None
    
    def fetch_stock_money_flow_hist(
        self,
        code: str,
        market: str = "sh",
        days: int = 30
    ) -> pd.DataFrame:
        """
        获取股票历史资金流向
        
        Args:
            code: 股票代码
            market: 市场
            days: 天数
        
        Returns:
            历史资金流向DataFrame
        """
        try:
            code_clean = code.split('.')[0] if '.' in code else code
            df = ak.stock_individual_fund_flow(stock=code_clean, market=market)
            
            if df.empty:
                return df
            
            # 标准化列名
            column_mapping = {
                '日期': 'trade_date',
                '收盘价': 'close_price',
                '涨跌幅': 'change_pct',
                '主力净流入-净额': 'main_net_flow',
                '主力净流入-净占比': 'main_net_ratio',
                '超大单净流入-净额': 'super_large_net',
                '超大单净流入-净占比': 'super_large_ratio',
                '大单净流入-净额': 'large_net',
                '大单净流入-净占比': 'large_ratio',
                '中单净流入-净额': 'medium_net',
                '中单净流入-净占比': 'medium_ratio',
                '小单净流入-净额': 'small_net',
                '小单净流入-净占比': 'small_ratio',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            # 添加代码
            df['code'] = code_clean
            
            # 限制天数
            df = df.head(days)
            
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 历史资金流向获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_sector_money_flow(self, sector_type: str = "industry") -> pd.DataFrame:
        """
        获取板块资金流向
        
        Args:
            sector_type: 板块类型 (industry/concept/region)
        
        Returns:
            板块资金流向DataFrame
        """
        try:
            if sector_type == "industry":
                df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
            elif sector_type == "concept":
                df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="概念资金流")
            elif sector_type == "region":
                df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="地域资金流")
            else:
                df = ak.stock_sector_fund_flow_rank()
            
            if df.empty:
                return df
            
            # 标准化列名
            column_mapping = {
                '名称': 'sector_name',
                '今日涨跌幅': 'change_pct',
                '今日主力净流入-净额': 'main_net_flow',
                '今日主力净流入-净占比': 'main_net_ratio',
                '今日超大单净流入-净额': 'super_large_net',
                '今日超大单净流入-净占比': 'super_large_ratio',
                '今日大单净流入-净额': 'large_net',
                '今日大单净流入-净占比': 'large_ratio',
                '今日中单净流入-净额': 'medium_net',
                '今日小单净流入-净额': 'small_net',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            df['sector_type'] = sector_type
            df['trade_date'] = datetime.now().strftime('%Y-%m-%d')
            
            return df
            
        except Exception as e:
            self.logger.error(f"板块资金流向获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_northbound_money_flow(self) -> pd.DataFrame:
        """
        获取北向资金流向
        
        Returns:
            北向资金流向DataFrame
        """
        try:
            # 获取沪股通和深股通数据
            df_sh = ak.stock_hsgt_hist_em(symbol="沪股通")
            df_sz = ak.stock_hsgt_hist_em(symbol="深股通")
            
            results = []
            
            for df, market in [(df_sh, 'sh'), (df_sz, 'sz')]:
                if df.empty:
                    continue
                
                # 标准化列名
                column_mapping = {
                    '日期': 'trade_date',
                    '当日资金流入': 'daily_inflow',
                    '当日余额': 'daily_balance',
                    '历史资金累计流入': 'cumulative_inflow',
                    '当日成交净买额': 'net_buy',
                    '买入成交额': 'buy_amount',
                    '卖出成交额': 'sell_amount',
                }
                
                rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
                df = df.rename(columns=rename_dict)
                df['market'] = market
                results.append(df)
            
            if results:
                return pd.concat(results, ignore_index=True)
            
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"北向资金流向获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_northbound_holdings(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        获取北向资金持股数据
        
        Args:
            date: 日期 (YYYYMMDD), None表示最新
        
        Returns:
            北向持股DataFrame
        """
        try:
            if date:
                df = ak.stock_hsgt_hold_stock_em(date=date)
            else:
                df = ak.stock_hsgt_hold_stock_em()
            
            if df.empty:
                return df
            
            # 标准化列名
            column_mapping = {
                '序号': 'seq',
                '代码': 'code',
                '名称': 'name',
                '持股日期': 'hold_date',
                '持股数量': 'hold_volume',
                '持股占比': 'hold_ratio',
                '当日收盘价': 'close_price',
                '当日涨跌幅': 'change_pct',
                '持股市值': 'hold_value',
                '持股数量占A股百分比': 'hold_ratio_a',
                '增持股数': 'increase_volume',
                '增持市值': 'increase_value',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            return df.rename(columns=rename_dict)
            
        except Exception as e:
            self.logger.error(f"北向持股数据获取失败: {e}")
            return pd.DataFrame()
    
    def _parse_amount(self, value) -> Optional[float]:
        """解析金额数据"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # 处理带单位的字符串
            value = value.replace(',', '').replace('万', '').replace('亿', '')
            try:
                return float(value)
            except ValueError:
                return None
        return None


# ==================== 便捷函数 ====================

def fetch_money_flow(code: str, market: str = "sh") -> Optional[MoneyFlowData]:
    """获取单只股票资金流向 (便捷函数)"""
    fetcher = MoneyFlowFetcher()
    return fetcher.fetch_stock_money_flow(code, market)


def fetch_sector_money_flow(sector_type: str = "industry") -> pd.DataFrame:
    """获取板块资金流向 (便捷函数)"""
    fetcher = MoneyFlowFetcher()
    return fetcher.fetch_sector_money_flow(sector_type)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 资金流向数据获取器")
    print("=" * 50)
    
    fetcher = MoneyFlowFetcher()
    
    # 测试个股资金流向
    print("\n1. 获取个股资金流向:")
    data = fetcher.fetch_stock_money_flow("000001")
    if data:
        print(f"代码: {data.code}")
        print(f"日期: {data.trade_date}")
        print(f"收盘价: {data.close_price}")
        print(f"主力净流入: {data.main_net_flow} 万元")
        print(f"主力净流入占比: {data.main_net_ratio}%")
        print(f"散户净流入: {data.retail_net_flow} 万元")
    
    # 测试板块资金流向
    print("\n2. 获取行业板块资金流向:")
    sector_df = fetcher.fetch_sector_money_flow("industry")
    if not sector_df.empty:
        print(f"获取到 {len(sector_df)} 个行业")
        print(sector_df[['sector_name', 'change_pct', 'main_net_flow']].head().to_string())
    
    # 测试北向资金
    print("\n3. 获取北向资金流向:")
    north_df = fetcher.fetch_northbound_money_flow()
    if not north_df.empty:
        print(north_df.head().to_string())
