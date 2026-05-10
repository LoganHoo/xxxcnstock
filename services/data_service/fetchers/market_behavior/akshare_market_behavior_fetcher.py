#!/usr/bin/env python3
"""
AKShare 市场行为数据获取器

使用 AKShare 接口获取市场行为数据:
- 龙虎榜数据
- 资金流向数据
- 板块资金流向
"""
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta
from pathlib import Path
import akshare as ak

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("akshare_market_behavior", log_file="system/akshare_market_behavior.log")


class AKShareMarketBehaviorFetcher:
    """AKShare 市场行为数据获取器"""
    
    def fetch_dragon_tiger_list(self, date: str) -> pd.DataFrame:
        """
        获取龙虎榜数据

        Args:
            date: 日期 (YYYY-MM-DD)

        Returns:
            龙虎榜数据 DataFrame
        """
        try:
            logger.info(f"获取 {date} 龙虎榜数据")

            df = ak.stock_lhb_detail_em()

            if df.empty:
                logger.warning(f"{date} 无龙虎榜数据")
                return pd.DataFrame()

            df['上榜日'] = pd.to_datetime(df['上榜日']).dt.strftime('%Y-%m-%d')
            df = df[df['上榜日'] == date]

            if df.empty:
                logger.warning(f"{date} 无龙虎榜数据")
                return pd.DataFrame()

            column_mapping = {
                '序号': 'seq',
                '代码': 'code',
                '名称': 'name',
                '上榜日': 'date',
                '收盘价': 'close',
                '涨跌幅': 'change_pct',
                '龙虎榜净买额': 'net_amount',
                '龙虎榜买入额': 'buy_amount',
                '龙虎榜卖出额': 'sell_amount',
                '龙虎榜成交额': 'total_amount',
                '上榜原因': 'reason',
                '换手率': 'turnover_rate',
                '流通市值': 'market_cap'
            }
            df = df.rename(columns=column_mapping)

            logger.info(f"{date} 获取到 {len(df)} 条龙虎榜记录")
            return df

        except Exception as e:
            logger.error(f"获取 {date} 龙虎榜数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_dragon_tiger_history(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取历史龙虎榜数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            历史龙虎榜数据 DataFrame
        """
        try:
            logger.info(f"获取龙虎榜历史数据: {start_date} 至 {end_date}")
            
            # 生成交易日列表
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_data = []
            current = start
            
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                df = self.fetch_dragon_tiger_list(date_str)
                if not df.empty:
                    all_data.append(df)
                current += timedelta(days=1)
            
            if not all_data:
                logger.warning(f"{start_date} 至 {end_date} 无龙虎榜数据")
                return pd.DataFrame()
            
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"总共获取到 {len(result)} 条龙虎榜记录")
            return result
            
        except Exception as e:
            logger.error(f"获取龙虎榜历史数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_money_flow(self, code: str) -> Optional[dict]:
        """
        获取个股资金流向
        
        Args:
            code: 股票代码
        
        Returns:
            资金流向数据字典
        """
        try:
            logger.info(f"获取 {code} 资金流向")
            
            # 使用 AKShare 获取资金流向
            # 移除后缀
            code_pure = code.split('.')[0] if '.' in code else code
            
            # 获取资金流向
            df = ak.stock_individual_fund_flow(stock=code_pure, market="sh" if code_pure.startswith('6') else "sz")
            
            if df.empty:
                logger.warning(f"{code} 无资金流向数据")
                return None
            
            # 获取最新数据
            latest = df.iloc[0]
            
            return {
                'code': code,
                'date': latest.get('日期'),
                'main_inflow': latest.get('主力净流入-净额'),
                'main_inflow_pct': latest.get('主力净流入-净占比'),
                'retail_inflow': latest.get('散户净流入-净额'),
                'retail_inflow_pct': latest.get('散户净流入-净占比'),
                'total_inflow': latest.get('净流入-净额'),
                'total_inflow_pct': latest.get('净流入-净占比'),
            }
            
        except Exception as e:
            logger.error(f"获取 {code} 资金流向失败: {e}")
            return None

    def fetch_northbound_money_flow(self) -> pd.DataFrame:
        """
        获取北向资金（沪深港通）流向数据

        Returns:
            北向资金流向 DataFrame
        """
        try:
            logger.info("获取北向资金流向数据")

            df = ak.stock_hsgt_fund_flow_summary_em()

            if df.empty:
                logger.warning("无北向资金数据")
                return pd.DataFrame()

            column_mapping = {
                '交易日': 'trade_date',
                '类型': 'type',
                '板块': 'sector',
                '资金方向': 'direction',
                '成交净买额': 'net_buy',
                '资金净流入': 'net_flow',
                '当日资金余额': 'daily_balance',
                '上涨数': 'rise_count',
                '持平数': 'flat_count',
                '下跌数': 'fall_count',
                '相关指数': 'index_name',
                '指数涨跌幅': 'index_change_pct'
            }
            df = df.rename(columns=column_mapping)
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

            logger.info(f"获取到 {len(df)} 条北向资金记录")
            return df

        except Exception as e:
            logger.error(f"获取北向资金流向失败: {e}")
            return pd.DataFrame()

    def fetch_sector_money_flow(self, sector_type: str = "industry") -> pd.DataFrame:
        """
        获取板块资金流向
        
        Args:
            sector_type: 板块类型 (industry/concept/region)
        
        Returns:
            板块资金流向 DataFrame
        """
        try:
            logger.info(f"获取板块资金流向: {sector_type}")
            
            # 使用 AKShare 获取板块资金流向
            if sector_type == "industry":
                df = ak.stock_sector_fund_flow_rank(indicator="今日")
            elif sector_type == "concept":
                df = ak.stock_concept_fund_flow_rank(indicator="今日")
            else:
                df = ak.stock_sector_fund_flow_rank(indicator="今日")
            
            if df.empty:
                logger.warning(f"无板块资金流向数据")
                return pd.DataFrame()
            
            # 标准化列名
            df.columns = ['rank', 'sector', 'change_pct', 'main_inflow', 'main_inflow_pct',
                         'super_large_inflow', 'super_large_pct', 'large_inflow', 'large_pct',
                         'medium_inflow', 'medium_pct', 'small_inflow', 'small_pct',
                         'stock_count', 'leading_stock', 'leading_change', 'total_market']
            
            logger.info(f"获取到 {len(df)} 条板块资金流向记录")
            return df
            
        except Exception as e:
            logger.error(f"获取板块资金流向失败: {e}")
            return pd.DataFrame()


# ==================== 便捷函数 ====================

def fetch_dragon_tiger_list(date: str) -> pd.DataFrame:
    """获取龙虎榜数据 (便捷函数)"""
    fetcher = AKShareMarketBehaviorFetcher()
    return fetcher.fetch_dragon_tiger_list(date)


def fetch_dragon_tiger_history(start_date: str, end_date: str) -> pd.DataFrame:
    """获取历史龙虎榜数据 (便捷函数)"""
    fetcher = AKShareMarketBehaviorFetcher()
    return fetcher.fetch_dragon_tiger_history(start_date, end_date)


def fetch_money_flow(code: str) -> Optional[dict]:
    """获取个股资金流向 (便捷函数)"""
    fetcher = AKShareMarketBehaviorFetcher()
    return fetcher.fetch_money_flow(code)


def fetch_sector_money_flow(sector_type: str = "industry") -> pd.DataFrame:
    """获取板块资金流向 (便捷函数)"""
    fetcher = AKShareMarketBehaviorFetcher()
    return fetcher.fetch_sector_money_flow(sector_type)
