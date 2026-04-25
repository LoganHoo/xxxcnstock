#!/usr/bin/env python3
"""
AKShare 公告数据获取器

使用 AKShare 接口获取公告数据:
- 公司公告
- 定期报告
- 重大事项
- 融资公告
- 风险提示
- 资产重组
- 信息变更
- 持股变动
"""
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta
from pathlib import Path
import akshare as ak

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("akshare_announcement", log_file="system/akshare_announcement.log")


# 公告类型映射
ANNOUNCEMENT_TYPES = {
    "全部": "全部",
    "重大事项": "重大事项",
    "财务报告": "财务报告",
    "融资公告": "融资公告",
    "风险提示": "风险提示",
    "资产重组": "资产重组",
    "信息变更": "信息变更",
    "持股变动": "持股变动"
}


class AKShareAnnouncementFetcher:
    """AKShare 公告数据获取器"""
    
    def fetch_announcements(self, date: str, symbol: str = "全部") -> pd.DataFrame:
        """
        获取公告数据
        
        Args:
            date: 日期 (YYYYMMDD)
            symbol: 公告类型 (全部/重大事项/财务报告/融资公告/风险提示/资产重组/信息变更/持股变动)
        
        Returns:
            公告数据 DataFrame
        """
        try:
            logger.info(f"获取 {date} {symbol} 公告数据")
            
            # 格式化日期
            date_str = date.replace('-', '')
            
            # 获取公告数据
            df = ak.stock_notice_report(symbol=symbol, date=date_str)
            
            if df.empty:
                logger.warning(f"{date} 无{symbol}公告数据")
                return pd.DataFrame()
            
            # 标准化列名
            column_mapping = {
                '代码': 'code',
                '名称': 'name',
                '公告标题': 'title',
                '公告类型': 'type',
                '公告日期': 'date',
                '网址': 'url'
            }
            df = df.rename(columns=column_mapping)
            
            logger.info(f"{date} 获取到 {len(df)} 条{symbol}公告记录")
            return df
            
        except Exception as e:
            logger.error(f"获取 {date} {symbol} 公告数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_company_announcement(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定公司的公告
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            公司公告 DataFrame
        """
        try:
            logger.info(f"获取 {code} 公告数据: {start_date} 至 {end_date}")
            
            # 生成日期列表
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_data = []
            current = start
            
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                df = self.fetch_announcements(date_str, symbol="全部")
                if not df.empty:
                    # 过滤指定公司
                    df = df[df['code'] == code]
                    if not df.empty:
                        all_data.append(df)
                current += timedelta(days=1)
            
            if not all_data:
                logger.warning(f"{code} 在 {start_date} 至 {end_date} 无公告数据")
                return pd.DataFrame()
            
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"{code} 总共获取到 {len(result)} 条公告记录")
            return result
            
        except Exception as e:
            logger.error(f"获取 {code} 公告数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_periodic_reports(self, date: str) -> pd.DataFrame:
        """
        获取定期报告公告
        
        Args:
            date: 日期 (YYYY-MM-DD)
        
        Returns:
            定期报告公告 DataFrame
        """
        return self.fetch_announcements(date, symbol="财务报告")
    
    def fetch_major_events(self, date: str) -> pd.DataFrame:
        """
        获取重大事项公告
        
        Args:
            date: 日期 (YYYY-MM-DD)
        
        Returns:
            重大事项公告 DataFrame
        """
        return self.fetch_announcements(date, symbol="重大事项")
    
    def fetch_financing_announcements(self, date: str) -> pd.DataFrame:
        """
        获取融资公告
        
        Args:
            date: 日期 (YYYY-MM-DD)
        
        Returns:
            融资公告 DataFrame
        """
        return self.fetch_announcements(date, symbol="融资公告")
    
    def fetch_risk_warnings(self, date: str) -> pd.DataFrame:
        """
        获取风险提示公告
        
        Args:
            date: 日期 (YYYY-MM-DD)
        
        Returns:
            风险提示公告 DataFrame
        """
        return self.fetch_announcements(date, symbol="风险提示")
    
    def fetch_announcement_history(self, start_date: str, end_date: str, symbol: str = "全部") -> pd.DataFrame:
        """
        获取历史公告数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            symbol: 公告类型
        
        Returns:
            历史公告数据 DataFrame
        """
        try:
            logger.info(f"获取历史公告数据: {start_date} 至 {end_date}, 类型: {symbol}")
            
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_data = []
            current = start
            
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                df = self.fetch_announcements(date_str, symbol=symbol)
                if not df.empty:
                    all_data.append(df)
                current += timedelta(days=1)
            
            if not all_data:
                logger.warning(f"{start_date} 至 {end_date} 无{symbol}公告数据")
                return pd.DataFrame()
            
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"总共获取到 {len(result)} 条{symbol}公告记录")
            return result
            
        except Exception as e:
            logger.error(f"获取历史公告数据失败: {e}")
            return pd.DataFrame()


# ==================== 便捷函数 ====================

def fetch_announcements(date: str, symbol: str = "全部") -> pd.DataFrame:
    """获取公告数据 (便捷函数)"""
    fetcher = AKShareAnnouncementFetcher()
    return fetcher.fetch_announcements(date, symbol)


def fetch_company_announcement(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取指定公司公告 (便捷函数)"""
    fetcher = AKShareAnnouncementFetcher()
    return fetcher.fetch_company_announcement(code, start_date, end_date)


def fetch_periodic_reports(date: str) -> pd.DataFrame:
    """获取定期报告 (便捷函数)"""
    fetcher = AKShareAnnouncementFetcher()
    return fetcher.fetch_periodic_reports(date)


def fetch_major_events(date: str) -> pd.DataFrame:
    """获取重大事项 (便捷函数)"""
    fetcher = AKShareAnnouncementFetcher()
    return fetcher.fetch_major_events(date)


def fetch_announcement_history(start_date: str, end_date: str, symbol: str = "全部") -> pd.DataFrame:
    """获取历史公告数据 (便捷函数)"""
    fetcher = AKShareAnnouncementFetcher()
    return fetcher.fetch_announcement_history(start_date, end_date, symbol)
