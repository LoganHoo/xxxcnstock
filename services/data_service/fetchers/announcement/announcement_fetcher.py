#!/usr/bin/env python3
"""
公告数据获取器

支持数据源:
- AKShare: 主要数据源,提供巨潮资讯网公告
- 新浪财经: 备用数据源

公告类型:
- 定期报告: 年报、半年报、季报
- 重大事项: 并购重组、股权激励、增发
- 交易提示: 停牌复牌、除权除息
- 股权变动: 增减持、质押解押
- 经营信息: 合同中标、业绩预告
"""
import pandas as pd
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
import time
import akshare as ak

from core.logger import setup_logger

logger = setup_logger("announcement_fetcher", log_file="system/announcement_fetcher.log")


class AnnouncementType(Enum):
    """公告类型枚举"""
    # 定期报告
    ANNUAL_REPORT = "年报"
    SEMI_ANNUAL_REPORT = "半年报"
    QUARTERLY_REPORT = "季报"
    
    # 重大事项
    MERGER_ACQUISITION = "并购重组"
    EQUITY_INCENTIVE = "股权激励"
    PRIVATE_PLACEMENT = "增发"
    RIGHTS_ISSUE = "配股"
    
    # 交易提示
    TRADING_HALT = "停牌"
    TRADING_RESUME = "复牌"
    EX_DIVIDEND = "除权除息"
    
    # 股权变动
    INCREASE_HOLDING = "增持"
    DECREASE_HOLDING = "减持"
    PLEDGE = "股权质押"
    PLEDGE_RELEASE = "解押"
    
    # 经营信息
    CONTRACT = "重大合同"
    PERFORMANCE_FORECAST = "业绩预告"
    PERFORMANCE_EXPRESS = "业绩快报"
    
    # 其他
    SHAREHOLDING_CHANGE = "股权变动"
    OTHER = "其他"


# 公告类型关键词映射
ANNOUNCEMENT_KEYWORDS = {
    AnnouncementType.ANNUAL_REPORT: ['年度报告', '年报'],
    AnnouncementType.SEMI_ANNUAL_REPORT: ['半年度报告', '半年报'],
    AnnouncementType.QUARTERLY_REPORT: ['季度报告', '季报', '一季报', '三季报'],
    AnnouncementType.MERGER_ACQUISITION: ['重大资产重组', '收购', '合并', '并购'],
    AnnouncementType.EQUITY_INCENTIVE: ['股权激励', '期权激励', '限制性股票'],
    AnnouncementType.PRIVATE_PLACEMENT: ['非公开发行', '定增', '增发'],
    AnnouncementType.RIGHTS_ISSUE: ['配股'],
    AnnouncementType.TRADING_HALT: ['停牌'],
    AnnouncementType.TRADING_RESUME: ['复牌'],
    AnnouncementType.EX_DIVIDEND: ['除权', '除息', '分红', '派息'],
    AnnouncementType.INCREASE_HOLDING: ['增持', '增持股份'],
    AnnouncementType.DECREASE_HOLDING: ['减持', '减持股份'],
    AnnouncementType.PLEDGE: ['质押', '股权质押'],
    AnnouncementType.PLEDGE_RELEASE: ['解除质押', '解押'],
    AnnouncementType.CONTRACT: ['中标', '签订合同', '重大合同'],
    AnnouncementType.PERFORMANCE_FORECAST: ['业绩预告', '业绩预增', '业绩预减'],
    AnnouncementType.PERFORMANCE_EXPRESS: ['业绩快报'],
    AnnouncementType.SHAREHOLDING_CHANGE: ['权益变动', '持股变动'],
}


@dataclass
class AnnouncementData:
    """公告数据模型"""
    # 基本信息
    code: str                           # 股票代码
    name: str                           # 股票名称
    title: str                          # 公告标题
    publish_date: str                   # 发布日期
    
    # 公告内容
    content: str = ""                   # 公告内容摘要
    url: str = ""                       # 公告链接
    
    # 分类信息
    announcement_type: AnnouncementType = AnnouncementType.OTHER  # 公告类型
    category: str = ""                  # 原始分类
    
    # 重要性
    importance: str = "normal"          # 重要性 (high/normal/low)
    
    # 元数据
    source: str = ""
    update_time: str = ""


class AnnouncementFetcher:
    """公告数据获取器"""
    
    def __init__(self):
        self.logger = logger
        self.keywords_map = ANNOUNCEMENT_KEYWORDS
    
    def fetch_stock_announcements(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取单只股票公告
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            公告DataFrame
        """
        try:
            # 移除代码后缀
            code_clean = code.split('.')[0] if '.' in code else code
            
            # 使用AKShare获取公告
            df = ak.stock_notice_report(
                symbol=code_clean,
                date=end_date or datetime.now().strftime('%Y%m%d')
            )
            
            if df.empty:
                self.logger.warning(f"{code} 公告数据为空")
                return df
            
            # 标准化列名
            df = self._standardize_columns(df)
            df['code'] = code_clean
            
            # 日期过滤
            if start_date:
                df = df[df['publish_date'] >= start_date]
            if end_date:
                df = df[df['publish_date'] <= end_date]
            
            # 分类公告
            df['announcement_type'] = df['title'].apply(self._classify_announcement)
            
            # 评估重要性
            df['importance'] = df.apply(self._evaluate_importance, axis=1)
            
            df['source'] = 'akshare'
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.logger.info(f"{code} 获取到 {len(df)} 条公告")
            return df
            
        except Exception as e:
            self.logger.error(f"{code} 公告获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_market_announcements(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page_size: int = 100
    ) -> pd.DataFrame:
        """
        获取市场公告
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            page_size: 每页数量
        
        Returns:
            公告DataFrame
        """
        try:
            # AKShare API 只接受单个 date 参数，需要逐日获取
            all_data = []
            
            if start_date and end_date:
                current = datetime.strptime(start_date, '%Y%m%d')
                end = datetime.strptime(end_date, '%Y%m%d')
            else:
                # 默认获取最近一天
                current = datetime.now()
                end = current
            
            while current <= end:
                date_str = current.strftime('%Y%m%d')
                try:
                    df = ak.stock_notice_report(symbol="全部", date=date_str)
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    self.logger.warning(f"获取 {date_str} 公告数据失败: {e}")
                current += timedelta(days=1)
            
            if not all_data:
                self.logger.warning("未获取到任何公告数据")
                return pd.DataFrame()
            
            df = pd.concat(all_data, ignore_index=True)
            df = self._standardize_columns(df)
            
            # 分类
            df['announcement_type'] = df['title'].apply(self._classify_announcement)
            df['importance'] = df.apply(self._evaluate_importance, axis=1)
            
            df['source'] = 'akshare'
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 限制数量
            df = df.head(page_size)
            
            self.logger.info(f"获取到 {len(df)} 条市场公告")
            return df
            
        except Exception as e:
            self.logger.error(f"市场公告获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_major_events(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取重大事项公告
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            重大事项DataFrame
        """
        try:
            # 获取所有公告
            df = self.fetch_market_announcements(start_date, end_date, page_size=500)
            
            if df.empty:
                return df
            
            # 筛选重大事项类型
            major_types = [
                AnnouncementType.MERGER_ACQUISITION,
                AnnouncementType.EQUITY_INCENTIVE,
                AnnouncementType.PRIVATE_PLACEMENT,
                AnnouncementType.TRADING_HALT,
                AnnouncementType.TRADING_RESUME,
                AnnouncementType.CONTRACT,
                AnnouncementType.PERFORMANCE_FORECAST,
            ]
            
            # 筛选重要公告
            df_major = df[
                (df['announcement_type'].isin([t.value for t in major_types])) |
                (df['importance'] == 'high')
            ]
            
            self.logger.info(f"筛选出 {len(df_major)} 条重大事项")
            return df_major
            
        except Exception as e:
            self.logger.error(f"重大事项获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_trading_hints(
        self,
        trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取交易提示公告
        
        Args:
            trade_date: 交易日期 (YYYYMMDD), None表示最新
        
        Returns:
            交易提示DataFrame
        """
        try:
            if trade_date is None:
                trade_date = datetime.now().strftime('%Y%m%d')
            
            # 获取停牌信息
            halt_df = ak.stock_tfp_em(date=trade_date)
            
            if halt_df.empty:
                return halt_df
            
            # 标准化列名
            column_mapping = {
                '代码': 'code',
                '名称': 'name',
                '停牌时间': 'halt_date',
                '停牌截止时间': 'halt_end_date',
                '停牌原因': 'reason',
                '预计复牌时间': 'expected_resume',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in halt_df.columns}
            halt_df = halt_df.rename(columns=rename_dict)
            
            halt_df['announcement_type'] = AnnouncementType.TRADING_HALT.value
            halt_df['importance'] = 'high'
            halt_df['publish_date'] = trade_date
            halt_df['source'] = 'akshare'
            
            return halt_df
            
        except Exception as e:
            self.logger.error(f"交易提示获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_performance_forecasts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取业绩预告
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            业绩预告DataFrame
        """
        try:
            # 使用AKShare获取业绩预告
            df = ak.stock_yjkb_em(date=end_date or datetime.now().strftime('%Y%m%d'))
            
            if df.empty:
                return df
            
            # 标准化列名
            column_mapping = {
                '序号': 'seq',
                '代码': 'code',
                '名称': 'name',
                '最新价': 'close_price',
                '业绩预告摘要': 'forecast_summary',
                '业绩预告类型': 'forecast_type',
                '业绩预告变动幅度': 'change_range',
                '上年同期净利润': 'last_year_profit',
                '公告日期': 'publish_date',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            df['announcement_type'] = AnnouncementType.PERFORMANCE_FORECAST.value
            df['importance'] = 'high'
            df['source'] = 'akshare'
            
            # 日期过滤
            if start_date:
                df = df[df['publish_date'] >= start_date]
            if end_date:
                df = df[df['publish_date'] <= end_date]
            
            return df
            
        except Exception as e:
            self.logger.error(f"业绩预告获取失败: {e}")
            return pd.DataFrame()
    
    def fetch_equity_changes(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取股权变动公告
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            股权变动DataFrame
        """
        try:
            # 获取增减持数据
            df = ak.stock_gdzc_em()
            
            if df.empty:
                return df
            
            # 标准化列名
            column_mapping = {
                '序号': 'seq',
                '代码': 'code',
                '名称': 'name',
                '最新价': 'close_price',
                '涨跌幅': 'change_pct',
                '股东名称': 'shareholder',
                '变动数量': 'change_volume',
                '变动比例': 'change_ratio',
                '变动开始日期': 'change_start',
                '变动截止日期': 'change_end',
                '公告日期': 'publish_date',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            # 判断是增持还是减持
            df['announcement_type'] = df.apply(
                lambda x: AnnouncementType.INCREASE_HOLDING.value 
                if x.get('change_volume', 0) > 0 
                else AnnouncementType.DECREASE_HOLDING.value,
                axis=1
            )
            
            df['importance'] = 'high'
            df['source'] = 'akshare'
            
            # 日期过滤
            if start_date:
                df = df[df['publish_date'] >= start_date]
            if end_date:
                df = df[df['publish_date'] <= end_date]
            
            return df
            
        except Exception as e:
            self.logger.error(f"股权变动获取失败: {e}")
            return pd.DataFrame()
    
    def _classify_announcement(self, title: str) -> str:
        """根据标题分类公告"""
        if not title:
            return AnnouncementType.OTHER.value
        
        title_lower = title.lower()
        
        for ann_type, keywords in self.keywords_map.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return ann_type.value
        
        return AnnouncementType.OTHER.value
    
    def _evaluate_importance(self, row: pd.Series) -> str:
        """评估公告重要性"""
        title = str(row.get('title', ''))
        ann_type = row.get('announcement_type', AnnouncementType.OTHER.value)
        
        # 重大事项为高重要性
        high_importance_types = [
            AnnouncementType.MERGER_ACQUISITION.value,
            AnnouncementType.EQUITY_INCENTIVE.value,
            AnnouncementType.PRIVATE_PLACEMENT.value,
            AnnouncementType.TRADING_HALT.value,
            AnnouncementType.TRADING_RESUME.value,
            AnnouncementType.PERFORMANCE_FORECAST.value,
        ]
        
        if ann_type in high_importance_types:
            return 'high'
        
        # 关键词判断
        high_keywords = ['重大', '重组', '收购', '合并', '停牌', '复牌', '业绩预增', '业绩预减']
        for keyword in high_keywords:
            if keyword in title:
                return 'high'
        
        return 'normal'
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        column_mapping = {
            '代码': 'code',
            '名称': 'name',
            '公告标题': 'title',
            '公告日期': 'publish_date',
            '公告内容': 'content',
            '公告链接': 'url',
            '分类': 'category',
        }
        
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        return df.rename(columns=rename_dict)


# ==================== 便捷函数 ====================

def fetch_announcements(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """获取股票公告 (便捷函数)"""
    fetcher = AnnouncementFetcher()
    return fetcher.fetch_stock_announcements(code, start_date, end_date)


def fetch_major_events(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """获取重大事项 (便捷函数)"""
    fetcher = AnnouncementFetcher()
    return fetcher.fetch_major_events(start_date, end_date)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 公告数据获取器")
    print("=" * 50)
    
    fetcher = AnnouncementFetcher()
    
    # 测试个股公告
    print("\n1. 获取个股公告:")
    df = fetcher.fetch_stock_announcements("000001", page_size=10)
    if not df.empty:
        print(f"获取到 {len(df)} 条公告")
        print(df[['code', 'title', 'announcement_type', 'importance']].head().to_string())
    
    # 测试重大事项
    print("\n2. 获取重大事项:")
    major_df = fetcher.fetch_major_events(page_size=10)
    if not major_df.empty:
        print(f"获取到 {len(major_df)} 条重大事项")
        print(major_df[['code', 'title', 'announcement_type']].head().to_string())
    
    # 测试业绩预告
    print("\n3. 获取业绩预告:")
    forecast_df = fetcher.fetch_performance_forecasts()
    if not forecast_df.empty:
        print(f"获取到 {len(forecast_df)} 条业绩预告")
        print(forecast_df[['code', 'name', 'forecast_type', 'change_range']].head().to_string())
