#!/usr/bin/env python3
"""
龙虎榜数据获取器

支持数据源:
- AKShare: 主要数据源,提供龙虎榜详情
- Tushare Pro: 备用数据源

数据内容:
- 上榜股票基本信息
- 买卖金额统计
- 营业部买卖明细(Top5)
- 机构席位识别
"""
import pandas as pd
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
import time
import akshare as ak

from core.logger import setup_logger

logger = setup_logger("dragon_tiger_fetcher", log_file="system/dragon_tiger_fetcher.log")


# 知名游资营业部列表
HOT_MONEY_SEATS = {
    '东方财富证券拉萨金融城南环路': '拉萨帮',
    '东方财富证券拉萨团结路第一': '拉萨帮',
    '东方财富证券拉萨团结路第二': '拉萨帮',
    '东方财富证券拉萨东环路第一': '拉萨帮',
    '东方财富证券拉萨东环路第二': '拉萨帮',
    '国泰君安证券南京太平南路': '作手新一',
    '中信证券上海溧阳路': '孙哥',
    '中信证券上海瑞金南路': '孙哥',
    '中国银河证券绍兴': '赵老哥',
    '浙商证券绍兴分公司': '赵老哥',
    '华泰证券深圳益田路': '欢乐海岸',
    '中信证券深圳总部': '欢乐海岸',
    '国金证券上海奉贤区金碧路': '章盟主',
    '国泰君安证券上海江苏路': '章盟主',
    '中信证券杭州延安路': '章盟主',
    '华鑫证券上海宛平南路': '炒股养家',
    '华鑫证券宁波沧海路': '炒股养家',
    '招商证券深圳深南东路': '深圳帮',
    '国信证券深圳泰然九路': '深圳帮',
    '平安证券深圳深南东路': '深圳帮',
    '西藏东方财富证券江苏分公司': '小鳄鱼',
    '南京证券南京大钟亭': '小鳄鱼',
    '中国中金财富证券北京宋庄路': '北京帮',
    '国泰君安证券北京光华路': '北京帮',
    '中信证券北京总部': '北京帮',
    '华鑫证券上海茅台路': '上海超短帮',
    '华鑫证券上海淞滨路': '上海超短帮',
}


@dataclass
class DragonTigerData:
    """龙虎榜数据模型"""
    # 基本信息
    code: str                           # 股票代码
    name: str                           # 股票名称
    trade_date: str                     # 交易日期
    reason: str                         # 上榜原因
    
    # 价格数据
    close_price: Optional[float] = None # 收盘价
    change_pct: Optional[float] = None  # 涨跌幅(%)
    turnover_rate: Optional[float] = None  # 换手率(%)
    
    # 买卖金额(万元)
    buy_amount: Optional[float] = None  # 买入总额
    sell_amount: Optional[float] = None # 卖出总额
    net_amount: Optional[float] = None  # 净额
    total_amount: Optional[float] = None # 总成交额
    
    # 买卖明细
    buy_details: List[Dict] = field(default_factory=list)   # 买入营业部明细
    sell_details: List[Dict] = field(default_factory=list)  # 卖出营业部明细
    
    # 机构统计
    institution_buy: Optional[float] = None     # 机构买入金额
    institution_sell: Optional[float] = None    # 机构卖出金额
    institution_net: Optional[float] = None     # 机构净额
    institution_count: int = 0                  # 机构数量
    
    # 游资统计
    hot_money_seats: List[str] = field(default_factory=list)  # 知名游资营业部
    hot_money_buy: Optional[float] = None       # 游资买入金额
    
    # 元数据
    source: str = ""
    update_time: str = ""


class DragonTigerFetcher:
    """龙虎榜数据获取器"""
    
    def __init__(self):
        self.logger = logger
        self.hot_money_seats = HOT_MONEY_SEATS
    
    def fetch_daily_list(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取每日龙虎榜列表
        
        Args:
            trade_date: 交易日期 (YYYYMMDD), None表示最新
        
        Returns:
            龙虎榜列表DataFrame
        """
        try:
            if trade_date:
                # AKShare API 只接受 date 参数
                df = ak.stock_lhb_detail_daily_sina(date=trade_date)
            else:
                # 获取最近一个交易日
                df = ak.stock_lhb_detail_daily_sina()
            
            if df.empty:
                self.logger.warning(f"{trade_date or '最新'} 龙虎榜列表为空")
                return df
            
            # 标准化列名
            df = self._standardize_list_columns(df)
            df['source'] = 'akshare'
            
            self.logger.info(f"获取到 {len(df)} 条龙虎榜记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取龙虎榜列表失败: {e}")
            return pd.DataFrame()
    
    def fetch_stock_detail(
        self,
        code: str,
        trade_date: str
    ) -> Optional[DragonTigerData]:
        """
        获取单只股票龙虎榜详情
        
        Args:
            code: 股票代码
            trade_date: 交易日期 (YYYYMMDD)
        
        Returns:
            DragonTigerData对象
        """
        try:
            # 移除代码后缀
            code_clean = code.split('.')[0] if '.' in code else code
            
            # 获取买卖详情
            buy_df = ak.stock_lhb_detail_3days_sina(symbol=code_clean, trade_date=trade_date, type_="buy")
            sell_df = ak.stock_lhb_detail_3days_sina(symbol=code_clean, trade_date=trade_date, type_="sell")
            
            # 获取机构专用数据
            institution_df = ak.stock_lhb_jgmmtj_em(start_date=trade_date, end_date=trade_date)
            
            # 构造数据对象
            data = DragonTigerData(
                code=code_clean,
                name="",
                trade_date=trade_date,
                reason="",
                source="akshare",
                update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            
            # 解析买入明细
            if not buy_df.empty:
                data.buy_details = self._parse_seat_details(buy_df, 'buy')
                data.buy_amount = buy_df['买入金额(万元)'].sum() if '买入金额(万元)' in buy_df.columns else 0
            
            # 解析卖出明细
            if not sell_df.empty:
                data.sell_details = self._parse_seat_details(sell_df, 'sell')
                data.sell_amount = sell_df['卖出金额(万元)'].sum() if '卖出金额(万元)' in sell_df.columns else 0
            
            # 计算净额
            if data.buy_amount and data.sell_amount:
                data.net_amount = data.buy_amount - data.sell_amount
            
            # 识别机构和游资
            self._identify_seats(data)
            
            self.logger.info(f"{code} {trade_date} 龙虎榜详情获取成功")
            return data
            
        except Exception as e:
            self.logger.error(f"{code} {trade_date} 龙虎榜详情获取失败: {e}")
            return None
    
    def fetch_date_range(
        self,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取日期范围内的龙虎榜数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            龙虎榜DataFrame
        """
        try:
            # AKShare API 只接受单个 date 参数，需要逐日获取
            from datetime import datetime, timedelta
            
            all_data = []
            current = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')
            
            while current <= end:
                date_str = current.strftime('%Y%m%d')
                try:
                    df = ak.stock_lhb_detail_daily_sina(date=date_str)
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    self.logger.warning(f"获取 {date_str} 龙虎榜数据失败: {e}")
                current += timedelta(days=1)
            
            if not all_data:
                self.logger.warning(f"{start_date}-{end_date} 龙虎榜数据为空")
                return pd.DataFrame()
            
            df = pd.concat(all_data, ignore_index=True)
            df = self._standardize_list_columns(df)
            df['source'] = 'akshare'
            
            self.logger.info(f"获取到 {len(df)} 条龙虎榜记录")
            return df
            
        except Exception as e:
            self.logger.error(f"获取龙虎榜数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_institution_trading(
        self,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取机构专用交易数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            机构交易DataFrame
        """
        try:
            df = ak.stock_lhb_jgmmtj_em(start_date=start_date, end_date=end_date)
            
            if df.empty:
                return df
            
            # 标准化列名
            column_mapping = {
                '代码': 'code',
                '名称': 'name',
                '上榜日期': 'trade_date',
                '收盘价': 'close_price',
                '涨跌幅': 'change_pct',
                '龙虎榜成交额': 'total_amount',
                '龙虎榜买入额': 'buy_amount',
                '龙虎榜卖出额': 'sell_amount',
                '机构买入净额': 'institution_net',
                '机构买入额': 'institution_buy',
                '机构卖出额': 'institution_sell',
                '机构买入次数': 'institution_buy_count',
                '机构卖出次数': 'institution_sell_count',
            }
            
            rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取机构交易数据失败: {e}")
            return pd.DataFrame()
    
    def _parse_seat_details(self, df: pd.DataFrame, direction: str) -> List[Dict]:
        """解析营业部明细"""
        details = []
        
        for _, row in df.iterrows():
            seat_name = row.get('营业部名称', '')
            
            detail = {
                'seat_name': seat_name,
                'direction': direction,
            }
            
            # 买入金额
            if '买入金额(万元)' in row:
                detail['amount'] = row['买入金额(万元)']
            elif '卖出金额(万元)' in row:
                detail['amount'] = row['卖出金额(万元)']
            
            # 占比
            if '占总成交比例' in row:
                detail['proportion'] = row['占总成交比例']
            
            details.append(detail)
        
        return details
    
    def _identify_seats(self, data: DragonTigerData):
        """识别机构和游资席位"""
        institution_buy = 0
        institution_sell = 0
        institution_count = 0
        
        hot_money_buy = 0
        hot_money_seats = []
        
        # 分析买入席位
        for detail in data.buy_details:
            seat_name = detail.get('seat_name', '')
            amount = detail.get('amount', 0)
            
            # 识别机构专用
            if '机构专用' in seat_name:
                institution_buy += amount
                institution_count += 1
            
            # 识别知名游资
            for hm_seat, hm_name in self.hot_money_seats.items():
                if hm_seat in seat_name:
                    hot_money_buy += amount
                    if hm_name not in hot_money_seats:
                        hot_money_seats.append(hm_name)
                    break
        
        # 分析卖出席位
        for detail in data.sell_details:
            seat_name = detail.get('seat_name', '')
            amount = detail.get('amount', 0)
            
            if '机构专用' in seat_name:
                institution_sell += amount
                institution_count += 1
        
        # 更新数据
        data.institution_buy = institution_buy
        data.institution_sell = institution_sell
        data.institution_net = institution_buy - institution_sell
        data.institution_count = institution_count
        data.hot_money_buy = hot_money_buy
        data.hot_money_seats = hot_money_seats
    
    def _standardize_list_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列表列名"""
        column_mapping = {
            '代码': 'code',
            '名称': 'name',
            '上榜日期': 'trade_date',
            '上榜原因': 'reason',
            '收盘价': 'close_price',
            '对应值': 'change_pct',
            '成交额': 'turnover',
            '成交量': 'volume',
        }
        
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        return df.rename(columns=rename_dict)


# ==================== 便捷函数 ====================

def fetch_dragon_tiger(trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    获取龙虎榜数据 (便捷函数)
    
    Args:
        trade_date: 交易日期 (YYYYMMDD), None表示最新
    
    Returns:
        龙虎榜DataFrame
    """
    fetcher = DragonTigerFetcher()
    return fetcher.fetch_daily_list(trade_date)


def fetch_dragon_tiger_history(
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取历史龙虎榜数据 (便捷函数)
    
    Args:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    
    Returns:
        龙虎榜DataFrame
    """
    fetcher = DragonTigerFetcher()
    return fetcher.fetch_date_range(start_date, end_date)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 龙虎榜数据获取器")
    print("=" * 50)
    
    fetcher = DragonTigerFetcher()
    
    # 测试获取最新龙虎榜
    print("\n1. 获取最新龙虎榜列表:")
    df = fetcher.fetch_daily_list()
    if not df.empty:
        print(f"获取到 {len(df)} 条记录")
        print(df[['code', 'name', 'trade_date', 'reason']].head().to_string())
    
    # 测试获取详情
    print("\n2. 获取单只股票龙虎榜详情:")
    if not df.empty:
        test_code = df.iloc[0]['code']
        test_date = df.iloc[0]['trade_date']
        detail = fetcher.fetch_stock_detail(test_code, test_date)
        if detail:
            print(f"代码: {detail.code}")
            print(f"日期: {detail.trade_date}")
            print(f"买入金额: {detail.buy_amount} 万元")
            print(f"卖出金额: {detail.sell_amount} 万元")
            print(f"净额: {detail.net_amount} 万元")
            print(f"机构买入: {detail.institution_buy} 万元")
            print(f"游资: {detail.hot_money_seats}")
