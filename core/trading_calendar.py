"""
交易日判断工具
用于判断当前是否为交易日，以及获取交易日历
"""
from datetime import datetime, timedelta
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class TradingCalendar:
    """交易日历工具类"""
    
    def __init__(self):
        self.cache = {}
        self.cache_date = None
        # 2026年中国法定节假日（根据国务院办公厅通知）
        self.holidays_2026 = {
            # 元旦
            '2026-01-01',
            # 春节
            '2026-02-10', '2026-02-11', '2026-02-12', '2026-02-13', '2026-02-14', '2026-02-15', '2026-02-16',
            # 清明节
            '2026-04-04', '2026-04-05', '2026-04-06',
            # 劳动节
            '2026-05-01', '2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05',
            # 端午节
            '2026-06-14', '2026-06-15', '2026-06-16',
            # 中秋节
            '2026-09-20', '2026-09-21', '2026-09-22',
            # 国庆节
            '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07'
        }
    
    def is_trading_day(self, date: Optional[datetime] = None) -> bool:
        """
        判断是否为交易日
        
        Args:
            date: 日期，默认为今天
            
        Returns:
            bool: 是否为交易日
        """
        if date is None:
            date = datetime.now()
        
        weekday = date.weekday()
        
        # 周末不是交易日
        if weekday >= 5:
            return False
        
        # 法定节假日不是交易日
        date_str = date.strftime('%Y-%m-%d')
        if date_str in self.holidays_2026:
            return False
        
        return True
    
    def is_after_market_close(self, hour: int = 16, minute: int = 0) -> bool:
        """
        判断当前时间是否在收盘后
        
        Args:
            hour: 收盘小时（默认16点）
            minute: 收盘分钟（默认0分）
            
        Returns:
            bool: 是否在收盘后
        """
        now = datetime.now()
        current_time = now.hour * 60 + now.minute
        close_time = hour * 60 + minute
        
        return current_time >= close_time
    
    def should_run_task(self) -> tuple:
        """
        判断是否应该执行任务
        
        Returns:
            tuple: (should_run: bool, reason: str)
        """
        now = datetime.now()
        
        if not self.is_trading_day(now):
            return False, f"非交易日（周{now.weekday() + 1}）"
        
        if not self.is_after_market_close():
            now_time = f"{now.hour:02d}:{now.minute:02d}"
            return False, f"未到收盘时间（当前{now_time}，需16:00后）"
        
        return True, "交易日收盘后，可以执行任务"
    
    def get_recent_trading_days(self, days: int = 30) -> List[str]:
        """
        获取最近N个交易日
        
        Args:
            days: 天数
            
        Returns:
            List[str]: 交易日列表（YYYY-MM-DD格式）
        """
        trading_days = []
        current_date = datetime.now()
        
        while len(trading_days) < days:
            if self.is_trading_day(current_date):
                trading_days.append(current_date.strftime('%Y-%m-%d'))
            current_date -= timedelta(days=1)
        
        return trading_days
    
    def get_last_trading_day(self) -> str:
        """
        获取上一个交易日
        
        Returns:
            str: 上一个交易日（YYYY-MM-DD格式）
        """
        current_date = datetime.now()
        
        if self.is_trading_day(current_date) and not self.is_after_market_close():
            current_date -= timedelta(days=1)
        
        while not self.is_trading_day(current_date):
            current_date -= timedelta(days=1)
        
        return current_date.strftime('%Y-%m-%d')


def get_recent_trade_dates(days: int = 5) -> List[str]:
    """
    获取最近N个交易日日期
    
    Args:
        days: 获取天数，默认5天
        
    Returns:
        List[str]: 最近交易日日期列表 (YYYY-MM-DD格式)
    """
    calendar = TradingCalendar()
    dates = []
    date = datetime.now()
    
    while len(dates) < days:
        if calendar.is_trading_day(date):
            dates.append(date.strftime('%Y-%m-%d'))
        date = date - timedelta(days=1)
    
    return dates


def check_market_status() -> dict:
    """
    检查市场状态
    
    Returns:
        dict: 市场状态信息
    """
    calendar = TradingCalendar()
    now = datetime.now()
    
    is_trading = calendar.is_trading_day(now)
    is_after_close = calendar.is_after_market_close()
    should_run, reason = calendar.should_run_task()
    
    return {
        'current_time': now.strftime('%Y-%m-%d %H:%M:%S'),
        'weekday': ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][now.weekday()],
        'is_trading_day': is_trading,
        'is_after_market_close': is_after_close,
        'should_run_task': should_run,
        'reason': reason,
        'last_trading_day': calendar.get_last_trading_day()
    }


if __name__ == '__main__':
    status = check_market_status()
    
    print("="*70)
    print("市场状态检查")
    print("="*70)
    print(f"当前时间: {status['current_time']} {status['weekday']}")
    print(f"是否交易日: {'是' if status['is_trading_day'] else '否'}")
    print(f"是否收盘后: {'是' if status['is_after_market_close'] else '否'}")
    print(f"应执行任务: {'是' if status['should_run_task'] else '否'}")
    print(f"原因: {status['reason']}")
    print(f"上一交易日: {status['last_trading_day']}")
    print("="*70)
