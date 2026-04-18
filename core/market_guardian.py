#!/usr/bin/env python3
"""
K线数据采集守护模块 - Market Guardian

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
核心铁律：
  交易日盘中（9:30-15:00）禁止采集当日K线数据

区分原则：
  - 当日数据（数据日期 == 今天）：必须收盘后采集
  - 历史数据（数据日期 < 今天）：任何时间都可采集

执行策略：
  - 盘中采集当日数据 → sys.exit(1) 强制终止
  - 历史数据不受时间限制
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import sys
import os
from datetime import datetime, time
from typing import Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class MarketGuardian:
    """
    K线数据采集守护者

    职责：
    1. 区分当日数据 vs 历史数据
    2. 当日数据在盘中时段强制终止程序 (sys.exit)
    3. 历史数据不受时间限制
    4. 记录所有检查日志用于审计

    使用场景：
        enforce_market_closed(target_date=today)      # 当日数据检查
        enforce_market_closed(target_date=history)    # 历史数据随时通过
        enforce_market_closed(force_date='2026-04-15') # 强制模式
    """
    
    # 2026年中国法定节假日
    HOLIDAYS_2026 = {
        '2026-01-01',  # 元旦
        '2026-02-10', '2026-02-11', '2026-02-12', '2026-02-13', 
        '2026-02-14', '2026-02-15', '2026-02-16',  # 春节
        '2026-04-04', '2026-04-05', '2026-04-06',  # 清明节
        '2026-05-01', '2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05',  # 劳动节
        '2026-06-14', '2026-06-15', '2026-06-16',  # 端午节
        '2026-09-20', '2026-09-21', '2026-09-22',  # 中秋节
        '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', 
        '2026-10-05', '2026-10-06', '2026-10-07',  # 国庆节
    }
    
    # 市场时间配置 (分钟)
    MARKET_OPEN = 9 * 60 + 30   # 9:30
    MARKET_CLOSE = 15 * 60       # 15:00
    ALLOW_COLLECT_AFTER = 15 * 60 + 30  # 15:30
    
    @classmethod
    def is_trading_day(cls, date: Optional[datetime] = None) -> bool:
        """判断是否为交易日"""
        if date is None:
            date = datetime.now()
        
        # 周末检查
        if date.weekday() >= 5:
            return False
        
        # 节假日检查
        date_str = date.strftime('%Y-%m-%d')
        if date_str in cls.HOLIDAYS_2026:
            return False
        
        return True
    
    @classmethod
    def get_current_time_minutes(cls) -> int:
        """获取当前时间的分钟数"""
        now = datetime.now()
        return now.hour * 60 + now.minute
    
    @classmethod
    def is_intraday(cls) -> bool:
        """
        判断当前是否在盘中时段 (9:30-15:00)
        
        Returns:
            bool: 是否在盘中
        """
        # 首先检查是否是交易日
        if not cls.is_trading_day():
            return False
        
        current_minutes = cls.get_current_time_minutes()
        
        # 9:30 <= time < 15:00 为盘中
        return cls.MARKET_OPEN <= current_minutes < cls.MARKET_CLOSE
    
    @classmethod
    def is_after_market_close(cls, buffer_minutes: int = 30) -> bool:
        """
        判断当前是否在收盘后（带缓冲）
        
        Args:
            buffer_minutes: 收盘后缓冲时间（默认30分钟）
        
        Returns:
            bool: 是否可以采集
        """
        if not cls.is_trading_day():
            return True  # 非交易日可以采集
        
        current_minutes = cls.get_current_time_minutes()
        
        # 15:00 + buffer 分钟后允许采集
        return current_minutes >= (cls.MARKET_CLOSE + buffer_minutes)
    
    @classmethod
    def check_collection_allowed(cls, target_date: Optional[str] = None, force_date: Optional[str] = None) -> Tuple[bool, str]:
        """
        检查是否允许采集
        
        核心规则：交易日盘中（9:30-15:00）禁止采集**当日**K线数据
        历史数据补采不受时间限制
        
        Args:
            target_date: 目标采集日期（默认今天）
            force_date: 强制指定日期（用于补采历史数据）
        
        Returns:
            Tuple[bool, str]: (是否允许, 原因)
        """
        now = datetime.now()
        current_time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        today_str = now.strftime('%Y-%m-%d')
        
        # 确定实际采集的目标日期
        actual_target = force_date or target_date or today_str
        
        # 情况1: 强制指定日期 或 目标日期不是今天 - 允许采集（历史数据）
        if force_date or actual_target != today_str:
            return True, f"[{current_time_str}] 采集历史数据: {actual_target}，允许采集"
        
        # 情况2: 非交易日 - 允许采集
        if not cls.is_trading_day():
            weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
            weekday = weekday_names[now.weekday()]
            return True, f"[{current_time_str}] 非交易日 ({weekday})，允许采集"
        
        # 情况3: 交易日盘中 - 禁止采集当日数据！
        if cls.is_intraday():
            current_minutes = cls.get_current_time_minutes()
            hour = current_minutes // 60
            minute = current_minutes % 60
            return False, f"[{current_time_str}] 🚨 交易日盘中 ({hour:02d}:{minute:02d})，禁止采集当日数据！"
        
        # 情况4: 交易日收盘后但在缓冲期内 - 谨慎处理
        current_minutes = cls.get_current_time_minutes()
        if cls.MARKET_CLOSE <= current_minutes < cls.ALLOW_COLLECT_AFTER:
            hour = current_minutes // 60
            minute = current_minutes % 60
            return False, f"[{current_time_str}] ⚠️ 刚收盘 ({hour:02d}:{minute:02d})，等待 {cls.ALLOW_COLLECT_AFTER // 60}:{cls.ALLOW_COLLECT_AFTER % 60:02d} 后再采集"
        
        # 情况5: 交易日收盘后 - 允许采集当日数据
        return True, f"[{current_time_str}] 交易日收盘后，允许采集当日数据"
    
    @classmethod
    def enforce_collection_rules(cls, target_date: Optional[str] = None, force_date: Optional[str] = None, exit_code: int = 1) -> bool:
        """
        强制执行采集规则
        
        如果在盘中时段且采集当日数据，直接终止程序！
        
        Args:
            target_date: 目标采集日期（默认今天）
            force_date: 强制指定日期
            exit_code: 退出码
        
        Returns:
            bool: 是否允许继续（不会返回False，会直接退出）
        """
        allowed, message = cls.check_collection_allowed(target_date, force_date)
        
        if allowed:
            logger.info(message)
            return True
        
        # 不允许采集 - 记录错误并退出
        logger.error("=" * 70)
        logger.error(message)
        logger.error("=" * 70)
        logger.error("根据项目规则：交易日盘中（9:30-15:00）禁止采集当日K线数据")
        logger.error("原因：盘中行情不完整，采集会导致数据污染")
        logger.error("")
        logger.error("如需补采历史数据，请使用 --date 参数指定历史日期")
        logger.error("示例: python data_collect.py --date 2026-04-15")
        logger.error("=" * 70)
        logger.error("程序已强制终止")
        logger.error("=" * 70)
        
        # 强制退出！
        sys.exit(exit_code)


# 便捷函数
def enforce_market_closed(target_date: Optional[str] = None, force_date: Optional[str] = None):
    """
    强制检查市场已收盘
    
    如果在盘中且采集当日数据，程序直接退出
    
    Args:
        target_date: 目标采集日期（默认今天）
        force_date: 强制指定日期（用于补采）
    """
    return MarketGuardian.enforce_collection_rules(target_date, force_date)


def is_intraday() -> bool:
    """判断当前是否在盘中"""
    return MarketGuardian.is_intraday()


if __name__ == "__main__":
    # 测试
    print("=" * 70)
    print("市场守护模块测试")
    print("=" * 70)
    
    now = datetime.now()
    print(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"是否交易日: {MarketGuardian.is_trading_day()}")
    print(f"是否在盘中: {MarketGuardian.is_intraday()}")
    
    allowed, message = MarketGuardian.check_collection_allowed()
    print(f"\n检查结果: {message}")
    
    print("\n" + "=" * 70)
    print("测试强制执行（不会真的退出）...")
    print("=" * 70)
    
    # 实际使用时调用：
    # enforce_market_closed()
