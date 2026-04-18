#!/usr/bin/env python3
"""
盘中采集检测器

在数据写入前进行最终检查，防止盘中数据入库
"""
import sys
from datetime import datetime, time
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class IntradayCollectionDetector:
    """
    盘中采集检测器
    
    作为最后一道防线，在数据写入前检查采集时间
    """
    
    @staticmethod
    def check_fetch_time(fetch_time_str: Optional[str]) -> bool:
        """
        检查采集时间是否在盘中
        
        Args:
            fetch_time_str: 采集时间字符串 (YYYY-MM-DD HH:MM:SS)
        
        Returns:
            bool: 是否允许写入（True=允许，False=禁止）
        """
        if not fetch_time_str:
            return True  # 无采集时间，允许写入
        
        try:
            fetch_time = datetime.strptime(fetch_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 检查是否为交易日
            if fetch_time.weekday() >= 5:  # 周末
                return True
            
            # 检查是否在盘中 (9:30-15:00)
            hour = fetch_time.hour
            minute = fetch_time.minute
            time_val = hour * 60 + minute
            
            market_open = 9 * 60 + 30   # 9:30
            market_close = 15 * 60       # 15:00
            
            if market_open <= time_val < market_close:
                logger.error(f"🚨 检测到盘中采集: {fetch_time_str}")
                logger.error("根据项目规则，盘中采集的数据禁止入库！")
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"无法解析采集时间: {fetch_time_str}, 错误: {e}")
            return True  # 解析失败，允许写入
    
    @staticmethod
    def validate_dataframe(df, fetch_time_col: str = 'fetch_time') -> bool:
        """
        验证 DataFrame 中的采集时间
        
        Args:
            df: Polars DataFrame
            fetch_time_col: 采集时间列名
        
        Returns:
            bool: 是否允许写入
        """
        if fetch_time_col not in df.columns:
            return True  # 无采集时间列，允许写入
        
        # 获取最新的采集时间
        try:
            latest_fetch_time = df[fetch_time_col].drop_nulls().tail(1).to_list()
            if latest_fetch_time:
                return IntradayCollectionDetector.check_fetch_time(str(latest_fetch_time[0]))
        except Exception as e:
            logger.warning(f"验证采集时间失败: {e}")
        
        return True
    
    @staticmethod
    def guard_data_write(code: str, fetch_time_str: Optional[str]) -> bool:
        """
        数据写入守卫
        
        如果检测到盘中采集，记录详细日志并拒绝写入
        
        Args:
            code: 股票代码
            fetch_time_str: 采集时间
        
        Returns:
            bool: 是否允许写入
        """
        if not fetch_time_str:
            return True
        
        try:
            fetch_time = datetime.strptime(fetch_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 周末允许
            if fetch_time.weekday() >= 5:
                return True
            
            hour = fetch_time.hour
            minute = fetch_time.minute
            time_val = hour * 60 + minute
            
            market_open = 9 * 60 + 30
            market_close = 15 * 60
            
            if market_open <= time_val < market_close:
                logger.error("=" * 70)
                logger.error(f"🚨 盘中采集拦截 - 股票: {code}")
                logger.error(f"   采集时间: {fetch_time_str}")
                logger.error(f"   采集时段: 交易日盘中 ({hour:02d}:{minute:02d})")
                logger.error("=" * 70)
                logger.error("❌ 数据被拒绝写入！")
                logger.error("原因: 根据项目规则，交易日盘中禁止采集数据")
                logger.error("      盘中数据不完整，会导致数据污染")
                logger.error("=" * 70)
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"守卫检查失败 {code}: {e}")
            return True


# 便捷函数
def guard_data_write(code: str, fetch_time: Optional[str]) -> bool:
    """数据写入守卫"""
    return IntradayCollectionDetector.guard_data_write(code, fetch_time)


def validate_fetch_time(fetch_time: Optional[str]) -> bool:
    """验证采集时间"""
    return IntradayCollectionDetector.check_fetch_time(fetch_time)


if __name__ == "__main__":
    # 测试
    print("=" * 70)
    print("盘中采集检测器测试")
    print("=" * 70)
    
    test_cases = [
        ("2026-04-18 09:45:00", "周五盘中"),
        ("2026-04-18 15:30:00", "周五收盘后"),
        ("2026-04-19 10:00:00", "周六"),
        ("2026-04-17 14:30:00", "周四盘中"),
    ]
    
    for fetch_time, desc in test_cases:
        result = IntradayCollectionDetector.check_fetch_time(fetch_time)
        status = "✅ 允许" if result else "❌ 禁止"
        print(f"{fetch_time} ({desc}): {status}")
