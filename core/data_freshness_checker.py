"""数据新鲜度和完整性检查器

为所有脚本提供统一的数据新鲜度和完整性检查功能，确保分析前数据是最新和完整的。
"""
import logging
import polars as pl
from typing import Dict, Any, Tuple
from datetime import datetime, time
from pathlib import Path

from core.trading_calendar import TradingCalendar, check_market_status


class DataFreshnessChecker:
    """数据新鲜度检查器
    
    负责检查数据的新鲜度和完整性，确保分析前数据是最新的。
    """
    
    def __init__(self, data_dir: str):
        """初始化数据新鲜度检查器
        
        Args:
            data_dir: 数据目录路径
        """
        self.data_dir = Path(data_dir)
        self.calendar = TradingCalendar()
        self.logger = logging.getLogger(__name__)
    
    def check_market_ready(self) -> Tuple[bool, str]:
        """检查市场是否准备就绪（交易日且收盘后）
        
        Returns:
            Tuple[bool, str]: (是否就绪, 原因)
        """
        market_status = check_market_status()
        
        self.logger.info(f"当前时间: {market_status['current_time']}")
        self.logger.info(f"是否交易日: {'是' if market_status['is_trading_day'] else '否'}")
        self.logger.info(f"是否收盘后: {'是' if market_status['is_after_market_close'] else '否'}")
        self.logger.info(f"上一交易日: {market_status['last_trading_day']}")
        
        if not market_status['is_trading_day']:
            return False, f"非交易日: {market_status['reason']}"

        if not market_status['is_after_market_close']:
            current_time = datetime.now().time()
            if current_time < time(15, 30):
                last_trading_day = market_status['last_trading_day']
                kline_dir = self.data_dir / "kline"
                if kline_dir.exists():
                    sample_file = kline_dir / "000001.parquet"
                    if sample_file.exists():
                        sample_data = pl.read_parquet(sample_file)
                        latest_date = sample_data['trade_date'].max()
                        if latest_date == last_trading_day:
                            return True, f"使用上一交易日({last_trading_day})数据"
                return False, f"市场未收盘，当前时间: {current_time}"

        return True, "市场已就绪"
    
    def check_data_freshness(self) -> Tuple[bool, str, Dict[str, Any]]:
        """检查数据新鲜度
        
        Returns:
            Tuple[bool, str, Dict[str, Any]]: (是否新鲜, 原因, 详细信息)
        """
        # 检查市场状态
        market_ready, market_reason = self.check_market_ready()
        if not market_ready:
            return False, market_reason, {}
        
        # 获取上一交易日
        last_trading_day = self.calendar.get_last_trading_day()
        
        # 检查K线数据目录
        kline_dir = self.data_dir / "kline"
        if not kline_dir.exists():
            return False, f"K线数据目录不存在: {kline_dir}", {}
        
        # 检查股票列表
        stock_list_file = self.data_dir / "stock_list.parquet"
        if not stock_list_file.exists():
            return False, f"股票列表文件不存在: {stock_list_file}", {}
        
        try:
            # 读取股票列表
            stock_list = pl.read_parquet(stock_list_file)
            stock_codes = stock_list['code'].to_list()
            
            if len(stock_codes) == 0:
                return False, "股票列表为空", {}
            
            # 检查样本股票的最新数据
            sample_code = stock_codes[0]
            sample_file = kline_dir / f"{sample_code}.parquet"
            
            if not sample_file.exists():
                return False, f"样本股票文件不存在: {sample_file}", {}
            
            # 读取样本数据
            sample_data = pl.read_parquet(sample_file)
            
            if len(sample_data) == 0:
                return False, "样本股票数据为空", {}
            
            # 检查是否包含上一交易日数据
            latest_date = sample_data['trade_date'].max()
            
            if isinstance(latest_date, str):
                latest_date = datetime.fromisoformat(latest_date).date()
            elif isinstance(latest_date, datetime):
                latest_date = latest_date.date()
            
            last_trading_day_date = datetime.fromisoformat(last_trading_day).date()
            
            # 计算数据新鲜度
            days_diff = (last_trading_day_date - latest_date).days if latest_date < last_trading_day_date else 0
            
            if days_diff > 1:
                return False, f"数据不新鲜，最新日期: {latest_date}, 上一交易日: {last_trading_day}", {
                    'latest_date': str(latest_date),
                    'last_trading_day': last_trading_day,
                    'days_diff': days_diff
                }
            
            # 检查数据完整性
            total_stocks = len(stock_codes)
            existing_files = [f.name.replace('.parquet', '') for f in kline_dir.glob('*.parquet')]
            existing_count = len(existing_files)
            
            completeness_rate = (existing_count / total_stocks) * 100 if total_stocks > 0 else 0
            
            if completeness_rate < 90:
                return False, f"数据完整性不足，完整率: {completeness_rate:.2f}%", {
                    'total_stocks': total_stocks,
                    'existing_files': existing_count,
                    'completeness_rate': completeness_rate
                }
            
            # 数据新鲜度检查通过
            return True, "数据新鲜度检查通过", {
                'latest_date': str(latest_date),
                'last_trading_day': last_trading_day,
                'total_stocks': total_stocks,
                'existing_files': existing_count,
                'completeness_rate': completeness_rate
            }
            
        except Exception as e:
            self.logger.error(f"检查数据新鲜度时发生错误: {e}")
            return False, f"检查数据新鲜度失败: {e}", {}
    
    def ensure_data_freshness(self) -> bool:
        """确保数据新鲜度
        
        检查数据是否新鲜，如不新鲜则触发数据采集
        
        Returns:
            bool: 是否成功确保数据新鲜度
        """
        import subprocess
        import sys
        
        # 配置日志
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        self.logger.info("="*70)
        self.logger.info("检查数据新鲜度")
        self.logger.info("="*70)
        
        # 检查市场状态
        market_ready, market_reason = self.check_market_ready()
        if not market_ready:
            self.logger.error(f"❌ 市场未就绪: {market_reason}")
            return False
        
        # 检查数据新鲜度
        fresh, reason, details = self.check_data_freshness()
        
        if fresh:
            self.logger.info("✅ 数据新鲜度检查通过")
            self.logger.info(f"  最新数据日期: {details.get('latest_date')}")
            self.logger.info(f"  股票数量: {details.get('total_stocks')}")
            self.logger.info(f"  完整率: {details.get('completeness_rate', 0):.2f}%")
            return True
        else:
            self.logger.warning(f"⚠️  数据不新鲜: {reason}")
            
            # 触发数据采集
            self.logger.info("开始执行数据采集...")
            
            try:
                fetch_script = Path(__file__).parent.parent / "scripts" / "fetch_history_klines_parquet.py"
                
                if not fetch_script.exists():
                    self.logger.error(f"❌ 数据采集脚本不存在: {fetch_script}")
                    return False
                
                result = subprocess.run(
                    [sys.executable, str(fetch_script), "--days", "7"],
                    cwd=str(Path(__file__).parent.parent),
                    capture_output=True,
                    text=True,
                    timeout=3600
                )
                
                if result.returncode == 0:
                    self.logger.info("✅ 数据采集成功")
                    # 再次检查数据新鲜度
                    fresh_after, reason_after, _ = self.check_data_freshness()
                    if fresh_after:
                        self.logger.info("✅ 数据新鲜度检查通过")
                        return True
                    else:
                        self.logger.error(f"❌ 数据采集后仍不新鲜: {reason_after}")
                        return False
                else:
                    self.logger.error(f"❌ 数据采集失败: {result.stderr}")
                    return False
                    
            except Exception as e:
                self.logger.error(f"❌ 执行数据采集时发生错误: {e}")
                return False
