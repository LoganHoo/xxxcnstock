"""
统一路径配置模块

集中管理所有数据文件路径，避免各脚本中硬编码路径
"""
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 数据目录
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"

# 子目录
KLINE_DIR = DATA_DIR / "kline"
INDEX_DIR = DATA_DIR / "index"
CACHE_DIR = DATA_DIR / "cache"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"
REPORTS_DATA_DIR = DATA_DIR / "reports"
REPORTS_HTML_DIR = REPORTS_DATA_DIR / "html"


def get_data_path() -> Path:
    """获取数据目录路径"""
    return DATA_DIR


class ReportPaths:
    """报告相关文件路径"""
    
    @staticmethod
    def get_date_str(date: Optional[datetime] = None) -> str:
        """获取日期字符串"""
        if date is None:
            date = datetime.now()
        return date.strftime('%Y%m%d')
    
    @staticmethod
    def get_date_str_hyphen(date: Optional[datetime] = None) -> str:
        """获取带连字符的日期字符串"""
        if date is None:
            date = datetime.now()
        return date.strftime('%Y-%m-%d')
    
    # ========== 晨间报告数据 ==========
    @classmethod
    def foreign_index(cls) -> Path:
        """外盘数据"""
        return DATA_DIR / "foreign_index.json"
    
    @classmethod
    def market_analysis(cls, date: Optional[datetime] = None, 
                       fallback_to_yesterday: bool = True) -> Optional[Path]:
        """
        大盘分析数据
        
        Args:
            date: 指定日期，默认今天
            fallback_to_yesterday: 如果今天不存在是否返回昨天的路径
        
        Returns:
            存在的文件路径，或None
        """
        today_str = cls.get_date_str(date)
        today_file = REPORTS_DIR / f"market_analysis_{today_str}.json"
        
        if today_file.exists():
            return today_file
        
        if fallback_to_yesterday:
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_str = cls.get_date_str(yesterday)
            yesterday_file = REPORTS_DIR / f"market_analysis_{yesterday_str}.json"
            if yesterday_file.exists():
                return yesterday_file
        
        return None
    
    @classmethod
    def daily_picks(cls, date: Optional[datetime] = None,
                   fallback_to_yesterday: bool = True) -> Optional[Path]:
        """选股数据"""
        today_str = cls.get_date_str(date)
        today_file = REPORTS_DIR / f"daily_picks_{today_str}.json"
        
        if today_file.exists():
            return today_file
        
        if fallback_to_yesterday:
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_str = cls.get_date_str(yesterday)
            yesterday_file = REPORTS_DIR / f"daily_picks_{yesterday_str}.json"
            if yesterday_file.exists():
                return yesterday_file
        
        return None
    
    @classmethod
    def fund_behavior_result(cls) -> Path:
        """资金行为学策略结果"""
        return REPORTS_DIR / "fund_behavior_result.json"
    
    @classmethod
    def strategy_result(cls) -> Path:
        """策略综合结果"""
        return REPORTS_DIR / "strategy_result.json"
    
    # ========== 晨前哨报数据 ==========
    @classmethod
    def macro_data(cls) -> Path:
        """宏观数据"""
        return DATA_DIR / "macro_data.json"
    
    @classmethod
    def oil_dollar_data(cls) -> Path:
        """石油美元数据"""
        return DATA_DIR / "oil_dollar_data.json"
    
    @classmethod
    def commodities_data(cls) -> Path:
        """大宗商品数据"""
        return DATA_DIR / "commodities_data.json"
    
    @classmethod
    def sentiment_data(cls) -> Path:
        """情绪数据"""
        return DATA_DIR / "sentiment_data.json"
    
    @classmethod
    def news_data(cls) -> Path:
        """新闻数据"""
        return DATA_DIR / "news_data.json"
    
    # ========== 复盘报告数据 ==========
    @classmethod
    def dq_close(cls) -> Path:
        """数据质检报告"""
        return DATA_DIR / "dq_close.json"
    
    @classmethod
    def market_review(cls) -> Path:
        """市场复盘数据"""
        return DATA_DIR / "market_review.json"
    
    @classmethod
    def picks_review(cls) -> Path:
        """选股复盘数据"""
        return DATA_DIR / "picks_review.json"
    
    @classmethod
    def okr_data(cls) -> Path:
        """OKR数据"""
        return DATA_DIR / "okr.json"
    
    @classmethod
    def ai_review(cls) -> Path:
        """AI复盘数据"""
        return DATA_DIR / "ai_review.json"
    
    @classmethod
    def enhanced_scores(cls) -> Path:
        """增强评分数据"""
        return DATA_DIR / "enhanced_full_temp.parquet"
    
    @classmethod
    def cvd_latest(cls) -> Path:
        """CVD最新数据"""
        return DATA_DIR / "cvd_latest.parquet"
    
    # ========== 报告输出路径 ==========
    @classmethod
    def morning_report(cls, date: Optional[datetime] = None) -> Path:
        """晨间报告输出路径"""
        date_str = cls.get_date_str_hyphen(date)
        return REPORTS_DATA_DIR / f"morning_{date_str}.txt"
    
    @classmethod
    def morning_shao_report(cls, date: Optional[datetime] = None) -> Path:
        """晨前哨报输出路径"""
        date_str = cls.get_date_str_hyphen(date)
        return REPORTS_DATA_DIR / f"morning_shao_{date_str}.txt"
    
    @classmethod
    def review_report(cls, date: Optional[datetime] = None) -> Path:
        """复盘报告输出路径"""
        date_str = cls.get_date_str_hyphen(date)
        return REPORTS_DATA_DIR / f"review_{date_str}.txt"


class DataPaths:
    """数据文件路径"""
    
    @classmethod
    def kline_file(cls, code: str) -> Path:
        """K线数据文件"""
        return KLINE_DIR / f"{code}.parquet"
    
    @classmethod
    def index_file(cls, code: str) -> Path:
        """指数数据文件"""
        return INDEX_DIR / f"{code}.parquet"
    
    @classmethod
    def stock_list(cls) -> Path:
        """股票列表"""
        return DATA_DIR / "stock_list.parquet"
    
    @classmethod
    def enhanced_scores_full(cls) -> Path:
        """完整评分数据"""
        return DATA_DIR / "enhanced_scores_full.parquet"


# 便捷函数
def get_project_root() -> Path:
    """获取项目根目录"""
    return PROJECT_ROOT


def ensure_dir(path: Path) -> Path:
    """确保目录存在"""
    path.mkdir(parents=True, exist_ok=True)
    return path
