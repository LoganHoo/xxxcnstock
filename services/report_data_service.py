"""
报告数据统一服务

提供统一的数据访问接口，用于报告生成
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from functools import lru_cache
import polars as pl

from core.paths import ReportPaths, DataPaths

logger = logging.getLogger(__name__)


class ReportDataService:
    """报告数据服务 - 统一数据访问接口"""

    def __init__(self):
        self._cache = {}
        self._cache_ttl = 300  # 缓存5分钟
        self._cache_time = {}

    def _get_from_cache(self, key: str) -> Optional[Any]:
        """从缓存获取数据"""
        if key in self._cache:
            cache_time = self._cache_time.get(key, 0)
            if datetime.now().timestamp() - cache_time < self._cache_ttl:
                return self._cache[key]
            else:
                # 过期清理
                del self._cache[key]
                del self._cache_time[key]
        return None

    def _set_cache(self, key: str, value: Any):
        """设置缓存"""
        self._cache[key] = value
        self._cache_time[key] = datetime.now().timestamp()

    def _load_json(self, file_path: Path, use_cache: bool = True) -> Optional[Dict]:
        """
        加载JSON文件
        
        Args:
            file_path: 文件路径
            use_cache: 是否使用缓存
            
        Returns:
            dict or None
        """
        if file_path is None:
            return None

        cache_key = str(file_path)
        
        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached

        if not file_path.exists():
            return None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if use_cache:
                    self._set_cache(cache_key, data)
                return data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"加载文件失败 {file_path}: {e}")
            return None

    # ========== 外盘数据 ==========
    def get_foreign_data(self, use_cache: bool = True) -> Dict:
        """
        获取外盘数据
        
        Returns:
            {
                'us_index': {...},
                'asia_index': {...},
                'europe_index': {...}
            }
        """
        return self._load_json(ReportPaths.foreign_index(), use_cache) or {}

    # ========== 大盘分析数据 ==========
    def get_market_analysis(self, date: Optional[datetime] = None,
                           fallback_to_yesterday: bool = True,
                           use_cache: bool = True) -> Dict:
        """
        获取大盘分析数据
        
        Args:
            date: 指定日期
            fallback_to_yesterday: 是否回退到昨天
            use_cache: 是否使用缓存
            
        Returns:
            {
                'indices': [...],
                'summary': {...},
                'cvd': {...},
                'key_levels': {...}
            }
        """
        file_path = ReportPaths.market_analysis(date, fallback_to_yesterday)
        return self._load_json(file_path, use_cache) or {}

    # ========== 选股数据 ==========
    def get_daily_picks(self, date: Optional[datetime] = None,
                       fallback_to_yesterday: bool = True,
                       use_cache: bool = True) -> Dict:
        """
        获取选股数据
        
        Returns:
            {
                'filters': {
                    's_grade': {...},
                    'a_grade': {...}
                },
                'summary': {...}
            }
        """
        file_path = ReportPaths.daily_picks(date, fallback_to_yesterday)
        return self._load_json(file_path, use_cache) or {}

    # ========== 策略结果 ==========
    def get_strategy_result(self, use_cache: bool = True) -> Dict:
        """
        获取策略综合结果
        
        Returns:
            {
                'signals': [...],
                'summary': {...}
            }
        """
        return self._load_json(ReportPaths.strategy_result(), use_cache) or {}

    # ========== 资金行为学结果 ==========
    def get_fund_behavior_result(self, use_cache: bool = True) -> Dict:
        """
        获取资金行为学策略结果
        
        Returns:
            {
                'scan_time': '...',
                'total_analyzed': 0,
                'signal_stocks': [...],
                'hot_themes': [...],
                'cvd_signal': '...',
                'recommendations': {...}
            }
        """
        return self._load_json(ReportPaths.fund_behavior_result(), use_cache) or {}

    # ========== 宏观数据 ==========
    def get_macro_data(self, use_cache: bool = True) -> Dict:
        """
        获取宏观数据
        
        Returns:
            {
                'dxy': {...},
                'us10y': {...},
                'cny': {...}
            }
        """
        return self._load_json(ReportPaths.macro_data(), use_cache) or {}

    # ========== 石油美元数据 ==========
    def get_oil_dollar_data(self, use_cache: bool = True) -> Dict:
        """
        获取石油美元数据
        
        Returns:
            {
                'oil': {...},
                'notes': [...]
            }
        """
        return self._load_json(ReportPaths.oil_dollar_data(), use_cache) or {}

    # ========== 大宗商品数据 ==========
    def get_commodities_data(self, use_cache: bool = True) -> Dict:
        """
        获取大宗商品数据
        
        Returns:
            {
                'metals': {...},
                'agriculture': {...}
            }
        """
        return self._load_json(ReportPaths.commodities_data(), use_cache) or {}

    # ========== 情绪数据 ==========
    def get_sentiment_data(self, use_cache: bool = True) -> Dict:
        """
        获取情绪数据
        
        Returns:
            {
                'fear_greed': {...},
                'bomb_rate': {...}
            }
        """
        return self._load_json(ReportPaths.sentiment_data(), use_cache) or {}

    # ========== 新闻数据 ==========
    def get_news_data(self, use_cache: bool = True) -> List[Dict]:
        """
        获取新闻数据
        
        Returns:
            [{'title': '...', 'content': '...'}, ...]
        """
        data = self._load_json(ReportPaths.news_data(), use_cache)
        return data if isinstance(data, list) else []

    # ========== 数据质检报告 ==========
    def get_dq_report(self, use_cache: bool = True) -> Dict:
        """
        获取数据质检报告
        
        Returns:
            {
                'completeness': {...},
                'validity': {...},
                'freshness': {...}
            }
        """
        return self._load_json(ReportPaths.dq_close(), use_cache) or {}

    # ========== 市场复盘数据 ==========
    def get_market_review(self, use_cache: bool = True) -> Dict:
        """
        获取市场复盘数据
        
        Returns:
            {
                'date': '...',
                'summary': {...},
                'market_status': '...',
                'cvd': {...},
                'key_levels': {...},
                'top_sectors': [...]
            }
        """
        return self._load_json(ReportPaths.market_review(), use_cache) or {}

    # ========== 选股复盘数据 ==========
    def get_picks_review(self, use_cache: bool = True) -> Dict:
        """
        获取选股复盘数据
        
        Returns:
            {
                'summary': {...},
                'top_picks': [...],
                'details': [...]
            }
        """
        return self._load_json(ReportPaths.picks_review(), use_cache) or {}

    # ========== 增强评分数据 ==========
    def get_enhanced_scores(self, date: Optional[str] = None) -> Optional[pl.DataFrame]:
        """
        获取增强评分数据
        
        Args:
            date: 指定日期，格式 'YYYY-MM-DD'
            
        Returns:
            polars DataFrame or None
        """
        cache_key = f"enhanced_scores_{date}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        file_path = ReportPaths.enhanced_scores()
        if not file_path.exists():
            return None

        try:
            df = pl.read_parquet(file_path)
            if date and 'trade_date' in df.columns:
                df = df.filter(pl.col('trade_date') == date)
            self._set_cache(cache_key, df)
            return df
        except Exception as e:
            logger.warning(f"加载增强评分数据失败: {e}")
            return None

    # ========== CVD数据 ==========
    def get_cvd_data(self) -> Optional[pl.DataFrame]:
        """
        获取CVD数据
        
        Returns:
            polars DataFrame or None
        """
        cache_key = "cvd_data"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        file_path = ReportPaths.cvd_latest()
        if not file_path.exists():
            return None

        try:
            df = pl.read_parquet(file_path)
            self._set_cache(cache_key, df)
            return df
        except Exception as e:
            logger.warning(f"加载CVD数据失败: {e}")
            return None

    # ========== 批量获取数据 ==========
    def get_morning_report_data(self) -> Dict[str, Any]:
        """
        批量获取晨间报告所需数据
        
        Returns:
            {
                'foreign': {...},
                'market': {...},
                'picks': {...},
                'strategy': {...},
                'fund_behavior': {...}
            }
        """
        return {
            'foreign': self.get_foreign_data(),
            'market': self.get_market_analysis(),
            'picks': self.get_daily_picks(),
            'strategy': self.get_strategy_result(),
            'fund_behavior': self.get_fund_behavior_result()
        }

    def get_morning_shao_data(self) -> Dict[str, Any]:
        """
        批量获取晨前哨报所需数据
        
        Returns:
            {
                'foreign': {...},
                'market': {...},
                'macro': {...},
                'oil_dollar': {...},
                'commodities': {...},
                'sentiment': {...},
                'news': [...],
                'fund_behavior': {...},
                'picks': {...}
            }
        """
        return {
            'foreign': self.get_foreign_data(),
            'market': self.get_market_analysis(),
            'macro': self.get_macro_data(),
            'oil_dollar': self.get_oil_dollar_data(),
            'commodities': self.get_commodities_data(),
            'sentiment': self.get_sentiment_data(),
            'news': self.get_news_data(),
            'fund_behavior': self.get_fund_behavior_result(),
            'picks': self.get_daily_picks()
        }

    def get_review_report_data(self) -> Dict[str, Any]:
        """
        批量获取复盘报告所需数据
        
        Returns:
            {
                'dq': {...},
                'market_review': {...},
                'picks_review': {...}
            }
        """
        return {
            'dq': self.get_dq_report(),
            'market_review': self.get_market_review(),
            'picks_review': self.get_picks_review()
        }

    # ========== 缓存管理 ==========
    def clear_cache(self):
        """清除所有缓存"""
        self._cache.clear()
        self._cache_time.clear()
        logger.info("报告数据缓存已清除")

    def invalidate_cache(self, key: str):
        """使特定缓存失效"""
        if key in self._cache:
            del self._cache[key]
            del self._cache_time[key]


# 全局服务实例
_report_data_service: Optional[ReportDataService] = None


def get_report_data_service() -> ReportDataService:
    """获取报告数据服务实例（单例）"""
    global _report_data_service
    if _report_data_service is None:
        _report_data_service = ReportDataService()
    return _report_data_service
