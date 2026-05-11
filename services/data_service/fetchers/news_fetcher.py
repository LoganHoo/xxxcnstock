#!/usr/bin/env python3
"""
财经新闻采集器 - 服务层实现
从 RSS 和网页采集国内/海外财经新闻，支持 MySQL(SQLAlchemy) 和 JSON 双存储。

数据源:
- 新华社 RSS (politics)
- 新浪财经 (网页)
- 东方财富 (网页)
- 华尔街见闻 RSS (海外)
"""
import json
import os
import random
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree

import requests
from core.logger import get_logger

from services.db_pool import get_db_pool
from services.data_service.models.market_data_models import FinancialNews, Base

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------
@dataclass
class NewsData:
    """新闻数据结构"""
    date: str
    update_time: str
    domestic: List[str]
    overseas: List[str]
    all: List[str]
    source: str = "api"
    retry_count: int = 0
    is_default: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsData":
        return cls(**data)

    def is_valid(self) -> bool:
        if not self.date or len(self.date) != 10:
            return False
        try:
            datetime.strptime(self.date, "%Y-%m-%d")
        except ValueError:
            return False
        domestic_valid = len([d for d in self.domestic if d and d.strip()]) >= 3
        overseas_valid = len([o for o in self.overseas if o and o.strip()]) >= 2
        all_valid = len([a for a in self.all if a and a.strip()]) >= 5
        return domestic_valid and overseas_valid and all_valid


# ---------------------------------------------------------------------------
# Retry manager
# ---------------------------------------------------------------------------
class RetryManager:
    """重试管理器 - 支持指数退避和抖动"""

    def __init__(
        self,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        jitter: bool = True,
    ):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        self.jitter = jitter

    def _get_sleep_time(self, attempt: int) -> float:
        base_time = self.delay * (self.backoff ** attempt)
        if self.jitter:
            base_time *= 0.8 + random.random() * 0.4
        return base_time

    def execute(self, func, *args, **kwargs) -> Tuple[Any, int]:
        last_exception = None
        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result, attempt
            except Exception as e:
                last_exception = e
                logger.warning(f"尝试 {attempt + 1}/{self.max_retries + 1} 失败: {e}")
            if attempt < self.max_retries:
                sleep_time = self._get_sleep_time(attempt)
                logger.info(f"等待 {sleep_time:.1f} 秒后重试...")
                time.sleep(sleep_time)
        logger.error(f"所有 {self.max_retries + 1} 次尝试都失败")
        return None, self.max_retries


# ---------------------------------------------------------------------------
# Main fetcher
# ---------------------------------------------------------------------------
class NewsFetcher:
    """财经新闻采集器

    Public API:
        fetch_news(date) -> Dict
        save_to_db(data)
        save_to_json(data)
        fetch_and_save(date) -> Dict
    """

    def __init__(self, retry_manager: Optional[RetryManager] = None):
        self.retry_manager = retry_manager or RetryManager(max_retries=3)
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        )
        self._db_initialized = False

    # ---- DB helpers -------------------------------------------------------
    def _ensure_db(self):
        """确保 SQLAlchemy 连接池已初始化"""
        if self._db_initialized:
            return
        pool = get_db_pool()
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "3306")
        db_user = os.getenv("DB_USER", "root")
        db_password = os.getenv("DB_PASSWORD", "")
        db_name = os.getenv("DB_NAME", "quantdb")
        conn_str = (
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}"
            f"/{db_name}?charset=utf8mb4"
        )
        pool.get_pool("market_data", conn_str)
        # 建表（幂等）
        Base.metadata.create_all(pool._pools["market_data"]["engine"])
        self._db_initialized = True

    # ---- HTTP with retry --------------------------------------------------
    def _safe_request(self, url: str, timeout: int = 10) -> Optional[requests.Response]:
        def do_request():
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp

        result, _ = self.retry_manager.execute(do_request)
        return result

    # ---- Source collectors ------------------------------------------------
    def _collect_xinhua(self) -> List[str]:
        news: List[str] = []
        try:
            resp = self._safe_request(
                "https://www.xinhuanet.com/politics/news_politics.xml", timeout=15
            )
            if resp:
                root = ElementTree.fromstring(resp.content)
                for item in root.findall(".//item")[:10]:
                    title = item.find("title")
                    if title is not None and title.text:
                        news.append(title.text)
        except Exception as e:
            logger.warning(f"新华社新闻采集失败: {e}")
        return news

    def _collect_sina_finance(self) -> List[str]:
        news: List[str] = []
        try:
            resp = self._safe_request("https://finance.sina.com.cn/stock/", timeout=15)
            if resp:
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(resp.text, "html.parser")
                selectors = ["a.news-link", "a.f14", "h2 a", ".news-item a"]
                for sel in selectors:
                    for h in soup.select(sel)[:10]:
                        text = h.get_text(strip=True)
                        if text and len(text) > 10:
                            news.append(text)
                    if news:
                        break
        except Exception as e:
            logger.warning(f"新浪财经采集失败: {e}")
        return news

    def _collect_eastmoney(self) -> List[str]:
        news: List[str] = []
        try:
            resp = self._safe_request("https://www.eastmoney.com", timeout=15)
            if resp:
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(resp.text, "html.parser")
                for h in soup.find_all("a", class_="news-title")[:10]:
                    t = h.text.strip()
                    if t:
                        news.append(t)
        except Exception as e:
            logger.warning(f"东方财富新闻采集失败: {e}")
        return news

    def _collect_wallstreetcn(self) -> List[str]:
        news: List[str] = []
        try:
            resp = self._safe_request(
                "https://rsshub.app/wallstreetcn/news/global", timeout=15
            )
            if resp:
                root = ElementTree.fromstring(resp.content)
                for item in root.findall(".//item")[:6]:
                    title = item.find("title")
                    if title is not None and title.text:
                        news.append(title.text)
        except Exception as e:
            logger.warning(f"华尔街见闻采集失败: {e}")
        return news

    # ---- Default content generators ---------------------------------------
    @staticmethod
    def _default_domestic_by_date(date: str) -> List[str]:
        templates = [
            "央行开展公开市场操作，维护流动性合理充裕",
            "国民经济持续恢复向好，高质量发展稳步推进",
            "资本市场改革深化，投资者保护力度加强",
            "科技创新取得新突破，新质生产力加快发展",
            "绿色低碳转型加速，生态文明建设成效显著",
            "区域协调发展推进，城乡融合不断深化",
            "就业形势总体稳定，居民收入稳步增长",
            "制造业PMI回升，工业经济稳中向好",
            "消费市场持续恢复，内需潜力逐步释放",
            "外贸进出口稳中提质，国际竞争力增强",
            "数字经济蓬勃发展，产业数字化加速推进",
            "乡村振兴战略实施，农业农村现代化加快",
        ]
        d = datetime.strptime(date, "%Y-%m-%d")
        offset = (d.timetuple().tm_yday + d.weekday()) % 6
        picked = templates[offset : offset + 6]
        if len(picked) < 6:
            picked += templates[: 6 - len(picked)]
        return picked

    @staticmethod
    def _default_overseas_by_date(date: str) -> List[str]:
        templates = [
            "美联储货币政策会议维持基准利率不变",
            "欧洲央行关注通胀走势，货币政策保持灵活",
            "全球主要经济体增长预期调整",
            "国际能源市场波动，油价维持区间震荡",
            "全球供应链持续修复，贸易活动回暖",
            "主要股市指数震荡整理，市场情绪谨慎",
            "国际金价波动，避险情绪有所升温",
            "美元指数走势分化，非美货币表现各异",
        ]
        d = datetime.strptime(date, "%Y-%m-%d")
        offset = d.timetuple().tm_yday % 4
        picked = templates[offset : offset + 4]
        if len(picked) < 4:
            picked += templates[: 4 - len(picked)]
        return picked

    # ---- Public API -------------------------------------------------------
    def fetch_news(self, date: Optional[str] = None) -> Dict[str, Any]:
        """采集所有数据源的新闻并返回字典。

        Args:
            date: 归属日期 YYYY-MM-DD，默认今天。仅用于标记，实际采集实时数据。

        Returns:
            符合 NewsData 结构的字典。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"开始采集 {date} 的新闻数据...")

        # 国内新闻
        domestic: List[str] = []
        sources_attempted = 0

        xinhua = self._collect_xinhua()
        if xinhua:
            domestic.extend(xinhua)
            sources_attempted += 1

        if len(domestic) < 6:
            sina = self._collect_sina_finance()
            if sina:
                domestic.extend(sina)
                sources_attempted += 1

        if len(domestic) < 6:
            eastmoney = self._collect_eastmoney()
            if eastmoney:
                domestic.extend(eastmoney)
                sources_attempted += 1

        # 是否使用默认数据
        is_default = len(domestic) < 3
        if is_default:
            domestic = self._default_domestic_by_date(date)
            overseas = self._default_overseas_by_date(date)
            source = "default"
        else:
            domestic = list(dict.fromkeys(domestic))[:6]
            overseas = self._collect_wallstreetcn()
            if not overseas:
                overseas = self._default_overseas_by_date(date)
            source = f"api({sources_attempted}sources)"

        all_news = domestic[:6] + overseas[:4]

        news = NewsData(
            date=date,
            update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            domestic=domestic[:6],
            overseas=overseas[:4],
            all=all_news,
            source=source,
            retry_count=0,
            is_default=is_default,
        )
        return news.to_dict()

    def save_to_db(self, data: Dict) -> bool:
        """将新闻数据保存到 MySQL (SQLAlchemy)。"""
        self._ensure_db()
        pool = get_db_pool()
        session = pool.get_session("market_data")
        try:
            news_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
            # upsert: 先查再更新/插入
            existing = (
                session.query(FinancialNews)
                .filter(FinancialNews.news_date == news_date)
                .first()
            )
            if existing:
                existing.update_time = data["update_time"]
                existing.domestic_news = data.get("domestic", [])
                existing.overseas_news = data.get("overseas", [])
                existing.all_news = data.get("all", [])
                existing.source = data.get("source", "api")
                existing.is_default = 1 if data.get("is_default") else 0
                existing.retry_count = data.get("retry_count", 0)
                existing.updated_at = datetime.now()
            else:
                row = FinancialNews(
                    news_date=news_date,
                    update_time=data["update_time"],
                    domestic_news=data.get("domestic", []),
                    overseas_news=data.get("overseas", []),
                    all_news=data.get("all", []),
                    source=data.get("source", "api"),
                    is_default=1 if data.get("is_default") else 0,
                    retry_count=data.get("retry_count", 0),
                )
                session.add(row)
            session.commit()
            logger.info(f"新闻数据已保存到 MySQL: {data['date']}")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"保存到 MySQL 失败: {e}")
            return False
        finally:
            session.close()

    def save_to_json(self, data: Dict, filepath: Optional[str] = None) -> bool:
        """将新闻数据保存到本地 JSON 文件。"""
        if filepath is None:
            filepath = str(PROJECT_ROOT / "data" / "news_data.json")
        try:
            out_path = Path(filepath)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"新闻数据已保存到 {filepath}")
            return True
        except Exception as e:
            logger.error(f"保存到 JSON 失败: {e}")
            return False

    def fetch_and_save(self, date: Optional[str] = None) -> Dict[str, Any]:
        """一站式: 采集 -> 存 DB -> 存 JSON。

        Returns:
            采集到的新闻数据字典（附带保存状态）。
        """
        data = self.fetch_news(date)
        db_ok = self.save_to_db(data)
        json_ok = self.save_to_json(data)
        data["_save_db"] = db_ok
        data["_save_json"] = json_ok
        return data


# ---------------------------------------------------------------------------
# Helper function (convenience)
# ---------------------------------------------------------------------------
def fetch_financial_news(date: Optional[str] = None) -> Dict[str, Any]:
    """快捷函数: 采集并保存财经新闻。"""
    fetcher = NewsFetcher()
    return fetcher.fetch_and_save(date)
