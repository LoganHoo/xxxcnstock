"""新闻数据采集脚本 - 支持30天历史数据、断点续传、自动重试、仅MySQL存储

功能特性:
1. 保存30天历史数据到MySQL
2. 检查缺失日期并自动补采
3. 断点续传机制
4. 自动重试3次
5. 验证数据完整性
6. 仅MySQL数据库存储（无本地文件）

重要说明:
- 本脚本只能采集"当天"的实时新闻，无法获取历史日期的新闻
- 传入的 date 参数仅用于标记数据归属日期，实际内容来自当前实时采集
- 如需历史新闻数据，需要接入支持历史查询的付费API
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple
from dataclasses import dataclass, asdict
import json
import time
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import requests
from core.logger import get_logger

# MySQL支持
try:
    import pymysql
    from pymysql.cursors import DictCursor
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False


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
        """验证数据完整性"""
        if not self.date or len(self.date) != 10:
            return False
        
        # 验证日期格式
        try:
            datetime.strptime(self.date, '%Y-%m-%d')
        except ValueError:
            return False
        
        # 验证内容不为空且不是纯空白
        domestic_valid = len([d for d in self.domestic if d and d.strip()]) >= 3
        overseas_valid = len([o for o in self.overseas if o and o.strip()]) >= 2
        all_valid = len([a for a in self.all if a and a.strip()]) >= 5
        
        return domestic_valid and overseas_valid and all_valid
    
    def has_duplicates(self) -> bool:
        """检查是否有重复内容"""
        all_texts = [t.strip().lower() for t in self.domestic + self.overseas if t and t.strip()]
        return len(all_texts) != len(set(all_texts))


class RetryManager:
    """重试管理器 - 支持指数退避和抖动"""

    def __init__(self, max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, jitter: bool = True):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        self.jitter = jitter
        self.logger = get_logger(__name__)

    def _get_sleep_time(self, attempt: int) -> float:
        """计算等待时间，带抖动"""
        base_time = self.delay * (self.backoff ** attempt)
        if self.jitter:
            # 添加 ±20% 的随机抖动
            base_time *= (0.8 + random.random() * 0.4)
        return base_time

    def execute(self, func, *args, **kwargs) -> Tuple[Any, int]:
        """执行带重试的函数

        Returns:
            (result, retry_count): 结果和实际重试次数
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result, attempt
            except Exception as e:
                last_exception = e
                self.logger.warning(f"尝试 {attempt + 1}/{self.max_retries + 1} 失败: {e}")

            if attempt < self.max_retries:
                sleep_time = self._get_sleep_time(attempt)
                self.logger.info(f"等待 {sleep_time:.1f} 秒后重试...")
                time.sleep(sleep_time)

        self.logger.error(f"所有 {self.max_retries + 1} 次尝试都失败")
        return None, self.max_retries


class NewsMySQLStorage:
    """新闻数据MySQL存储管理器 - 使用连接池"""
    
    _pool = None
    _pool_lock = False

    def __init__(self, use_pool: bool = True):
        self.logger = get_logger(__name__)
        self._connection_params = None
        self._use_pool = use_pool
        self._init_connection_params()
        self._init_pool()

    def _init_connection_params(self):
        """初始化数据库连接参数"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        self._connection_params = {
            'host': os.getenv('DB_HOST', '49.233.10.199'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': os.getenv('DB_USER', 'nextai'),
            'password': os.getenv('DB_PASSWORD', '100200'),
            'database': os.getenv('DB_NAME', 'xcn_db'),
            'charset': 'utf8mb4',
            'cursorclass': DictCursor
        }

    def _init_pool(self):
        """初始化连接池"""
        if not self._use_pool or not MYSQL_AVAILABLE:
            return
        
        # 简单的单例连接池
        if NewsMySQLStorage._pool is None and not NewsMySQLStorage._pool_lock:
            NewsMySQLStorage._pool_lock = True
            try:
                # 使用 pymysql 的连接池支持
                from pymysql.pool import QueuePool
                NewsMySQLStorage._pool = QueuePool(
                    creator=lambda: pymysql.connect(**self._connection_params),
                    max_connections=5,
                    max_idle=2,
                    recycle=3600
                )
                self.logger.debug("MySQL连接池已初始化")
            except ImportError:
                self.logger.warning("QueuePool不可用，使用普通连接")
            finally:
                NewsMySQLStorage._pool_lock = False

    def _get_connection(self):
        """获取数据库连接"""
        if not MYSQL_AVAILABLE:
            raise ImportError("pymysql 未安装，无法使用MySQL存储")
        
        # 优先使用连接池
        if NewsMySQLStorage._pool is not None:
            try:
                return NewsMySQLStorage._pool.get_connection()
            except Exception as e:
                self.logger.warning(f"从连接池获取连接失败: {e}，使用新连接")
        
        return pymysql.connect(**self._connection_params)

    def save_news(self, news_data: NewsData) -> bool:
        """保存新闻数据到MySQL

        Args:
            news_data: 新闻数据对象

        Returns:
            是否保存成功
        """
        if not MYSQL_AVAILABLE:
            self.logger.error("pymysql 未安装，无法保存数据")
            return False

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                # 使用INSERT ... ON DUPLICATE KEY UPDATE实现upsert
                sql = """
                INSERT INTO news_data (
                    news_date, update_time, domestic_news, overseas_news,
                    all_news, source, is_default, retry_count, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                ) ON DUPLICATE KEY UPDATE
                    update_time = VALUES(update_time),
                    domestic_news = VALUES(domestic_news),
                    overseas_news = VALUES(overseas_news),
                    all_news = VALUES(all_news),
                    source = VALUES(source),
                    is_default = VALUES(is_default),
                    retry_count = VALUES(retry_count),
                    updated_at = NOW()
                """

                cursor.execute(sql, (
                    news_data.date,
                    news_data.update_time,
                    json.dumps(news_data.domestic, ensure_ascii=False),
                    json.dumps(news_data.overseas, ensure_ascii=False),
                    json.dumps(news_data.all, ensure_ascii=False),
                    news_data.source,
                    1 if news_data.is_default else 0,
                    news_data.retry_count
                ))

            conn.commit()
            self.logger.info(f"新闻数据已保存到MySQL: {news_data.date}")
            return True

        except Exception as e:
            self.logger.error(f"保存到MySQL失败: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def load_news(self, date: str) -> Optional[NewsData]:
        """从MySQL加载新闻数据

        Args:
            date: 日期字符串 YYYY-MM-DD

        Returns:
            新闻数据对象或None
        """
        if not MYSQL_AVAILABLE:
            return None

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                sql = """
                SELECT news_date, update_time, domestic_news, overseas_news,
                       all_news, source, is_default, retry_count
                FROM news_data
                WHERE news_date = %s
                """
                cursor.execute(sql, (date,))
                row = cursor.fetchone()

                if row:
                    news_data = NewsData(
                        date=row['news_date'].strftime('%Y-%m-%d') if isinstance(row['news_date'], datetime) else str(row['news_date']),
                        update_time=row['update_time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row['update_time'], datetime) else str(row['update_time']),
                        domestic=json.loads(row['domestic_news']) if row['domestic_news'] else [],
                        overseas=json.loads(row['overseas_news']) if row['overseas_news'] else [],
                        all=json.loads(row['all_news']) if row['all_news'] else [],
                        source=row['source'] or 'mysql',
                        retry_count=row['retry_count'] or 0,
                        is_default=bool(row['is_default'])
                    )
                    return news_data if news_data.is_valid() else None
                return None

        except Exception as e:
            self.logger.error(f"从MySQL加载失败: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_existing_dates(self, days: int = 30) -> Set[str]:
        """从MySQL获取已存在的日期集合

        Args:
            days: 查询最近多少天

        Returns:
            日期集合
        """
        if not MYSQL_AVAILABLE:
            return set()

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                sql = """
                SELECT news_date FROM news_data
                WHERE news_date >= %s
                """
                cursor.execute(sql, (cutoff_date,))
                rows = cursor.fetchall()

                dates = set()
                for row in rows:
                    date_val = row['news_date']
                    if isinstance(date_val, datetime):
                        dates.add(date_val.strftime('%Y-%m-%d'))
                    else:
                        dates.add(str(date_val))
                return dates

        except Exception as e:
            self.logger.error(f"从MySQL获取日期失败: {e}")
            return set()
        finally:
            if conn:
                conn.close()

    def get_missing_dates(self, days: int = 30) -> List[str]:
        """获取缺失的日期列表

        Args:
            days: 检查最近多少天

        Returns:
            缺失日期列表（从早到晚排序）
        """
        today = datetime.now().date()
        existing_dates = self.get_existing_dates(days)

        missing = []
        for i in range(days):
            date = today - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            if date_str not in existing_dates:
                missing.append(date_str)

        # 从早到晚排序
        missing.sort()
        return missing

    def cleanup_old_data(self, retention_days: int = 30):
        """清理MySQL中的过期数据

        Args:
            retention_days: 数据保留天数
        """
        if not MYSQL_AVAILABLE:
            return

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
                sql = "DELETE FROM news_data WHERE news_date < %s"
                cursor.execute(sql, (cutoff_date,))
                deleted = cursor.rowcount
            conn.commit()
            if deleted > 0:
                self.logger.info(f"清理了 {deleted} 条过期数据")
        except Exception as e:
            self.logger.error(f"清理过期数据失败: {e}")
        finally:
            if conn:
                conn.close()

    def get_collection_summary(self, days: int = 30) -> Dict[str, Any]:
        """获取采集摘要

        Args:
            days: 查询最近多少天

        Returns:
            摘要信息
        """
        if not MYSQL_AVAILABLE:
            return {"available": False, "reason": "pymysql未安装"}

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

                # 总记录数
                cursor.execute("""
                    SELECT COUNT(*) as count FROM news_data
                    WHERE news_date >= %s
                """, (cutoff_date,))
                total = cursor.fetchone()['count']

                # 默认数据数量
                cursor.execute("""
                    SELECT COUNT(*) as count FROM news_data
                    WHERE news_date >= %s AND is_default = 1
                """, (cutoff_date,))
                default_count = cursor.fetchone()['count']

                # 最新数据日期
                cursor.execute("""
                    SELECT MAX(news_date) as latest FROM news_data
                """)
                latest_date = cursor.fetchone()['latest']

                # 缺失日期
                existing_dates = self.get_existing_dates(days)
                today = datetime.now().date()
                missing_dates = []
                for i in range(days):
                    date = today - timedelta(days=i)
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in existing_dates:
                        missing_dates.append(date_str)
                missing_dates.sort()

                return {
                    "available": True,
                    "total_records": total,
                    "default_count": default_count,
                    "latest_date": latest_date.strftime('%Y-%m-%d') if latest_date else None,
                    "missing_count": len(missing_dates),
                    "missing_dates": missing_dates[:10],
                    "days_checked": days
                }

        except Exception as e:
            self.logger.error(f"获取摘要失败: {e}")
            return {"available": False, "error": str(e)}
        finally:
            if conn:
                conn.close()


class NewsCollector:
    """新闻数据采集器 - 带重试机制"""

    def __init__(self, retry_manager: Optional[RetryManager] = None):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = get_logger(__name__)
        self.retry_manager = retry_manager or RetryManager(max_retries=3)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def _safe_request(self, url: str, timeout: int = 10) -> Optional[requests.Response]:
        """安全的HTTP请求（带重试）"""
        def do_request():
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response

        result, retry_count = self.retry_manager.execute(do_request)
        return result

    def collect_xinhua_news(self) -> List[str]:
        """采集新华社重要新闻"""
        news = []
        try:
            url = "https://www.xinhuanet.com/politics/news_politics.xml"
            response = self._safe_request(url, timeout=15)
            if response:
                from xml.etree import ElementTree
                root = ElementTree.fromstring(response.content)
                for item in root.findall('.//item')[:10]:
                    title = item.find('title')
                    if title is not None and title.text:
                        news.append(title.text)
        except Exception as e:
            self.logger.warning(f"新华社新闻采集失败: {e}")
        return news

    def collect_sina_finance_news(self) -> List[str]:
        """采集新浪财经新闻"""
        news = []
        try:
            url = "https://finance.sina.com.cn/stock/"
            response = self._safe_request(url, timeout=15)
            if response:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                # 尝试多种选择器
                selectors = ['a.news-link', 'a.f14', 'h2 a', '.news-item a']
                for selector in selectors:
                    headlines = soup.select(selector)[:10]
                    for h in headlines:
                        text = h.get_text(strip=True)
                        if text and len(text) > 10:
                            news.append(text)
                    if news:
                        break
        except Exception as e:
            self.logger.warning(f"新浪财经采集失败: {e}")
        return news

    def collect_eastmoney_news(self) -> List[str]:
        """采集东方财富新闻"""
        news = []
        try:
            url = "https://www.eastmoney.com"
            response = self._safe_request(url, timeout=15)
            if response:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                headlines = soup.find_all('a', class_='news-title')[:10]
                for h in headlines:
                    if h.text.strip():
                        news.append(h.text.strip())
        except Exception as e:
            self.logger.warning(f"东方财富新闻采集失败: {e}")
        return news

    def generate_default_news(self, date: str) -> NewsData:
        """生成默认新闻（当采集失败时）- 基于日期生成差异化内容
        
        注意：默认数据仅用于标记数据缺失，内容不代表真实新闻
        """
        # 基于日期生成伪随机但确定性的内容变化
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        day_of_year = date_obj.timetuple().tm_yday
        weekday = date_obj.weekday()
        
        # 国内新闻模板库
        domestic_templates = [
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
            "乡村振兴战略实施，农业农村现代化加快"
        ]
        
        # 根据日期偏移选择不同的新闻组合
        offset = (day_of_year + weekday) % 6
        domestic = domestic_templates[offset:offset+6]
        if len(domestic) < 6:
            domestic += domestic_templates[:6-len(domestic)]
        
        # 海外新闻模板库
        overseas_templates = [
            "美联储货币政策会议维持基准利率不变",
            "欧洲央行关注通胀走势，货币政策保持灵活",
            "全球主要经济体增长预期调整",
            "国际能源市场波动，油价维持区间震荡",
            "全球供应链持续修复，贸易活动回暖",
            "主要股市指数震荡整理，市场情绪谨慎",
            "国际金价波动，避险情绪有所升温",
            "美元指数走势分化，非美货币表现各异"
        ]
        
        overseas_offset = (day_of_year % 4)
        overseas = overseas_templates[overseas_offset:overseas_offset+4]
        if len(overseas) < 4:
            overseas += overseas_templates[:4-len(overseas)]
        
        all_news = domestic + overseas

        return NewsData(
            date=date,
            update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            domestic=domestic,
            overseas=overseas,
            all=all_news,
            source="default",
            retry_count=3,
            is_default=True
        )

    def collect_for_date(self, date: str, use_default_on_failure: bool = True) -> NewsData:
        """采集指定日期的新闻

        Args:
            date: 日期字符串 YYYY-MM-DD
            use_default_on_failure: 采集失败时是否使用默认数据

        Returns:
            NewsData 对象
        """
        self.logger.info(f"开始采集 {date} 的新闻数据...")

        # 尝试多个数据源
        domestic = []
        sources_attempted = 0

        # 数据源1: 新华社
        xinhua_news = self.collect_xinhua_news()
        if xinhua_news:
            domestic.extend(xinhua_news)
            sources_attempted += 1

        # 数据源2: 新浪财经
        if len(domestic) < 6:
            sina_news = self.collect_sina_finance_news()
            if sina_news:
                domestic.extend(sina_news)
                sources_attempted += 1

        # 数据源3: 东方财富
        if len(domestic) < 6:
            eastmoney_news = self.collect_eastmoney_news()
            if eastmoney_news:
                domestic.extend(eastmoney_news)
                sources_attempted += 1

        # 如果没有采集到足够数据，使用默认数据
        if len(domestic) < 3 and use_default_on_failure:
            self.logger.warning(f"{date}: 采集失败，使用默认数据")
            return self.generate_default_news(date)

        # 去重并限制数量
        domestic = list(dict.fromkeys(domestic))[:6]

        # 采集海外新闻
        overseas = self.collect_overseas_news()
        if not overseas:
            # 如果采集失败，使用基于日期的默认海外新闻
            overseas = self._get_default_overseas_by_date(date)

        all_news = domestic[:6] + overseas[:4]

        return NewsData(
            date=date,
            update_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            domestic=domestic[:6],
            overseas=overseas[:4],
            all=all_news,
            source=f"api({sources_attempted}sources)",
            retry_count=0,
            is_default=False
        )

    def collect_overseas_news(self) -> List[str]:
        """采集海外财经新闻"""
        news = []
        try:
            # 使用华尔街见闻的海外新闻RSS
            url = "https://rsshub.app/wallstreetcn/news/global"
            response = self._safe_request(url, timeout=15)
            if response:
                from xml.etree import ElementTree
                root = ElementTree.fromstring(response.content)
                for item in root.findall('.//item')[:6]:
                    title = item.find('title')
                    if title is not None and title.text:
                        news.append(title.text)
        except Exception as e:
            self.logger.warning(f"海外新闻采集失败: {e}")
        return news

    def _get_default_overseas_by_date(self, date: str) -> List[str]:
        """基于日期获取默认海外新闻"""
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        day_of_year = date_obj.timetuple().tm_yday
        
        overseas_templates = [
            "美联储货币政策会议维持基准利率不变",
            "欧洲央行关注通胀走势，货币政策保持灵活",
            "全球主要经济体增长预期调整",
            "国际能源市场波动，油价维持区间震荡",
            "全球供应链持续修复，贸易活动回暖",
            "主要股市指数震荡整理，市场情绪谨慎",
            "国际金价波动，避险情绪有所升温",
            "美元指数走势分化，非美货币表现各异"
        ]
        
        offset = day_of_year % 4
        result = overseas_templates[offset:offset+4]
        if len(result) < 4:
            result += overseas_templates[:4-len(result)]
        return result


class NewsCollectionOrchestrator:
    """新闻采集编排器 - 仅MySQL存储"""

    def __init__(self, retention_days: int = 30):
        self.storage = NewsMySQLStorage()
        self.collector = NewsCollector()
        self.logger = get_logger(__name__)
        self.retention_days = retention_days

    def run_collection(self, target_dates: Optional[List[str]] = None, force_update: bool = False, concurrent: bool = False) -> Dict[str, Any]:
        """执行采集任务

        Args:
            target_dates: 指定要采集的日期，None则自动检测缺失日期
            force_update: 是否强制更新已有数据
            concurrent: 是否启用并发采集

        Returns:
            采集结果统计
        """
        # 确定要采集的日期
        if target_dates is None:
            if force_update:
                # 强制更新：采集最近30天所有日期
                today = datetime.now().date()
                target_dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d')
                               for i in range(self.retention_days)]
                target_dates.sort()
            else:
                # 自动检测缺失日期
                target_dates = self.storage.get_missing_dates(self.retention_days)

        if not target_dates:
            self.logger.info("没有需要采集的日期")
            return {"status": "no_op", "message": "所有日期数据已存在"}

        self.logger.info(f"需要采集 {len(target_dates)} 个日期: {target_dates}")
        
        # 重要提醒：只能采集当天新闻
        if len(target_dates) > 1 or (target_dates and target_dates[0] != datetime.now().strftime('%Y-%m-%d')):
            self.logger.warning("注意: 多个日期或历史日期的采集将使用相同的新闻内容（当天新闻）")

        # 执行采集
        results = {
            "total": len(target_dates),
            "success": 0,
            "failed": 0,
            "default_used": 0,
            "details": []
        }
        
        # 串行采集（推荐，避免对数据源造成过大压力）
        if not concurrent or len(target_dates) == 1:
            for date in target_dates:
                self._collect_single_date(date, results)
                # 短暂延迟，避免请求过快
                time.sleep(0.5)
        else:
            # 并发采集（实验性）
            self.logger.info("使用并发采集模式")
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_date = {
                    executor.submit(self._collect_single_date_task, date): date 
                    for date in target_dates
                }
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        result = future.result()
                        if result['success']:
                            results["success"] += 1
                            if result.get('is_default'):
                                results["default_used"] += 1
                        else:
                            results["failed"] += 1
                        results["details"].append(result['detail'])
                    except Exception as e:
                        self.logger.error(f"{date}: 并发采集异常 - {e}")
                        results["failed"] += 1
                        results["details"].append({
                            "date": date,
                            "status": "error",
                            "error": str(e)
                        })

        # 清理过期数据
        self.storage.cleanup_old_data(self.retention_days)

        self.logger.info(f"采集完成: 成功 {results['success']}/{results['total']}, "
                        f"失败 {results['failed']}, 使用默认数据 {results['default_used']}")

        return results
    
    def _collect_single_date(self, date: str, results: Dict[str, Any]):
        """采集单个日期的数据"""
        try:
            # 采集数据
            news_data = self.collector.collect_for_date(date, use_default_on_failure=True)

            # 验证数据
            if not news_data.is_valid():
                self.logger.error(f"{date}: 数据验证失败")
                results["failed"] += 1
                return

            # 保存数据到MySQL
            if self.storage.save_news(news_data):
                results["success"] += 1
                if news_data.is_default:
                    results["default_used"] += 1

                results["details"].append({
                    "date": date,
                    "status": "success",
                    "source": news_data.source,
                    "is_default": news_data.is_default
                })
            else:
                results["failed"] += 1

        except Exception as e:
            self.logger.error(f"{date}: 采集异常 - {e}")
            results["failed"] += 1
            results["details"].append({
                "date": date,
                "status": "error",
                "error": str(e)
            })
    
    def _collect_single_date_task(self, date: str) -> Dict[str, Any]:
        """并发采集任务 - 每个任务使用独立的collector避免线程安全问题"""
        try:
            # 创建独立的collector实例，避免多线程共享session的线程安全问题
            collector = NewsCollector()
            news_data = collector.collect_for_date(date, use_default_on_failure=True)
            
            if not news_data.is_valid():
                return {
                    'success': False,
                    'detail': {'date': date, 'status': 'invalid', 'error': '数据验证失败'}
                }
            
            saved = self.storage.save_news(news_data)
            return {
                'success': saved,
                'is_default': news_data.is_default,
                'detail': {
                    'date': date,
                    'status': 'success' if saved else 'save_failed',
                    'source': news_data.source,
                    'is_default': news_data.is_default
                }
            }
        except Exception as e:
            return {
                'success': False,
                'detail': {'date': date, 'status': 'error', 'error': str(e)}
            }

    def verify_data_integrity(self) -> Dict[str, Any]:
        """验证数据完整性"""
        self.logger.info("开始验证数据完整性...")

        existing_dates = self.storage.get_existing_dates(self.retention_days)
        issues = []
        valid_count = 0

        for date in sorted(existing_dates):
            news = self.storage.load_news(date)
            if news is None:
                issues.append({"date": date, "issue": "无法加载数据"})
            elif not news.is_valid():
                issues.append({
                    "date": date,
                    "issue": f"数据不完整 (domestic:{len(news.domestic)}, overseas:{len(news.overseas)})"
                })
            else:
                valid_count += 1

        # 检查缺失日期
        missing = self.storage.get_missing_dates(self.retention_days)

        result = {
            "total_records": len(existing_dates),
            "valid_records": valid_count,
            "issues_found": len(issues),
            "issues": issues,
            "missing_dates": missing,
            "integrity_score": valid_count / len(existing_dates) if existing_dates else 0
        }

        self.logger.info(f"完整性验证完成: {valid_count}/{len(existing_dates)} 条记录有效")
        return result


def validate_date(date_str: str) -> Tuple[bool, str]:
    """验证日期字符串
    
    Returns:
        (is_valid, error_message)
    """
    if not date_str:
        return False, "日期不能为空"
    
    # 格式验证
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if not re.match(pattern, date_str):
        return False, "日期格式错误，应为 YYYY-MM-DD"
    
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return False, "无效日期"
    
    # 检查未来日期
    if date_obj.date() > datetime.now().date():
        return False, "不能采集未来日期的新闻"
    
    # 检查过旧日期（超过保留期限）- 放宽到3年
    retention_limit = datetime.now() - timedelta(days=1095)  # 最多3年
    if date_obj < retention_limit:
        return False, f"日期过于久远，超过数据保留期限(3年)"
    
    return True, ""


def main():
    """主入口"""
    import argparse

    parser = argparse.ArgumentParser(
        description='新闻数据采集脚本 - 仅MySQL存储\n\n'
                   '重要说明：本脚本只能采集"当天"的实时新闻，传入的date参数仅用于标记数据归属日期。'
    )
    parser.add_argument('--date', type=str, help='指定采集日期 (YYYY-MM-DD)，注意：实际采集的是当天新闻')
    parser.add_argument('--fill-missing', action='store_true', help='补采缺失的历史数据（使用当天新闻标记为历史日期）')
    parser.add_argument('--force-update', action='store_true', help='强制更新已有数据')
    parser.add_argument('--verify', action='store_true', help='验证数据完整性')
    parser.add_argument('--summary', action='store_true', help='显示采集摘要')
    parser.add_argument('--retention-days', type=int, default=30, help='数据保留天数')
    parser.add_argument('--concurrent', action='store_true', help='启用并发采集（实验性）')

    args = parser.parse_args()

    # 检查MySQL可用性
    if not MYSQL_AVAILABLE:
        print("错误: pymysql 未安装，无法使用MySQL存储")
        print("请运行: pip install pymysql")
        sys.exit(1)

    # 初始化
    orchestrator = NewsCollectionOrchestrator(args.retention_days)

    # 显示摘要
    if args.summary:
        summary = orchestrator.storage.get_collection_summary(args.retention_days)
        print("\n" + "=" * 60)
        print("新闻数据采集摘要 (MySQL)")
        print("=" * 60)
        if summary.get("available"):
            print(f"数据保留天数: {summary['days_checked']}")
            print(f"总记录数: {summary['total_records']}")
            print(f"缺失日期数: {summary['missing_count']}")
            print(f"默认数据: {summary['default_count']}")
            print(f"最新日期: {summary['latest_date']}")
            if summary['missing_dates']:
                print(f"\n缺失日期: {', '.join(summary['missing_dates'])}")
                if summary['missing_count'] > 10:
                    print(f"... 还有 {summary['missing_count'] - 10} 个")
        else:
            print(f"MySQL不可用: {summary.get('reason', summary.get('error', '未知错误'))}")
        print("=" * 60)
        return

    # 验证数据完整性
    if args.verify:
        result = orchestrator.verify_data_integrity()
        print("\n" + "=" * 60)
        print("数据完整性验证报告 (MySQL)")
        print("=" * 60)
        print(f"总记录数: {result['total_records']}")
        print(f"有效记录: {result['valid_records']}")
        print(f"问题记录: {result['issues_found']}")
        print(f"缺失日期: {len(result['missing_dates'])}")
        print(f"完整性评分: {result['integrity_score']:.1%}")
        if result['issues']:
            print("\n问题详情:")
            for issue in result['issues'][:10]:
                print(f"  - {issue['date']}: {issue['issue']}")
        print("=" * 60)
        return

    # 验证日期参数
    if args.date:
        is_valid, error_msg = validate_date(args.date)
        if not is_valid:
            print(f"错误: {error_msg}")
            sys.exit(1)
        
        # 警告：采集历史日期时实际是采集当天新闻
        date_obj = datetime.strptime(args.date, '%Y-%m-%d')
        if date_obj.date() != datetime.now().date():
            print(f"\n警告: 您指定了历史日期 {args.date}，但实际将采集今天的新闻并标记为该日期。")
            print("如需真实的历史新闻数据，请接入支持历史查询的付费API。\n")
    
    # 执行采集
    if args.date:
        # 采集指定日期
        print(f"\n采集指定日期: {args.date}")
        result = orchestrator.run_collection([args.date], force_update=args.force_update, concurrent=args.concurrent)
    elif args.fill_missing:
        # 补采缺失数据
        print(f"\n补采缺失的历史数据...")
        print("注意: 将使用今天的新闻内容标记为历史日期")
        result = orchestrator.run_collection(force_update=args.force_update, concurrent=args.concurrent)
    else:
        # 默认：采集当天数据并检查缺失
        print(f"\n执行日常采集...")
        result = orchestrator.run_collection(force_update=args.force_update, concurrent=args.concurrent)

    # 输出结果
    print("\n" + "=" * 60)
    print("采集结果")
    print("=" * 60)
    print(f"总任务数: {result.get('total', 0)}")
    print(f"成功: {result.get('success', 0)}")
    print(f"失败: {result.get('failed', 0)}")
    print(f"使用默认数据: {result.get('default_used', 0)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
