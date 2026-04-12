"""
新闻联播数据抓取模块 / CCTV News Broadcast Data Fetcher Module
按日期抓取新闻联播内容并存储到MySQL
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pymysql
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
import os

load_dotenv()


@dataclass
class NewsItem:
    """
    新闻条目数据类 / News Item Data Class
    """
    date: str
    title: str
    content: str
    category: str = "国内新闻"
    source_url: str = ""


@dataclass
class NewsBroadcast:
    """
    新闻联播数据类 / News Broadcast Data Class
    """
    date: str
    main_topics: str
    full_content: str
    source_url: str
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class CCTVFNewsFetcher:
    """
    新闻联播抓取器 / CCTV News Broadcast Fetcher
    从 mrxwlb.com 抓取新闻联播文字版
    """

    BASE_URL = "http://mrxwlb.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        })

    def _build_url(self, date: str) -> str:
        """
        构建新闻联播URL / Build news broadcast URL
        
        Args:
            date: 日期字符串，格式 YYYYMMDD 或 YYYY-MM-DD
        
        Returns:
            完整URL / Full URL
        """
        date_clean = date.replace("-", "")
        year = date_clean[:4]
        month = date_clean[4:6]
        day = date_clean[6:8]
        
        return f"{self.BASE_URL}/{year}/{month}/{day}/{year}年{month}月{day}日新闻联播文字版/"

    def fetch_by_date(self, date: str) -> Optional[NewsBroadcast]:
        """
        按日期抓取新闻联播 / Fetch news broadcast by date
        
        Args:
            date: 日期字符串，格式 YYYYMMDD 或 YYYY-MM-DD
        
        Returns:
            新闻联播数据对象 / News broadcast data object
        """
        url = self._build_url(date)
        
        try:
            response = self.session.get(url, timeout=30)
            response.encoding = "utf-8"
            
            if response.status_code != 200:
                print(f"请求失败，状态码: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            content_div = (
                soup.find("div", class_="entry-content") or
                soup.find("div", class_="post-content") or
                soup.find("div", class_="content") or
                soup.find("article") or
                soup.find("div", class_="article-content") or
                soup.find("div", {"id": "content"}) or
                soup.find("main")
            )
            
            if not content_div:
                content_div = soup.find("div", class_=lambda x: x and "content" in str(x).lower())
            
            if not content_div:
                all_paragraphs = soup.find_all("p")
                if all_paragraphs:
                    full_content = "\n\n".join([p.get_text(strip=True) for p in all_paragraphs if p.get_text(strip=True)])
                    main_topics = self._extract_main_topics_from_text(full_content)
                    date_clean = date.replace("-", "")
                    formatted_date = f"{date_clean[:4]}-{date_clean[4:6]}-{date_clean[6:8]}"
                    return NewsBroadcast(
                        date=formatted_date,
                        main_topics=main_topics,
                        full_content=full_content,
                        source_url=url
                    )
                print("未找到内容区域")
                return None
            
            for script in content_div.find_all("script"):
                script.decompose()
            for style in content_div.find_all("style"):
                style.decompose()
            for nav in content_div.find_all("nav"):
                nav.decompose()
            for footer in content_div.find_all("footer"):
                footer.decompose()
            
            full_content = content_div.get_text(separator="\n", strip=True)
            
            full_content = re.sub(r'\n{3,}', '\n\n', full_content)
            full_content = re.sub(r' {2,}', ' ', full_content)
            
            main_topics = self._extract_main_topics(content_div)
            
            date_clean = date.replace("-", "")
            formatted_date = f"{date_clean[:4]}-{date_clean[4:6]}-{date_clean[6:8]}"
            
            return NewsBroadcast(
                date=formatted_date,
                main_topics=main_topics,
                full_content=full_content,
                source_url=url
            )
            
        except requests.RequestException as e:
            print(f"网络请求错误: {e}")
            return None
        except Exception as e:
            print(f"解析错误: {e}")
            return None

    def _extract_main_topics(self, content_div) -> str:
        """
        提取主要内容标题 / Extract main topic titles
        
        Args:
            content_div: BeautifulSoup元素
        
        Returns:
            主要内容标题列表 / List of main topic titles
        """
        topics = []
        
        for element in content_div.find_all(["h2", "h3", "h4", "strong", "b"]):
            text = element.get_text(strip=True)
            if text and len(text) > 5 and len(text) < 100:
                if "【" in text or "】" in text or "新思想" in text or "联播" in text:
                    topics.append(text)
        
        return "\n".join(topics[:10]) if topics else ""

    def _extract_main_topics_from_text(self, text: str) -> str:
        """
        从文本中提取主要内容标题 / Extract main topic titles from text
        
        Args:
            text: 文本内容
        
        Returns:
            主要内容标题列表 / List of main topic titles
        """
        topics = []
        lines = text.split("\n")
        
        for line in lines:
            line = line.strip()
            if len(line) > 5 and len(line) < 100:
                if "【" in line or "】" in line or "新思想" in line or "联播" in line:
                    topics.append(line)
        
        return "\n".join(topics[:10]) if topics else ""

    def fetch_date_range(self, start_date: str, end_date: str) -> list[NewsBroadcast]:
        """
        抓取日期范围内的新闻联播 / Fetch news broadcast within date range
        
        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        
        Returns:
            新闻联播列表 / List of news broadcasts
        """
        from datetime import timedelta
        
        start = datetime.strptime(start_date.replace("-", ""), "%Y%m%d")
        end = datetime.strptime(end_date.replace("-", ""), "%Y%m%d")
        
        results = []
        current = start
        
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            print(f"正在抓取: {date_str}")
            
            news = self.fetch_by_date(date_str)
            if news:
                results.append(news)
            
            current += timedelta(days=1)
        
        return results


class NewsDatabase:
    """
    新闻数据库操作类 / News Database Operations Class
    """

    def __init__(self):
        self.config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", "xcn_db"),
            "charset": os.getenv("DB_CHARSET", "utf8mb4"),
        }
        self._ensure_table_exists()

    def _get_connection(self):
        """
        获取数据库连接 / Get database connection
        
        Returns:
            数据库连接对象 / Database connection object
        """
        return pymysql.connect(**self.config)

    def _ensure_table_exists(self):
        """
        确保数据表存在 / Ensure database table exists
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cctv_news_broadcast (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            news_date DATE NOT NULL UNIQUE COMMENT '新闻日期',
            main_topics TEXT COMMENT '主要内容标题',
            full_content LONGTEXT COMMENT '完整内容',
            source_url VARCHAR(500) COMMENT '来源URL',
            ai_summary TEXT COMMENT 'AI摘要',
            ai_bullish TEXT COMMENT 'AI利好因素',
            ai_hot_sectors TEXT COMMENT 'AI热门板块',
            ai_leading_stocks TEXT COMMENT 'AI龙头个股',
            ai_macro_guidance TEXT COMMENT 'AI宏观指导',
            ai_risk_alerts TEXT COMMENT 'AI风险提示',
            ai_sentiment VARCHAR(20) COMMENT 'AI情绪判断',
            ai_updated_at TIMESTAMP NULL COMMENT 'AI分析更新时间',
            ai_remarks TEXT COMMENT 'AI分析备注',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            INDEX idx_news_date (news_date),
            INDEX idx_ai_updated_at (ai_updated_at),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='新闻联播内容表';
        """
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                conn.commit()
            print("数据表检查/创建完成")
        except Exception as e:
            print(f"创建数据表失败: {e}")

    def save_news(self, news: NewsBroadcast) -> bool:
        """
        保存新闻联播数据 / Save news broadcast data
        
        Args:
            news: 新闻联播数据对象
        
        Returns:
            是否保存成功 / Whether save was successful
        """
        insert_sql = """
        INSERT INTO cctv_news_broadcast (news_date, main_topics, full_content, source_url)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            main_topics = VALUES(main_topics),
            full_content = VALUES(full_content),
            source_url = VALUES(source_url),
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, (
                        news.date,
                        news.main_topics,
                        news.full_content,
                        news.source_url
                    ))
                conn.commit()
            print(f"保存成功: {news.date}")
            return True
        except Exception as e:
            print(f"保存失败: {e}")
            return False

    def save_news_batch(self, news_list: list[NewsBroadcast]) -> int:
        """
        批量保存新闻联播数据 / Batch save news broadcast data
        
        Args:
            news_list: 新闻联播数据列表
        
        Returns:
            成功保存的数量 / Number of successfully saved items
        """
        success_count = 0
        for news in news_list:
            if self.save_news(news):
                success_count += 1
        return success_count

    def get_news_by_date(self, date: str) -> Optional[dict]:
        """
        按日期查询新闻 / Query news by date
        
        Args:
            date: 日期字符串 YYYY-MM-DD
        
        Returns:
            新闻数据字典 / News data dictionary
        """
        query_sql = """
        SELECT id, news_date, main_topics, full_content, source_url, created_at, updated_at
        FROM cctv_news_broadcast
        WHERE news_date = %s
        """
        
        try:
            with self._get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(query_sql, (date,))
                    return cursor.fetchone()
        except Exception as e:
            print(f"查询失败: {e}")
            return None

    def get_news_date_range(self, start_date: str, end_date: str) -> list[dict]:
        """
        查询日期范围内的新闻 / Query news within date range
        
        Args:
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
        
        Returns:
            新闻数据列表 / List of news data
        """
        query_sql = """
        SELECT id, news_date, main_topics, full_content, source_url, created_at, updated_at
        FROM cctv_news_broadcast
        WHERE news_date BETWEEN %s AND %s
        ORDER BY news_date DESC
        """
        
        try:
            with self._get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(query_sql, (start_date, end_date))
                    return cursor.fetchall()
        except Exception as e:
            print(f"查询失败: {e}")
            return []


def fetch_and_save_news(date: str) -> bool:
    """
    抓取并保存单日新闻 / Fetch and save single day news
    
    Args:
        date: 日期字符串 YYYYMMDD 或 YYYY-MM-DD
    
    Returns:
        是否成功 / Whether successful
    """
    fetcher = CCTVFNewsFetcher()
    db = NewsDatabase()
    
    news = fetcher.fetch_by_date(date)
    if news:
        return db.save_news(news)
    return False


def fetch_and_save_range(start_date: str, end_date: str) -> int:
    """
    抓取并保存日期范围内的新闻 / Fetch and save news within date range
    
    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
    
    Returns:
        成功保存的数量 / Number of successfully saved items
    """
    fetcher = CCTVFNewsFetcher()
    db = NewsDatabase()
    
    news_list = fetcher.fetch_date_range(start_date, end_date)
    return db.save_news_batch(news_list)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法:")
        print("  python cctv_news_fetcher.py YYYYMMDD           # 抓取单日")
        print("  python cctv_news_fetcher.py YYYYMMDD YYYYMMDD  # 抓取日期范围")
        sys.exit(1)
    
    if len(sys.argv) == 2:
        date = sys.argv[1]
        success = fetch_and_save_news(date)
        print(f"抓取{'成功' if success else '失败'}")
    else:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        count = fetch_and_save_range(start_date, end_date)
        print(f"成功保存 {count} 条新闻")
