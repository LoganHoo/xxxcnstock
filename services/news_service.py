"""新闻联播数据服务 - 从MySQL获取新闻联播数据"""
import os
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pymysql
from pymysql.cursors import DictCursor


logger = logging.getLogger(__name__)


class NewsService:
    """新闻联播数据服务"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', '49.233.10.199'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'nextai'),
            'password': os.getenv('DB_PASSWORD', '100200'),
            'database': os.getenv('DB_NAME', 'xcn_db'),
            'charset': 'utf8mb4',
            'cursorclass': DictCursor
        }
    
    def _get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.db_config)
    
    def get_latest_news(self, days: int = 7) -> List[Dict]:
        """
        获取最近N天的新闻联播
        
        Args:
            days: 获取天数
            
        Returns:
            新闻列表
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT news_date, main_topics, full_content
                    FROM cctv_news_broadcast
                    ORDER BY news_date DESC
                    LIMIT %s
                """, (days,))
                results = cursor.fetchall()
                
                news_list = []
                for row in results:
                    news_list.append({
                        'date': row['news_date'].strftime('%Y-%m-%d') if row['news_date'] else '',
                        'topics': row['main_topics'] or '',
                        'content': row['full_content'] or ''
                    })
                
                return news_list
        finally:
            conn.close()
    
    def get_yesterday_news(self) -> Optional[Dict]:
        """
        获取昨日新闻联播（用于决策报告）
        
        Returns:
            昨日新闻字典
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT news_date, main_topics, full_content
                    FROM cctv_news_broadcast
                    WHERE news_date = %s
                    LIMIT 1
                """, (yesterday,))
                row = cursor.fetchone()
                
                if row:
                    return {
                        'date': row['news_date'].strftime('%Y-%m-%d') if row['news_date'] else '',
                        'topics': row['main_topics'] or '',
                        'content': row['full_content'] or ''
                    }
                return None
        finally:
            conn.close()
    
    def extract_keywords(self, news: Dict) -> List[str]:
        """
        从新闻中提取关键词
        
        Args:
            news: 新闻字典
            
        Returns:
            关键词列表
        """
        keywords = []
        topics = news.get('topics', '')
        
        # 常见政策关键词
        policy_keywords = [
            '新质生产力', '低空经济', '人工智能', '集成电路',
            '新能源', '光伏', '风电', '储能', '氢能',
            '数字经济', '智能制造', '工业母机', '机器人',
            '生物医药', '量子计算', '脑机接口', '6G',
            '城市更新', '保障房', '家电下乡', '汽车以旧换新',
            '一带一路', '乡村振兴', '共同富裕'
        ]
        
        for kw in policy_keywords:
            if kw in topics or kw in (news.get('content', '') or ''):
                keywords.append(kw)
        
        return keywords[:5]  # 最多返回5个
    
    def generate_summary(self, news: Dict) -> str:
        """
        生成新闻摘要
        
        Args:
            news: 新闻字典
            
        Returns:
            摘要文本
        """
        topics = news.get('topics', '')
        if not topics:
            return '昨日无重要新闻'
        
        # 提取前3个要点
        lines = topics.split('\n')
        key_points = [line.strip() for line in lines if line.strip() and '【' in line][:3]
        
        if key_points:
            return ' | '.join(key_points)
        return topics[:100]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    service = NewsService()
    
    # 测试获取昨日新闻
    yesterday_news = service.get_yesterday_news()
    if yesterday_news:
        print(f"昨日新闻: {yesterday_news['date']}")
        print(f"关键词: {service.extract_keywords(yesterday_news)}")
        print(f"摘要: {service.generate_summary(yesterday_news)}")
    else:
        print("未找到昨日新闻")
