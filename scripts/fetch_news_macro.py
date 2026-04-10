#!/usr/bin/env python3
"""
宏观新闻采集脚本
【19:35执行】抓取新闻联播及收盘后的重要政策发布
"""
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asztime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MacroNewsFetcher:
    """宏观新闻采集器"""

    def __init__(self):
        self.data_dir = project_root / "data"
        self.news_dir = self.data_dir / "news"
        self.news_dir.mkdir(exist_ok=True)
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.timeout = 30

    def fetch_xinhua_news(self) -> List[Dict]:
        """采集新华网新闻"""
        news_list = []
        try:
            url = "http://search.news.cn.org.cn/SSOSearchService/searchNewsByKeyword"
            params = {
                'keyword': '财经',
                'pageSize': 20,
                'pageNo': 1
            }

            response = requests.get(url, params=params, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                for item in data.get('data', [])[:10]:
                    news_list.append({
                        'title': item.get('title', ''),
                        'source': '新华网',
                        'time': item.get('pubDate', ''),
                        'url': item.get('url', '')
                    })

            logger.info(f"采集到新华网新闻 {len(news_list)} 条")

        except Exception as e:
            logger.warning(f"新华网新闻采集失败: {e}")

        return news_list

    def fetch_sina_finance(self) -> List[Dict]:
        """采集新浪财经新闻"""
        news_list = []
        try:
            url = "https://interface.sina.cn/news/finance/getHot.d.json"
            response = requests.get(url, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                for item in data.get('result', {}).get('data', [])[:10]:
                    news_list.append({
                        'title': item.get('title', ''),
                        'source': '新浪财经',
                        'time': item.get('create_time', ''),
                        'url': item.get('url', '')
                    })

            logger.info(f"采集到新浪财经新闻 {len(news_list)} 条")

        except Exception as e:
            logger.warning(f"新浪财经新闻采集失败: {e}")

        return news_list

    def fetch_eastmoney_policy(self) -> List[Dict]:
        """采集东方财富政策新闻"""
        news_list = []
        try:
            url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 200:
                text = response.text
                pattern = r'"title":"([^"]+)".*?"showtime":"([^"]+)"'
                matches = re.findall(pattern, text)

                for title, time in matches[:10]:
                    news_list.append({
                        'title': title.replace('\\/', '/'),
                        'source': '东方财富',
                        'time': time,
                        'url': ''
                    })

            logger.info(f"采集到东方财富政策新闻 {len(news_list)} 条")

        except Exception as e:
            logger.warning(f"东方财富新闻采集失败: {e}")

        return news_list

    def fetch_stock_news(self) -> List[Dict]:
        """采集股票相关新闻"""
        news_list = []
        try:
            url = "https://stock-api.xueqiu.com/v1/news/list"
            params = {
                'size': 20,
                'symbol': 'SH000001,SZ399001',
                'is_index': 'true'
            }
            headers = {
                'User-Agent': 'Mozilla/5.0'
            }

            response = requests.get(url, params=params, headers=headers, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                for item in data.get('data', []):
                    news_list.append({
                        'title': item.get('title', ''),
                        'source': item.get('source', '雪球'),
                        'time': item.get('created_at', ''),
                        'url': item.get('url', '')
                    })

            logger.info(f"采集到股票新闻 {len(news_list)} 条")

        except Exception as e:
            logger.warning(f"股票新闻采集失败: {e}")

        return news_list

    def filter_market_impact_news(self, news_list: List[Dict]) -> List[Dict]:
        """筛选对市场有影响的新闻关键词"""
        keywords = [
            '央行', '证监会', '银保监会', '财政部', '国务院',
            '降准', '降息', 'IPO', '注册制', '改革',
            '贸易', '关税', '美联储', '加息', '缩表',
            '房地产', '限购', '限贷', 'LPR',
            '新能源', '半导体', '人工智能', '芯片'
        ]

        filtered = []
        for news in news_list:
            title = news.get('title', '').lower()
            if any(kw in title for kw in keywords):
                filtered.append(news)

        return filtered

    def fetch_all_news(self) -> Dict:
        """采集所有新闻"""
        logger.info("开始采集宏观新闻...")

        all_news = {
            'xinhua': self.fetch_xinhua_news(),
            'sina': self.fetch_sina_finance(),
            'eastmoney': self.fetch_eastmoney_policy(),
            'stock': self.fetch_stock_news()
        }

        all_news_list = []
        for source, news in all_news.items():
            all_news_list.extend(news)

        filtered_news = self.filter_market_impact_news(all_news_list)

        return {
            'timestamp': datetime.now().isoformat(),
            'total_count': len(all_news_list),
            'filtered_count': len(filtered_news),
            'all_news': all_news,
            'market_impact_news': filtered_news
        }

    def save_news(self, news_data: Dict) -> Path:
        """保存新闻数据"""
        filename = f"macro_news_{self.today}.json"
        filepath = self.news_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, ensure_ascii=False, indent=2)

        logger.info(f"新闻数据已保存: {filepath}")
        return filepath

    def generate_news_summary(self, news_data: Dict) -> str:
        """生成新闻摘要"""
        lines = []
        lines.append("=" * 60)
        lines.append("【今日重要新闻摘要】")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 60)

        market_news = news_data.get('market_impact_news', [])
        if market_news:
            lines.append(f"\n今日共采集 {len(market_news)} 条可能影响市场的新闻:\n")

            for i, news in enumerate(market_news[:15], 1):
                title = news.get('title', '无标题')
                source = news.get('source', '未知来源')
                time = news.get('time', '')

                lines.append(f"{i}. {title}")
                lines.append(f"   来源: {source} | {time}")
                lines.append("")
        else:
            lines.append("\n今日无重要政策或市场新闻")

        return "\n".join(lines)


def main():
    logger.info("=" * 60)
    logger.info("开始采集宏观新闻")
    logger.info("=" * 60)

    fetcher = MacroNewsFetcher()

    news_data = fetcher.fetch_all_news()

    filepath = fetcher.save_news(news_data)

    summary = fetcher.generate_news_summary(news_data)
    print(summary)

    logger.info("=" * 60)
    logger.info(f"宏观新闻采集完成，共 {news_data['total_count']} 条")
    logger.info(f"其中 {news_data['filtered_count']} 条可能影响市场")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
