"""
AI分析模块 / AI Analysis Module
分析新闻联播内容与股票市场、经济的关系
"""

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List

import pymysql
import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

load_dotenv()


@dataclass
class NewsAnalysis:
    """
    新闻分析结果数据类 / News Analysis Result Data Class
    """
    date: str
    summary: str
    bullish: str
    hot_sectors: str
    leading_stocks: str
    macro_guidance: str
    risk_alerts: str
    overall_sentiment: str


class AIAnalyzer:
    """
    AI分析器 / AI Analyzer
    使用Ollama API分析新闻内容
    """

    def __init__(self):
        self.base_url = os.getenv("OLLAMA_BASE_URL", "http://aia.newoffen.com:11434")
        self.model = os.getenv("OLLAMA_MODEL", "deepseek-r1:1.5b")

    def analyze_news(self, news_content: str, news_date: str) -> Optional[NewsAnalysis]:
        """
        分析新闻内容 / Analyze news content
        
        Args:
            news_content: 新闻内容
            news_date: 新闻日期
        
        Returns:
            分析结果 / Analysis result
        """
        prompt = self._build_analysis_prompt(news_content, news_date)
        analysis_text = self._call_api(prompt)
        
        if analysis_text:
            return self._parse_analysis_result(news_date, analysis_text)
        return self._mock_analysis(news_date, news_content)

    def _call_api(self, prompt: str) -> Optional[str]:
        """
        调用AI API / Call AI API
        优先使用chat接口，失败则使用generate接口
        """
        try:
            response = requests.post(
                f"{self.base_url}/api/chat",
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "options": {
                        "temperature": 0.7,
                        "num_predict": 2000
                    }
                },
                timeout=600,
                proxies={"http": None, "https": None}
            )

            if response.status_code == 200:
                result = response.json()
                content = result.get("message", {}).get("content", "")
                if content:
                    return content
        except Exception as e:
            print(f"Chat API调用失败: {e}")

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.7,
                        "num_predict": 2000
                    }
                },
                timeout=600,
                proxies={"http": None, "https": None}
            )

            if response.status_code == 200:
                result = response.json()
                return result.get("response", "")
        except Exception as e:
            print(f"Generate API调用失败: {e}")

        return None

    def _build_analysis_prompt(self, news_content: str, news_date: str) -> str:
        """
        构建分析提示词 / Build analysis prompt
        股票资深研究员风格，聚焦宏观指导、利好、热门板块、龙头
        """
        return f"""假设你是中国股票资深研究员，擅长从新闻联播中挖掘投资机会。

任务：分析{news_date}新闻联播内容，从A股投资角度给出专业研报。

【新闻内容】
{news_content[:4000]}

请严格按照以下JSON格式输出分析结果（不要输出其他内容）：

{{
  "summary": "用3-5句话概括今日新闻核心要点",
  "bullish": "明确列出新闻对A股的利好因素（如有）",
  "hot_sectors": "识别当日热门板块/题材",
  "leading_stocks": "识别可能成为龙头的个股（代码+名称+逻辑）",
  "macro_guidance": "宏观层面的操作指导",
  "risk_alerts": "提示可能的风险因素",
  "overall_sentiment": "积极/中性/谨慎"
}}

现在请开始分析并输出JSON结果："""

    def _parse_analysis_result(self, news_date: str, analysis_text: str) -> NewsAnalysis:
        """
        解析分析结果 / Parse analysis result
        支持JSON格式和Markdown格式
        """
        import json
        import re
        
        sections = {
            "summary": "",
            "bullish": "",
            "hot_sectors": "",
            "leading_stocks": "",
            "macro_guidance": "",
            "risk_alerts": "",
            "overall_sentiment": ""
        }

        json_match = re.search(r'\{[^{}]*\}', analysis_text, re.DOTALL)
        if json_match:
            try:
                json_data = json.loads(json_match.group())
                for key in sections:
                    value = json_data.get(key, "")
                    if isinstance(value, list):
                        sections[key] = "\n".join(str(v) for v in value)
                    else:
                        sections[key] = str(value) if value else ""
                sections["overall_sentiment"] = sections.get("overall_sentiment", "中性") or "中性"
            except json.JSONDecodeError:
                pass

        if not sections["summary"]:
            current_section = None
            section_mapping = {
                "一、新闻摘要": "summary",
                "二、市场影响分析": "market_impact",
                "三、板块机会分析": "sector_analysis",
                "四、投资建议": "investment_suggestions",
                "五、风险提示": "risk_alerts",
                "六、市场情绪判断": "overall_sentiment"
            }

            for line in analysis_text.split("\n"):
                line = line.strip()
                
                for key, section_name in section_mapping.items():
                    if key in line:
                        current_section = section_name
                        break
                else:
                    if current_section and line:
                        sections[current_section] += line + "\n"

        return NewsAnalysis(
            date=news_date,
            summary=sections["summary"].strip(),
            bullish=sections["bullish"].strip(),
            hot_sectors=sections["hot_sectors"].strip(),
            leading_stocks=sections["leading_stocks"].strip(),
            macro_guidance=sections["macro_guidance"].strip(),
            risk_alerts=sections["risk_alerts"].strip(),
            overall_sentiment=sections["overall_sentiment"].strip()
        )

    def _mock_analysis(self, news_date: str, news_content: str) -> NewsAnalysis:
        """
        模拟分析结果（无API时使用）/ Mock analysis result
        """
        sentiment = "中性"
        if "增长" in news_content or "利好" in news_content:
            sentiment = "积极"
        elif "风险" in news_content or "下跌" in news_content:
            sentiment = "谨慎"

        return NewsAnalysis(
            date=news_date,
            summary=f"【{news_date}新闻联播摘要】\n今日新闻联播内容涉及多个重要领域，建议关注政策导向和行业动态。",
            bullish="关注政策支持方向，把握结构性机会",
            hot_sectors="建议关注：新能源、科技创新、消费升级等热门板块",
            leading_stocks="关注各行业龙头股回调机会",
            macro_guidance="保持理性投资，关注基本面，分散风险",
            risk_alerts="注意市场波动风险，关注外部环境变化",
            overall_sentiment=f"【市场情绪判断】\n{sentiment}\n建议根据个人风险承受能力调整投资策略。"
        )


if __name__ == "__main__":
    analyzer = AIAnalyzer()
    test_content = "今日新闻联播主要内容..."
    result = analyzer.analyze_news(test_content, "2026-04-05")
    if result:
        print(f"日期: {result.date}")
        print(f"摘要: {result.summary}")


class CCTVNewsProvider:
    """
    新闻联播数据提供器 / CCTV News Data Provider
    从MySQL获取新闻数据，必要时自动采集
    """

    def __init__(self):
        self.db_config = {
            "host": os.getenv("DB_HOST", "49.233.10.199"),
            "port": int(os.getenv("DB_PORT", 3306)),
            "user": os.getenv("DB_USER", "nextai"),
            "password": os.getenv("DB_PASSWORD", "100200"),
            "database": os.getenv("DB_NAME", "xcn_db"),
            "charset": "utf8mb4",
            "connect_timeout": 30,
            "read_timeout": 60,
            "write_timeout": 60,
        }

    def _get_connection(self):
        return pymysql.connect(**self.db_config)

    def get_latest_news(self, days: int = 7) -> List[dict]:
        """
        获取最近N天的新闻联播 / Get latest news for N days
        
        Args:
            days: 获取天数
        
        Returns:
            新闻列表，每条包含 date, main_topics, full_content
        """
        conn = self._get_connection()
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT news_date, main_topics, full_content
                    FROM cctv_news_broadcast
                    ORDER BY news_date DESC
                    LIMIT %s
                """, (days,))
                return cursor.fetchall()
        finally:
            conn.close()

    def get_latest_news_content(self) -> Optional[dict]:
        """
        获取最新一条新闻的完整内容 / Get latest news content
        
        Returns:
            新闻数据 dict，包含 date, main_topics, full_content
        """
        conn = self._get_connection()
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT news_date, main_topics, full_content
                    FROM cctv_news_broadcast
                    ORDER BY news_date DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()
                return result if result else None
        finally:
            conn.close()

    def fetch_and_save_yesterday_news(self) -> bool:
        """
        抓取昨天和今天的新闻联播 / Fetch yesterday's and today's news
        
        Returns:
            是否成功
        """
        try:
            from services.data_service.fetchers.cctv_news_fetcher import fetch_and_save_news
        except ImportError:
            print("无法导入新闻采集模块")
            return False
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        today = datetime.now().strftime("%Y%m%d")
        
        success = True
        for date in [yesterday, today]:
            print(f"尝试抓取: {date}")
            if fetch_and_save_news(date):
                print(f"  {date} 抓取成功")
            else:
                print(f"  {date} 抓取失败（或数据已存在）")
        
        return success

    def fetch_missing_news(self, days: int = 7) -> int:
        """
        补采缺失的新闻数据 / Fetch missing news data within N days
        
        检查过去N天内缺失的日期，并尝试抓取
        
        Args:
            days: 检查天数范围
        
        Returns:
            成功抓取的缺失数据数量
        """
        try:
            from services.data_service.fetchers.cctv_news_fetcher import fetch_and_save_news
        except ImportError:
            print("无法导入新闻采集模块")
            return 0
        
        conn = self._get_connection()
        existing_dates = set()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT news_date FROM cctv_news_broadcast
                    WHERE news_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                """, (days,))
                for row in cursor:
                    existing_dates.add(row[0])
        finally:
            conn.close()
        
        today = datetime.now().date()
        missing_count = 0
        
        for i in range(days):
            check_date = today - timedelta(days=i)
            if check_date not in existing_dates:
                date_str = check_date.strftime("%Y%m%d")
                print(f"补采缺失日期: {date_str}")
                if fetch_and_save_news(date_str):
                    missing_count += 1
                    print(f"  {date_str} 补采成功")
        
        return missing_count

    def ensure_latest_news(self, days: int = 7) -> List[dict]:
        """
        确保有最新数据，返回最近N天新闻 / Ensure latest news, return N days
        
        检查最新数据日期，如果最新数据不是今天/昨天，则自动抓取
        
        Args:
            days: 获取天数
        
        Returns:
            新闻列表
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(news_date) FROM cctv_news_broadcast")
                latest_date = cursor.fetchone()[0]
                
                today = datetime.now().date()
                yesterday = (today - timedelta(days=1))
                
                if latest_date is None or latest_date < yesterday:
                    print(f"最新数据日期: {latest_date}，正在抓取最新新闻...")
                    self.fetch_and_save_yesterday_news()
        finally:
            conn.close()
        
        return self.get_latest_news(days)

    def save_ai_analysis(self, news_date: str, analysis: 'NewsAnalysis', remarks: str = "", retry: int = 3) -> bool:
        """
        保存AI分析结果到数据库 / Save AI analysis result to database
        
        Args:
            news_date: 新闻日期
            analysis: AI分析结果
            remarks: 备注
            retry: 重试次数
        
        Returns:
            是否保存成功
        """
        for attempt in range(retry):
            conn = None
            try:
                conn = self._get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE cctv_news_broadcast 
                        SET ai_summary = %s,
                            ai_bullish = %s,
                            ai_hot_sectors = %s,
                            ai_leading_stocks = %s,
                            ai_macro_guidance = %s,
                            ai_risk_alerts = %s,
                            ai_sentiment = %s,
                            ai_updated_at = NOW(),
                            ai_remarks = %s
                        WHERE news_date = %s
                    """, (
                        analysis.summary,
                        analysis.bullish,
                        analysis.hot_sectors,
                        analysis.leading_stocks,
                        analysis.macro_guidance,
                        analysis.risk_alerts,
                        analysis.overall_sentiment,
                        remarks,
                        news_date
                    ))
                conn.commit()
                if cursor.rowcount > 0:
                    return True
                else:
                    print(f"未找到日期 {news_date} 的记录")
                    return False
            except Exception as e:
                print(f"保存AI分析失败 (尝试 {attempt + 1}/{retry}): {e}")
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                import time
                time.sleep(1)
                continue
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
        return False

    def get_news_with_analysis(self, news_date: str) -> Optional[dict]:
        """
        获取带AI分析的新闻 / Get news with AI analysis
        
        Args:
            news_date: 新闻日期
        
        Returns:
            新闻数据 dict，包含AI分析字段
        """
        conn = self._get_connection()
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT news_date, main_topics, full_content,
                           ai_summary, ai_bullish, ai_hot_sectors, ai_leading_stocks,
                           ai_macro_guidance, ai_risk_alerts, ai_sentiment,
                           ai_updated_at, ai_remarks
                    FROM cctv_news_broadcast
                    WHERE news_date = %s
                """, (news_date,))
                return cursor.fetchone()
        finally:
            conn.close()


def get_news_for_report() -> dict:
    """
    获取新闻数据供报告使用 / Get news data for report
    
    Returns:
        dict: {
            'latest': 最新新闻 {'date', 'main_topics', 'full_content'},
            'recent': 最近N天新闻列表,
            'analysis': AI分析结果 (NewsAnalysis)
        }
    """
    provider = CCTVNewsProvider()
    analyzer = AIAnalyzer()
    
    news_list = provider.ensure_latest_news(days=7)
    
    if not news_list:
        return {
            'latest': None,
            'recent': [],
            'analysis': None,
            'error': '无新闻数据'
        }
    
    latest = news_list[0]
    
    analysis = None
    if latest and latest.get('full_content'):
        try:
            news_date = latest['news_date'].strftime('%Y-%m-%d') if hasattr(latest['news_date'], 'strftime') else str(latest['news_date'])
            analysis = analyzer.analyze_news(latest['full_content'], news_date)
        except Exception as e:
            print(f"AI分析失败: {e}")
    
    return {
        'latest': {
            'date': latest['news_date'].strftime('%Y-%m-%d') if hasattr(latest['news_date'], 'strftime') else str(latest['news_date']),
            'main_topics': latest.get('main_topics', ''),
            'full_content': latest.get('full_content', '')
        },
        'recent': [
            {
                'date': n['news_date'].strftime('%Y-%m-%d') if hasattr(n['news_date'], 'strftime') else str(n['news_date']),
                'main_topics': n.get('main_topics', '')
            }
            for n in news_list
        ],
        'analysis': analysis
    }


if __name__ == "__cctv_provider__":
    provider = CCTVNewsProvider()
    news = provider.ensure_latest_news(days=3)
    print(f"获取到 {len(news)} 条新闻")
    for n in news:
        print(f"  {n['news_date']}: {n.get('main_topics', '')[:50]}...")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="新闻联播采集与分析")
    parser.add_argument("--days", type=int, default=7, help="获取最近N天新闻")
    parser.add_argument("--fetch-only", action="store_true", help="仅采集不分析")
    parser.add_argument("--mode", type=str, choices=["full", "supplement"], default="supplement", 
                       help="采集模式: full=主采集(昨天+今天), supplement=补采(7天内缺失)")
    args = parser.parse_args()
    
    print("=" * 50)
    print("新闻联播数据采集与处理")
    print(f"模式: {'主采集' if args.mode == 'full' else '补采'}")
    print("=" * 50)
    
    provider = CCTVNewsProvider()
    
    if args.mode == "full":
        provider.fetch_and_save_yesterday_news()
        news_list = provider.get_latest_news(days=args.days)
    elif args.mode == "supplement":
        provider.fetch_missing_news(days=args.days)
        news_list = provider.get_latest_news(days=args.days)
    else:
        news_list = provider.ensure_latest_news(days=args.days)
    
    print(f"\n获取到 {len(news_list)} 条新闻")
    for n in news_list:
        date_str = n['news_date'].strftime('%Y-%m-%d') if hasattr(n['news_date'], 'strftime') else str(n['news_date'])
        print(f"  {date_str}: {n.get('main_topics', '')[:60]}...")
    
    if not args.fetch_only and news_list:
        print("\n进行AI分析...")
        analyzer = AIAnalyzer()
        
        latest = news_list[0]
        news_date = latest['news_date'].strftime('%Y-%m-%d') if hasattr(latest['news_date'], 'strftime') else str(latest['news_date'])
        
        print(f"\n分析最新新闻: {news_date}")
        print(f"主题: {latest.get('main_topics', '')[:100]}...")
        
        analysis = analyzer.analyze_news(latest['full_content'], news_date)
        if analysis:
            print(f"\n=== AI分析结果 ({analysis.date}) ===")
            print(f"【摘要】{analysis.summary}")
            print(f"【利好因素】{analysis.bullish}")
            print(f"【热门板块】{analysis.hot_sectors}")
            print(f"【龙头个股】{analysis.leading_stocks}")
            print(f"【宏观指导】{analysis.macro_guidance}")
            print(f"【风险提示】{analysis.risk_alerts}")
            print(f"【情绪判断】{analysis.overall_sentiment}")
            
            print("\n保存AI分析结果到数据库...")
            save_result = provider.save_ai_analysis(news_date, analysis, "自动分析")
            if save_result:
                print("✅ AI分析结果已保存")
            else:
                print("❌ AI分析结果保存失败")
    
    print("\n完成!")
