"""A股量化战略内参推送 - 08:45执行"""
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from services.email_sender import EmailService
from services.notify_service.templates.morning_shao_report import get_量化战略内参_template
from services.report_db_service import ReportDBService


class 量化战略内参Reporter:
    """A股量化战略内参推送器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.data_dir = self.project_root / "data"
        self.reports_dir = self.project_root / "reports"
        self.logger = self._setup_logger()

    def _setup_logger(self):
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def load_foreign_data(self) -> dict:
        """加载外盘数据"""
        foreign_file = self.data_dir / "foreign_index.json"
        if foreign_file.exists():
            with open(foreign_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_market_analysis(self) -> dict:
        """加载大盘分析数据"""
        today = datetime.now().strftime('%Y%m%d')
        market_file = self.reports_dir / f"market_analysis_{today}.json"
        if not market_file.exists():
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            market_file = self.reports_dir / f"market_analysis_{yesterday}.json"
        if market_file.exists():
            with open(market_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_macro_data(self) -> dict:
        """加载宏观数据"""
        macro_file = self.data_dir / "macro_data.json"
        if macro_file.exists():
            with open(macro_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_oil_dollar_data(self) -> dict:
        """加载石油美元数据"""
        oil_file = self.data_dir / "oil_dollar_data.json"
        if oil_file.exists():
            with open(oil_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_commodities_data(self) -> dict:
        """加载大宗商品数据"""
        comm_file = self.data_dir / "commodities_data.json"
        if comm_file.exists():
            with open(comm_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_sentiment_data(self) -> dict:
        """加载情绪数据"""
        sent_file = self.data_dir / "sentiment_data.json"
        if sent_file.exists():
            with open(sent_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_news_data(self) -> dict:
        """加载新闻数据"""
        news_file = self.data_dir / "news_data.json"
        if news_file.exists():
            with open(news_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_fund_behavior_result(self) -> dict:
        """加载资金行为学策略结果"""
        fb_file = self.reports_dir / "fund_behavior_result.json"
        if fb_file.exists():
            with open(fb_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def load_daily_picks(self) -> dict:
        """加载选股数据"""
        today = datetime.now().strftime('%Y%m%d')
        picks_file = self.reports_dir / f"daily_picks_{today}.json"
        if not picks_file.exists():
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            picks_file = self.reports_dir / f"daily_picks_{yesterday}.json"
        if picks_file.exists():
            with open(picks_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def calculate_macro_factor(self, data: dict) -> dict:
        """计算宏观调节因子 M = M_macro × M_sentiment"""
        m_macro = 1.0
        m_sentiment = 1.0

        foreign = data.get('foreign', {})
        macro = data.get('macro', {})
        sentiment = data.get('sentiment', {})
        fb_data = data.get('fund_behavior', {})

        us = foreign.get('us_index', {}).get('data', {})
        if us:
            sp500 = us.get('sp500', {})
            nasdaq = us.get('nasdaq', {})
            if isinstance(sp500, dict) and sp500.get('change_pct', 0) < -2:
                m_macro *= 0.8
            if isinstance(nasdaq, dict) and nasdaq.get('change_pct', 0) < -2:
                m_macro *= 0.8

        dxy = macro.get('dxy', {})
        if isinstance(dxy, dict):
            if dxy.get('change_pct', 0) > 1:
                m_macro *= 0.9

        bomb = sentiment.get('bomb_rate', {})
        if isinstance(bomb, dict):
            if bomb.get('rate', 0) > 40:
                m_sentiment *= 0.7
            elif bomb.get('rate', 0) < 20:
                m_sentiment *= 1.2

        cvd_signal = fb_data.get('cvd_signal', 'neutral')
        if cvd_signal == 'sell_dominant':
            m_sentiment *= 0.8
        elif cvd_signal == 'buy_dominant':
            m_sentiment *= 1.1

        fear_greed = sentiment.get('fear_greed', {})
        if isinstance(fear_greed, dict):
            fg_value = fear_greed.get('value', 50)
            if fg_value >= 75:
                m_sentiment *= 0.85

        return {
            'm_macro': round(m_macro, 2),
            'm_sentiment': round(m_sentiment, 2),
            'final': round(m_macro * m_sentiment, 2)
        }

    def generate_reverse_logic(self, data: dict) -> list:
        """生成AI反向逻辑提醒"""
        reverse_items = []

        foreign = data.get('foreign', {})
        sentiment = data.get('sentiment', {})
        news = data.get('news', [])
        fb_data = data.get('fund_behavior', {})
        macro = data.get('macro', {})

        us = foreign.get('us_index', {}).get('data', {})
        if us:
            sp500 = us.get('sp500', {})
            if isinstance(sp500, dict) and sp500.get('change_pct', 0) < -2:
                reverse_items.append({
                    'type': '外围',
                    'content': '美股标普500大幅下跌，留意A股开盘压力'
                })

        bomb = sentiment.get('bomb_rate', {})
        if bomb and bomb.get('rate', 0) > 40:
            reverse_items.append({
                'type': '情绪',
                'content': f'炸板率{bomb.get("rate", 0):.1f}%偏高，亏钱效应放大，谨慎打板'
            })

        fear_greed = sentiment.get('fear_greed', {})
        if fear_greed:
            fg_value = fear_greed.get('value', 50)
            if fg_value >= 75:
                reverse_items.append({
                    'type': '情绪',
                    'content': f'恐慌贪婪指数{fg_value}进入极度贪婪区，注意获利了结'
                })

        cvd_signal = fb_data.get('cvd_signal', 'neutral')
        if cvd_signal == 'sell_dominant':
            reverse_items.append({
                'type': '资金',
                'content': 'CVD显示主力资金净流出，短期偏空'
            })

        dxy = macro.get('dxy', {})
        if isinstance(dxy, dict) and dxy.get('change_pct', 0) > 1:
            reverse_items.append({
                'type': '宏观',
                'content': '美元指数暴涨，外资可能撤离，关注北向资金流向'
            })

        news_items = news if isinstance(news, list) else []
        for item in news_items[:2]:
            item_str = str(item)
            if '利好' in item_str and '缩量' in item_str:
                reverse_items.append({
                    'type': '量价',
                    'content': f'新闻利好但缩量：{item[:30]}...警惕利好兑现'
                })

        return reverse_items[:4]

    def generate_report_data(self) -> dict:
        """生成报告数据"""
        foreign = self.load_foreign_data()
        market = self.load_market_analysis()
        macro = self.load_macro_data()
        oil_dollar = self.load_oil_dollar_data()
        commodities = self.load_commodities_data()
        sentiment = self.load_sentiment_data()
        news = self.load_news_data()
        fb_data = self.load_fund_behavior_result()
        picks = self.load_daily_picks()

        data = {
            'foreign': foreign,
            'macro': macro,
            'sentiment': sentiment,
            'fund_behavior': fb_data,
            'news': news
        }

        macro_factor = self.calculate_macro_factor(data)
        reverse_logic = self.generate_reverse_logic(data)

        global_alpha = {
            'foreign': {},
            'macro': {},
            'oil_dollar': {},
            'commodities': {}
        }

        if foreign:
            us_data = foreign.get('us_index', {}).get('data', {})
            ga_foreign = {'us': {}, 'a50': {}, 'china_concept': {}}
            name_map_us = {'nasdaq': '纳斯达克', 'sp500': '标普500', 'dow': '道琼斯', 'dji': '道琼斯'}
            for name, val in us_data.items():
                if isinstance(val, dict):
                    mapped_name = name_map_us.get(name, name)
                    ga_foreign['us'][mapped_name] = {
                        'price': val.get('price', 0),
                        'change_pct': val.get('change_pct', 0)
                    }
            global_alpha['foreign'] = ga_foreign

            asia_data = foreign.get('asia_index', {}).get('data', {})
            if isinstance(asia_data, dict):
                for name, val in asia_data.items():
                    if isinstance(val, dict):
                        if name == 'hang_seng':
                            ga_foreign['a50'] = {
                                'price': val.get('price', 0),
                                'change_pct': val.get('change_pct', 0)
                            }
                        elif name == 'hstech':
                            ga_foreign['china_concept'] = {
                                'price': val.get('price', 0),
                                'change_pct': val.get('change_pct', 0)
                            }

        if macro:
            ga_macro = {}
            dxy = macro.get('dxy', {})
            if dxy:
                ga_macro['dxy'] = {'value': dxy.get('value', 0), 'change_pct': dxy.get('change_pct', 0)}
            us10y = macro.get('us10y', {})
            if us10y:
                ga_macro['us10y'] = {'value': us10y.get('value', 0)}
            cny = macro.get('cny', {})
            if cny:
                ga_macro['cny'] = {'value': cny.get('value', 0)}
            global_alpha['macro'] = ga_macro

        if oil_dollar:
            global_alpha['oil_dollar'] = {
                'oil': oil_dollar.get('oil', {}),
                'notes': oil_dollar.get('notes', [])[:2]
            }

        if commodities:
            metals = commodities.get('metals', {})
            agriculture = commodities.get('agriculture', {})
            global_alpha['commodities'] = {
                'gold': metals.get('gold', {}),
                'silver': metals.get('silver', {}),
                'copper': metals.get('copper', {}),
                'lithium': agriculture.get('lithium', {})
            }

        domestic_core = {
            'yesterday': {},
            'key_levels': {},
            'news': []
        }

        if market:
            indices = market.get('indices', [])
            summary = market.get('summary', {})
            domestic_core['yesterday'] = {
                'indices': indices[:3],
                'summary': summary
            }

            for idx in indices[:1]:
                levels = idx.get('levels', {})
                domestic_core['key_levels'][idx.get('name', '')] = {
                    'resistance': levels.get('resistance_1', 'N/A'),
                    'support': levels.get('support_1', 'N/A'),
                    'prediction': idx.get('analysis', {}).get('action', '等待确认')
                }
        else:
            market_review_file = self.data_dir / "market_review.json"
            if market_review_file.exists():
                with open(market_review_file, 'r', encoding='utf-8') as f:
                    review = json.load(f)
                    summary = review.get('summary', {})
                    cvd = review.get('cvd', {})
                    domestic_core['yesterday'] = {
                        'indices': [],
                        'summary': {
                            'rising': summary.get('rising_count', 0),
                            'falling': summary.get('falling_count', 0),
                            'limit_up': summary.get('limit_up_count', 0),
                            'limit_down': summary.get('limit_down_count', 0),
                            'volume': summary.get('total_volume', 0) * 10000
                        }
                    }
                    domestic_core['key_levels'] = {
                        '市场状态': {
                            'resistance': 'N/A',
                            'support': 'N/A',
                            'prediction': cvd.get('signal', 'neutral')
                        }
                    }

        domestic_core['news'] = news[:4] if isinstance(news, list) else []

        sentiment_engine = {
            'board': {},
            'bomb_rate': {},
            'sentiment': {}
        }

        if sentiment:
            sentiment_engine['sentiment'] = sentiment
            bomb = sentiment.get('bomb_rate', {})
            if bomb:
                sentiment_engine['bomb_rate'] = bomb

        if fb_data:
            sentiment_engine['board'] = {
                'highest': fb_data.get('highest_board', 0),
                'trend': fb_data.get('board_trend', 'N/A'),
                'themes': fb_data.get('hot_themes', [])
            }

        if not sentiment_engine.get('bomb_rate'):
            sentiment_data_file = self.data_dir / "sentiment_data.json"
            if sentiment_data_file.exists():
                with open(sentiment_data_file, 'r', encoding='utf-8') as f:
                    sent_data = json.load(f)
                    bomb = sent_data.get('bomb_rate', {})
                    if bomb:
                        sentiment_engine['bomb_rate'] = bomb

        if market:
            summary = market.get('summary', {})
            sentiment_engine['bomb_rate'] = {
                'rate': summary.get('bomb_rate', 0),
                'premium': summary.get('avg_premium', 0)
            }

        ai_strategy = {
            'focus_themes': [],
            'stocks': {'s_grade': [], 'a_grade': []},
            'reverse_logic': reverse_logic,
            'warnings': [],
            'macro_factor': macro_factor
        }

        if picks:
            filters = picks.get('filters', {})
            s_grade = filters.get('s_grade', {}).get('stocks', [])[:3]
            a_grade = filters.get('a_grade', {}).get('stocks', [])[:3]

            ai_strategy['stocks']['s_grade'] = [
                {'code': s.get('code', ''), 'name': s.get('name', ''), 'theme': s.get('reasons', '')[:20]}
                for s in s_grade
            ]
            ai_strategy['stocks']['a_grade'] = [
                {'code': s.get('code', ''), 'name': s.get('name', ''), 'condition': 'MA20低吸'}
                for s in a_grade
            ]

        if fb_data:
            hot_themes = fb_data.get('hot_themes', [])
            ai_strategy['focus_themes'] = hot_themes[:2]

        if macro_factor['final'] < 0.8:
            ai_strategy['warnings'].append('宏观环境偏弱，建议降低仓位')
        if sentiment_engine['bomb_rate'].get('rate', 0) > 40:
            ai_strategy['warnings'].append('打板亏钱效应大，谨慎操作')

        return {
            'global_alpha': global_alpha,
            'domestic_core': domestic_core,
            'sentiment_engine': sentiment_engine,
            'ai_strategy': ai_strategy
        }

    def generate_report(self) -> str:
        """生成完整报告"""
        data = self.generate_report_data()
        template = get_量化战略内参_template()
        return template.generate(data)

    def run(self) -> bool:
        """执行推送"""
        self.logger.info("开始推送A股量化战略内参...")

        try:
            content = self.generate_report()
            self.logger.info(f"报告内容:\n{content}")

            email_service = EmailService()

            recipients_str = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')
            recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

            if not recipients:
                self.logger.error("未配置收件人")
                return False

            today = datetime.now().strftime('%Y-%m-%d')
            subject = f"【A股量化战略内参】{today} 盘前"

            self.logger.info(f"发送邮件到: {recipients}")
            self.logger.info(f"主题: {subject}")

            result = email_service.send(
                to_addrs=recipients,
                subject=subject,
                content=content
            )

            if result:
                self.logger.info("量化战略内参推送成功")
                self.save_report_to_db(today, subject, content)
                return True
            else:
                self.logger.error("量化战略内参推送失败")
                return False

        except Exception as e:
            self.logger.error(f"量化战略内参推送异常: {e}")
            return False

    def save_report_to_db(self, report_date: str, subject: str, text_content: str):
        """保存报告到MySQL和TXT"""
        try:
            db_service = ReportDBService()
            db_service.init_tables()

            db_service.save_report(
                report_type='morning_shao',
                report_date=report_date,
                subject=subject,
                text_content=text_content
            )

            txt_path = db_service.save_txt_file('morning_shao', report_date, text_content)
            self.logger.info(f"TXT已保存: {txt_path}")

        except Exception as e:
            self.logger.warning(f"保存报告到数据库失败: {e}")


if __name__ == "__main__":
    reporter = 量化战略内参Reporter()
    result = reporter.run()
    sys.exit(0 if result else 1)
