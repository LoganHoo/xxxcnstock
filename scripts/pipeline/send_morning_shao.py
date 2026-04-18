#!/usr/bin/env python3
"""
A股量化战略内参推送 - 使用 BaseReporter 重构
【08:45执行】
"""
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.base_reporter import BaseReporter
from core.paths import ReportPaths
from services.notify_service.templates import get_template
from services.report_db_service import ReportDBService


class 量化战略内参Reporter(BaseReporter):
    """A股量化战略内参推送器"""

    @property
    def report_type(self) -> str:
        return "morning_shao"

    @property
    def required_data_sources(self) -> List[str]:
        return ['foreign_data', 'market_data']

    @property
    def optional_data_sources(self) -> List[str]:
        return [
            'macro_data', 'oil_dollar_data', 'commodities_data',
            'sentiment_data', 'news_data', 'fund_behavior_data', 'picks_data'
        ]

    def load_data(self) -> Dict[str, Any]:
        """加载战略内参所需数据"""
        return {
            'foreign_data': self._load_json_file(ReportPaths.foreign_index()),
            'market_data': self._load_json_file(
                ReportPaths.market_analysis(fallback_to_yesterday=True)
            ),
            'macro_data': self._load_json_file(ReportPaths.macro_data()),
            'oil_dollar_data': self._load_json_file(ReportPaths.oil_dollar_data()),
            'commodities_data': self._load_json_file(ReportPaths.commodities_data()),
            'sentiment_data': self._load_json_file(ReportPaths.sentiment_data()),
            'news_data': self._load_json_file(ReportPaths.news_data()),
            'fund_behavior_data': self._load_json_file(ReportPaths.fund_behavior_result()),
            'picks_data': self._load_json_file(
                ReportPaths.daily_picks(fallback_to_yesterday=True)
            )
        }

    # 保持向后兼容的方法名
    def load_foreign_data(self) -> dict:
        return self._load_json_file(ReportPaths.foreign_index())

    def load_market_analysis(self) -> dict:
        file_path = ReportPaths.market_analysis(fallback_to_yesterday=True)
        return self._load_json_file(file_path) if file_path else None

    def load_macro_data(self) -> dict:
        return self._load_json_file(ReportPaths.macro_data())

    def load_oil_dollar_data(self) -> dict:
        return self._load_json_file(ReportPaths.oil_dollar_data())

    def load_commodities_data(self) -> dict:
        return self._load_json_file(ReportPaths.commodities_data())

    def load_sentiment_data(self) -> dict:
        return self._load_json_file(ReportPaths.sentiment_data())

    def load_news_data(self) -> list:
        return self._load_json_file(ReportPaths.news_data()) or []

    def load_fund_behavior_result(self) -> dict:
        return self._load_json_file(ReportPaths.fund_behavior_result())

    def load_daily_picks(self) -> dict:
        file_path = ReportPaths.daily_picks(fallback_to_yesterday=True)
        return self._load_json_file(file_path) if file_path else None

    def _calculate_macro_factor(self, data: Dict) -> Dict:
        """计算宏观因子"""
        foreign = data.get('foreign', {})
        macro = data.get('macro', {})
        oil_dollar = data.get('oil_dollar', {})
        commodities = data.get('commodities', {})

        factors = []

        us = foreign.get('us_index', {}).get('data', {}) if foreign else {}
        if isinstance(us, dict):
            for name, val in us.items():
                if isinstance(val, dict) and val.get('change_pct', 0) > 0:
                    factors.append(1)
                else:
                    factors.append(0)

        dxy = macro.get('dxy', {}) if macro else {}
        if isinstance(dxy, dict) and dxy.get('change_pct', 0) < 0.5:
            factors.append(1)
        else:
            factors.append(0)

        oil = oil_dollar.get('oil', {}) if oil_dollar else {}
        if isinstance(oil, dict):
            brent = oil.get('brent', {})
            if isinstance(brent, dict) and brent.get('change_pct', 0) < 3:
                factors.append(1)
            else:
                factors.append(0)

        gold = commodities.get('metals', {}).get('gold', {}) if commodities else {}
        if isinstance(gold, dict) and gold.get('change_pct', 0) < 2:
            factors.append(1)
        else:
            factors.append(0)

        final_score = sum(factors) / len(factors) if factors else 0.5
        return {'final': round(final_score, 2), 'details': factors}

    def _generate_reverse_logic(self, data: Dict) -> List[Dict]:
        """生成反向逻辑"""
        reverse_items = []
        sentiment = data.get('sentiment', {})
        fb_data = data.get('fund_behavior', {})
        macro = data.get('macro', {})
        news = data.get('news', [])

        bomb = sentiment.get('bomb_rate', {}) if sentiment else {}
        if bomb and bomb.get('rate', 0) > 40:
            reverse_items.append({
                'type': '情绪',
                'content': f'炸板率{bomb.get("rate", 0):.1f}%偏高，亏钱效应放大，谨慎打板'
            })

        fear_greed = sentiment.get('fear_greed', {}) if sentiment else {}
        if fear_greed:
            fg_value = fear_greed.get('value', 50)
            if fg_value >= 75:
                reverse_items.append({
                    'type': '情绪',
                    'content': f'恐慌贪婪指数{fg_value}进入极度贪婪区，注意获利了结'
                })

        cvd_signal = fb_data.get('cvd_signal', 'neutral') if fb_data else 'neutral'
        if cvd_signal == 'sell_dominant':
            reverse_items.append({
                'type': '资金',
                'content': 'CVD显示主力资金净流出，短期偏空'
            })

        dxy = macro.get('dxy', {}) if macro else {}
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

    def _transform_data(self, raw_data: Dict) -> Dict:
        """转换数据为模板所需格式"""
        foreign = raw_data.get('foreign_data', {})
        market = raw_data.get('market_data', {})
        macro = raw_data.get('macro_data', {})
        oil_dollar = raw_data.get('oil_dollar_data', {})
        commodities = raw_data.get('commodities_data', {})
        sentiment = raw_data.get('sentiment_data', {})
        news = raw_data.get('news_data', [])
        fb_data = raw_data.get('fund_behavior_data', {})
        picks = raw_data.get('picks_data', {})

        # 计算宏观因子和反向逻辑
        data_for_calc = {
            'foreign': foreign,
            'macro': macro,
            'oil_dollar': oil_dollar,
            'commodities': commodities,
            'sentiment': sentiment,
            'fund_behavior': fb_data,
            'news': news
        }
        macro_factor = self._calculate_macro_factor(data_for_calc)
        reverse_logic = self._generate_reverse_logic(data_for_calc)

        # 构建 global_alpha
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

        # 构建 domestic_core
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
            market_review_file = ReportPaths.market_review()
            if market_review_file.exists():
                review = self._load_json_file(market_review_file)
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

        # 构建 sentiment_engine
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
            sentiment_data_file = ReportPaths.sentiment_data()
            if sentiment_data_file.exists():
                sent_data = self._load_json_file(sentiment_data_file)
                bomb = sent_data.get('bomb_rate', {})
                if bomb:
                    sentiment_engine['bomb_rate'] = bomb

        if market:
            summary = market.get('summary', {})
            sentiment_engine['bomb_rate'] = {
                'rate': summary.get('bomb_rate', 0),
                'premium': summary.get('avg_premium', 0)
            }

        # 构建 ai_strategy
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

    # 保持向后兼容的方法名
    def generate_report_data(self) -> dict:
        """生成报告数据（兼容旧接口）"""
        return self._transform_data(self.load_data())

    def generate(self, data: Dict[str, Any]) -> str:
        """生成战略内参报告"""
        # 转换数据格式
        template_data = self._transform_data(data)

        template = get_template('morning_shao_report')
        return template.generate(template_data)

    # 保持向后兼容的方法名
    def generate_report(self) -> str:
        """生成报告（兼容旧接口）"""
        data = self.load_data()
        return self.generate(data)

    def _send(self, content: str) -> bool:
        """发送报告并保存到数据库"""
        success = super()._send(content)

        if success:
            try:
                db_service = ReportDBService()
                today = datetime.now().strftime('%Y-%m-%d')
                subject = f"【A股量化战略内参】{today} 盘前"

                db_service.save_report(
                    report_type='morning_shao',
                    report_date=today,
                    subject=subject,
                    text_content=content
                )
                self.logger.info("报告已保存到数据库")

                # 保存TXT文件
                txt_path = db_service.save_txt_file('morning_shao', today, content)
                self.logger.info(f"TXT已保存: {txt_path}")

            except Exception as e:
                self.logger.warning(f"保存到数据库失败: {e}")

        return success

    def run(self) -> bool:
        """执行推送（兼容旧接口）"""
        return super().run()


def main():
    """主函数"""
    reporter = 量化战略内参Reporter()
    success = reporter.run()

    result = reporter.get_last_result()
    if result:
        print(f"\n执行结果: {result.status.value}")
        if result.error_message:
            print(f"错误: {result.error_message}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
