#!/usr/bin/env python3
"""
漏斗选股执行脚本
5层漏斗 + AI综合评分 → daily_prediction 表
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.stock_service.funnel_selector import FunnelSelector
from services.ai.funnel_ai_scorer import FunnelAIScorer
from services.stock_selection_db_service import StockSelectionDBService, DailyPrediction
from core.logger import setup_logger

logger = setup_logger("funnel_stock_pick")


class FunnelStockPicker:
    """漏斗选股执行器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.data_dir = self.project_root / "data" / "kline"
        self.funnel_selector = FunnelSelector(str(self.data_dir))
        self.ai_scorer = FunnelAIScorer(str(self.data_dir))
        self.db_service = StockSelectionDBService()

    def run(self, trade_date: str = None) -> bool:
        """执行漏斗选股"""
        if trade_date is None:
            trade_date = self._get_previous_trade_date()

        logger.info("=" * 60)
        logger.info(f"漏斗选股开始 日期: {trade_date}")
        logger.info("=" * 60)

        try:
            funnel_stocks = self.funnel_selector.run(trade_date)
            if len(funnel_stocks) == 0:
                logger.warning("漏斗筛选后无股票")
                return False

            ai_recommendations = self.ai_scorer.score_and_recommend(funnel_stocks, trade_date, top_n=50)
            if len(ai_recommendations) == 0:
                logger.warning("AI评分为空")
                return False

            self._save_to_database(ai_recommendations, trade_date)

            output_file = self.project_root / "data" / f"funnel_selection_{trade_date}.parquet"
            ai_recommendations.to_parquet(output_file, index=False, compression='zstd')

            logger.info(f"漏斗选股完成: {len(ai_recommendations)} 只")
            logger.info(f"结果已保存: {output_file}")
            return True

        except Exception as e:
            logger.error(f"漏斗选股失败: {e}")
            return False

    def _get_previous_trade_date(self) -> str:
        """获取前一交易日"""
        today = datetime.now().date()
        prev_day = today - timedelta(days=1)
        return prev_day.strftime('%Y-%m-%d')

    def _save_to_database(self, recommendations, trade_date: str):
        """保存推荐结果到 daily_prediction 表"""
        try:
            session = self.db_service.Session()

            for _, row in recommendations.iterrows():
                existing = session.query(DailyPrediction).filter(
                    DailyPrediction.prediction_date == trade_date,
                    DailyPrediction.code == row['code']
                ).first()

                if existing:
                    existing.funnel_score = row.get('funnel_score', 0)
                    existing.layer1_score = row.get('layer1_score', 0)
                    existing.layer2_score = row.get('layer2_score', 0)
                    existing.layer3_score = row.get('layer3_score', 0)
                    existing.layer4_score = row.get('layer4_score', 0)
                    existing.layer5_score = row.get('layer5_score', 0)
                    existing.ai_score = row.get('ai_score', 0)
                    existing.entry_price = row.get('entry_price', 0)
                    existing.stoploss_price = row.get('stoploss_price', 0)
                    existing.take_profit_1 = row.get('take_profit_1', 0)
                    existing.take_profit_2 = row.get('take_profit_2', 0)
                    existing.support_price = row.get('support_price', 0)
                    existing.resistance_price = row.get('resistance_price', 0)
                else:
                    record = DailyPrediction(
                        prediction_date=trade_date,
                        code=row['code'],
                        name=row.get('name', ''),
                        selection_type='funnel_ai',
                        score=row.get('ai_score', 0),
                        funnel_score=row.get('funnel_score', 0),
                        layer1_score=row.get('layer1_score', 0),
                        layer2_score=row.get('layer2_score', 0),
                        layer3_score=row.get('layer3_score', 0),
                        layer4_score=row.get('layer4_score', 0),
                        layer5_score=row.get('layer5_score', 0),
                        ai_score=row.get('ai_score', 0),
                        entry_price=row.get('entry_price', 0),
                        stoploss_price=row.get('stoploss_price', 0),
                        take_profit_1=row.get('take_profit_1', 0),
                        take_profit_2=row.get('take_profit_2', 0),
                        support_price=row.get('support_price', 0),
                        resistance_price=row.get('resistance_price', 0),
                    )
                    session.add(record)

            session.commit()
            session.close()
            logger.info(f"已保存 {len(recommendations)} 条到 daily_prediction 表")

        except Exception as e:
            logger.error(f"保存数据库失败: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='漏斗选股')
    parser.add_argument('--date', type=str, help='交易日期 YYYY-MM-DD')
    args = parser.parse_args()

    picker = FunnelStockPicker()
    success = picker.run(args.date)
    sys.exit(0 if success else 1)
