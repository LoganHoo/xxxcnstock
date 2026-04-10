#!/usr/bin/env python3
"""
生成昨日选股复盘数据
【17:00执行】在复盘报告前生成，用于验证昨日推荐股票的表现
"""
import sys
import json
import logging
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PicksReviewGenerator:
    """选股复盘生成器"""

    def __init__(self):
        self.data_dir = project_root / "data"
        self.kline_dir = self.data_dir / "kline"
        self.output_path = self.data_dir / "picks_review.json"
        self.picks_review_path = self.data_dir / "picks_review.json"

    def load_yesterday_picks_from_db(self) -> List[Dict]:
        """从数据库加载昨日推荐股票"""
        picks = []
        try:
            from services.stock_pick_verification_service import StockPickVerificationService
            from models.stock_pick_verification import StockRecommendation

            service = StockPickVerificationService()
            session = service.Session()

            yesterday = date.today() - timedelta(days=1)
            records = session.query(StockRecommendation).filter(
                StockRecommendation.recommend_date == yesterday
            ).all()

            for record in records:
                picks.append({
                    'stock_code': record.stock_code,
                    'stock_name': record.stock_name,
                    'recommend_date': str(record.recommend_date),
                    'reason': getattr(record, 'reason', '') or '',
                    'score': getattr(record, 'total_score', 0) or 0
                })

            session.close()
            logger.info(f"从数据库加载昨日推荐: {len(picks)}只")

        except Exception as e:
            logger.warning(f"无法从数据库加载: {e}")
            picks = self._load_picks_from_file()

        return picks

    def _load_picks_from_file(self) -> List[Dict]:
        """从文件加载昨日选股结果"""
        picks = []
        try:
            picks_file = self.data_dir / "picks"
            if picks_file.exists():
                for f in picks_file.glob("picks_*.json"):
                    try:
                        with open(f, 'r', encoding='utf-8') as fp:
                            data = json.load(fp)
                            if isinstance(data, list):
                                picks.extend(data)
                            elif isinstance(data, dict) and 'picks' in data:
                                picks.extend(data['picks'])
                    except Exception as e:
                        logger.warning(f"加载 {f.name} 失败: {e}")

                yesterday = date.today() - timedelta(days=1)
                picks = [p for p in picks if str(p.get('date', '')) == str(yesterday)]

        except Exception as e:
            logger.warning(f"从文件加载失败: {e}")

        return picks

    def get_stock_price_data(self, stock_code: str, trade_date: date) -> Optional[Dict]:
        """获取股票指定日期的价格数据"""
        try:
            kline_file = self.kline_dir / f"{stock_code}.parquet"
            if not kline_file.exists():
                return None

            df = pl.read_parquet(str(kline_file))
            date_str = trade_date.strftime('%Y-%m-%d')

            filtered = df.filter(pl.col('trade_date') == date_str)
            if filtered.is_empty():
                return None

            row = filtered.first()
            return {
                'open': row.get('open', 0),
                'close': row.get('close', 0),
                'high': row.get('high', 0),
                'low': row.get('low', 0),
                'volume': row.get('volume', 0)
            }
        except Exception as e:
            logger.debug(f"获取 {stock_code} 数据失败: {e}")
            return None

    def calculate_change_pct(self, stock_code: str, recommend_date: date) -> float:
        """计算推荐日期到今天的涨跌幅"""
        try:
            kline_file = self.kline_dir / f"{stock_code}.parquet"
            if not kline_file.exists():
                return 0.0

            df = pl.read_parquet(str(kline_file))
            dates = df['trade_date'].to_list()

            if not dates:
                return 0.0

            today = date.today()
            recommend_date_str = recommend_date.strftime('%Y-%m-%d')

            today_idx = -1
            rec_idx = -1
            for i, d in enumerate(dates):
                if str(d) == str(today):
                    today_idx = i
                if str(d) == recommend_date_str:
                    rec_idx = i

            if today_idx < 0 or rec_idx < 0:
                return 0.0

            if today_idx <= rec_idx:
                return 0.0

            today_row = df.filter(pl.col('trade_date') == dates[today_idx]).first()
            rec_row = df.filter(pl.col('trade_date') == dates[rec_idx]).first()

            today_close = today_row.get('close', 0)
            rec_close = rec_row.get('close', 0)

            if rec_close == 0:
                return 0.0

            change_pct = ((today_close - rec_close) / rec_close) * 100
            return round(change_pct, 2)

        except Exception as e:
            logger.debug(f"计算 {stock_code} 涨跌幅失败: {e}")
            return 0.0

    def generate_review(self) -> Dict:
        """生成选股复盘数据"""
        logger.info("开始生成选股复盘...")

        picks = self.load_yesterday_picks_from_db()
        if not picks:
            logger.warning("昨日无推荐股票，跳过复盘")
            return self._create_empty_review()

        yesterday = date.today() - timedelta(days=1)
        today = date.today()

        details = []
        win_count = 0
        loss_count = 0
        hold_count = 0

        for pick in picks:
            stock_code = pick.get('stock_code', '')
            stock_name = pick.get('stock_name', '')
            reason = pick.get('reason', '')
            score = pick.get('score', 0)

            price_data = self.get_stock_price_data(stock_code, today)
            change_pct = self.calculate_change_pct(stock_code, yesterday)

            if price_data:
                if change_pct > 0:
                    status = 'win'
                    win_count += 1
                elif change_pct < 0:
                    status = 'loss'
                    loss_count += 1
                else:
                    status = 'hold'
                    hold_count += 1

                detail = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'recommend_reason': reason,
                    'recommend_score': score,
                    'change_pct': change_pct,
                    'status': status,
                    'open': price_data.get('open', 0),
                    'close': price_data.get('close', 0),
                    'high': price_data.get('high', 0),
                    'low': price_data.get('low', 0),
                    'volume': price_data.get('volume', 0)
                }
            else:
                status = 'unknown'
                detail = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'recommend_reason': reason,
                    'recommend_score': score,
                    'change_pct': 0,
                    'status': status,
                    'open': 0,
                    'close': 0,
                    'high': 0,
                    'low': 0,
                    'volume': 0
                }

            details.append(detail)

        details.sort(key=lambda x: x['change_pct'], reverse=True)

        top_picks = [
            {
                'stock_code': d['stock_code'],
                'stock_name': d['stock_name'],
                'change_pct': d['change_pct'],
                'status': d['status'],
                'reason': d['recommend_reason']
            }
            for d in details[:10]
        ]

        reviewed_picks = win_count + loss_count + hold_count

        review = {
            'date': str(today),
            'recommend_date': str(yesterday),
            'summary': {
                'total_picks': len(picks),
                'reviewed_picks': reviewed_picks,
                'win_count': win_count,
                'loss_count': loss_count,
                'hold_count': hold_count,
                'win_rate': round((win_count / reviewed_picks * 100) if reviewed_picks > 0 else 0, 1)
            },
            'top_picks': top_picks,
            'details': details,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        return review

    def _create_empty_review(self) -> Dict:
        """创建空的复盘数据"""
        return {
            'date': str(date.today()),
            'recommend_date': str(date.today() - timedelta(days=1)),
            'summary': {
                'total_picks': 0,
                'reviewed_picks': 0,
                'win_count': 0,
                'loss_count': 0,
                'hold_count': 0,
                'win_rate': 0
            },
            'top_picks': [],
            'details': [],
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def save_review(self, review: Dict) -> bool:
        """保存复盘数据到文件"""
        try:
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(review, f, ensure_ascii=False, indent=2)
            logger.info(f"选股复盘已保存: {self.output_path}")
            return True
        except Exception as e:
            logger.error(f"保存复盘数据失败: {e}")
            return False

    def run(self) -> bool:
        """运行选股复盘生成"""
        logger.info("=" * 60)
        logger.info("开始生成昨日选股复盘")
        logger.info("=" * 60)

        review = self.generate_review()
        success = self.save_review(review)

        if success:
            summary = review.get('summary', {})
            logger.info(f"✅ 选股复盘生成完成")
            logger.info(f"   昨日推荐: {summary.get('total_picks', 0)}只")
            logger.info(f"   已复盘: {summary.get('reviewed_picks', 0)}只")
            logger.info(f"   上涨: {summary.get('win_count', 0)}只")
            logger.info(f"   下跌: {summary.get('loss_count', 0)}只")
            logger.info(f"   胜率: {summary.get('win_rate', 0)}%")
        else:
            logger.error("❌ 选股复盘生成失败")

        return success


def main():
    generator = PicksReviewGenerator()
    success = generator.run()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())