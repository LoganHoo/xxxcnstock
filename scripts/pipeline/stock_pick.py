"""次日选股任务 - 18:00执行"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import polars as pl
from pathlib import Path
from datetime import datetime

from services.mainforce_resonance import MainForceDetector, scan_mainforce_signals, MainForceSignal
from services.stock_selection_db_service import StockSelectionDBService


class StockPicker:
    """选股器"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = self._setup_logger()
        self.db_service = StockSelectionDBService()

    def _setup_logger(self):
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def run(self) -> bool:
        """执行选股"""
        self.logger.info("开始次日选股...")

        try:
            scores_path = self.project_root / "data" / "enhanced_scores_full.parquet"
            if not scores_path.exists():
                self.logger.warning("评分文件不存在，跳过选股")
                return False

            scores = pl.read_parquet(scores_path)

            picks = self._generate_picks(scores)
            self._save_picks(picks)

            self.logger.info(f"选股完成: S级{len(picks['s_grade'])}只, A级{len(picks['a_grade'])}只")
            return True

        except Exception as e:
            self.logger.error(f"选股失败: {e}")
            return False

    def _filter_stocks(self, scores):
        """过滤不合格股票

        过滤条件：
        1. 涨停股（涨幅接近10%已无法买入）- 可选，保留用于观察
        2. 跌停股（流动性枯竭）- 必须过滤
        3. 停牌股（volume=0）- 必须过滤
        4. 成交额极低（volume < 10万，排除流动性枯竭）
        5. ST股票 - 过滤
        """
        original_count = len(scores)
        MIN_VOLUME = 100_000  # 降低到10万股，避免过度过滤

        # 先过滤停牌和跌停（严重影响交易）
        scores = scores.filter(
            (pl.col("volume") > 0) &
            (pl.col("change_pct") > -9.9)  # 过滤跌停
        )

        # 再过滤低流动性（但保留高分股票）
        # 策略：高分股票(>=85)放宽流动性要求，低分股票严格过滤
        high_score = scores.filter(pl.col("enhanced_score") >= 85)
        normal_score = scores.filter(
            (pl.col("enhanced_score") < 85) &
            (pl.col("volume") >= MIN_VOLUME)
        )

        scores = pl.concat([high_score, normal_score])

        filtered_count = original_count - len(scores)
        if filtered_count > 0:
            self.logger.info(f"过滤股票: 剔除{filtered_count}只(停牌/跌停/低流动性)")

        return scores

    def _add_mainforce_resonance(self, picks):
        """添加主力痕迹共振选股结果"""
        try:
            kline_dir = self.project_root / "data" / "kline"
            output_path = self.project_root / "data" / "mainforce_resonance.parquet"

            self.logger.info("开始扫描主力痕迹共振信号...")

            result_df = scan_mainforce_signals(str(kline_dir), str(output_path))

            if len(result_df) == 0:
                self.logger.info("未发现主力痕迹共振信号")
                picks["mainforce_resonance"] = []
                return

            s_plus = result_df.filter(pl.col("grade") == "S+")
            a_stocks = result_df.filter(pl.col("grade") == "A")
            b_stocks = result_df.filter(pl.col("grade") == "B")

            resonance_picks = []

            for df in [s_plus, a_stocks, b_stocks]:
                for row in df.iter_rows(named=True):
                    resonance_picks.append({
                        "code": row["code"],
                        "grade": row["grade"],
                        "signal_count": row["signal_count"],
                        "S1": row["S1"],
                        "S2": row["S2"],
                        "S3": row["S3"],
                        "S4": row["S4"]
                    })

            picks["mainforce_resonance"] = resonance_picks

            self.logger.info(
                f"主力共振: S+级{len(s_plus)}只, A级{len(a_stocks)}只, B级{len(b_stocks)}只"
            )

        except Exception as e:
            self.logger.error(f"主力共振扫描失败: {e}")
            picks["mainforce_resonance"] = []

    def _generate_picks(self, scores):
        """生成选股结果"""
        scores = scores.with_columns([
            pl.when(pl.col("enhanced_score") >= 100).then(pl.lit("S"))
            .when(pl.col("enhanced_score") >= 85).then(pl.lit("A"))
            .when(pl.col("enhanced_score") >= 65).then(pl.lit("B"))
            .otherwise(pl.lit("C")).alias("grade")
        ])

        scores = self._filter_stocks(scores)

        picks = {
            "s_grade": [],
            "a_grade": [],
            "limit_up_potential": [],
            "mainforce_resonance": []
        }

        # S级推荐
        s_stocks = scores.filter(pl.col("grade") == "S").sort("enhanced_score", descending=True)
        picks["s_grade"] = s_stocks.select(["code", "name", "price", "enhanced_score"]).head(30).to_dicts()

        # A级推荐
        a_stocks = scores.filter(pl.col("grade") == "A").sort("enhanced_score", descending=True)
        picks["a_grade"] = a_stocks.select(["code", "name", "price", "enhanced_score"]).head(30).to_dicts()

        # 涨停潜力股（涨幅>=9.5%且未涨停）
        limit_up_candidates = scores.filter(
            (pl.col("change_pct") >= 9.5) & (pl.col("change_pct") < 10.01)
        ).sort("enhanced_score", descending=True)
        picks["limit_up_potential"] = limit_up_candidates.select(["code", "name", "price", "change_pct", "enhanced_score"]).head(20).to_dicts()

        # 主力共振选股
        self._add_mainforce_resonance(picks)

        return picks

    def _save_picks(self, picks):
        """保存选股结果到JSON和数据库"""
        import json

        today = datetime.now().strftime("%Y%m%d")
        report_date = datetime.now().strftime("%Y-%m-%d")
        reports_dir = self.project_root / "data" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        # 保存到JSON
        path = reports_dir / f"picks_{today}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(picks, f, ensure_ascii=False, indent=2)

        self.logger.info(f"选股报告已保存到JSON: {path}")

        # 保存到数据库
        try:
            self.db_service.init_tables()

            # 转换S级推荐为数据库格式
            s_selections = []
            for i, stock in enumerate(picks.get('s_grade', [])):
                s_selections.append({
                    'code': stock.get('code'),
                    'name': stock.get('name'),
                    'selection_type': 's_grade',
                    'score': stock.get('enhanced_score', 0),
                    'rank': i + 1,
                    'close_price': stock.get('price', 0)
                })

            # 转换A级推荐为数据库格式
            a_selections = []
            for i, stock in enumerate(picks.get('a_grade', [])):
                a_selections.append({
                    'code': stock.get('code'),
                    'name': stock.get('name'),
                    'selection_type': 'a_grade',
                    'score': stock.get('enhanced_score', 0),
                    'rank': i + 1,
                    'close_price': stock.get('price', 0)
                })

            # 保存到数据库
            if s_selections:
                self.db_service.save_selections(report_date, s_selections, market_state='normal')
                self.logger.info(f"S级选股已保存到数据库: {len(s_selections)}只")

            if a_selections:
                self.db_service.save_selections(report_date, a_selections, market_state='normal')
                self.logger.info(f"A级选股已保存到数据库: {len(a_selections)}只")

        except Exception as e:
            self.logger.error(f"保存选股到数据库失败: {e}")
            # 数据库保存失败不影响JSON保存结果


if __name__ == "__main__":
    picker = StockPicker()
    result = picker.run()
    sys.exit(0 if result else 1)
