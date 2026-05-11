"""
每日涨停板预测流水线
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import json

from limit_up_system.models.ensemble import LimitUpPredictor
from limit_up_system.data_collection.limit_up_collector import LimitUpCollector
from limit_up_system.data_collection.fund_flow_collector import FundFlowCollector
from limit_up_system.data_collection.dragon_tiger_collector import DragonTigerCollector

from services.stock_selection_db_service import StockSelectionDBService


class DailyPredictionPipeline:
    """每日涨停板预测流水线"""

    def __init__(
        self,
        output_dir: str = None,
        factor_weights: Optional[Dict[str, float]] = None,
        strategy_weights: Optional[Dict[str, float]] = None
    ):
        """
        初始化流水线

        Args:
            output_dir: 输出目录（默认为项目根目录的 data/predictions）
            factor_weights: 因子组权重
            strategy_weights: 策略权重
        """
        if output_dir is None:
            project_root = Path(__file__).resolve().parent.parent
            output_dir = project_root / "data" / "predictions"
        else:
            output_dir = Path(output_dir)
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 初始化数据采集器
        self.limit_up_collector = LimitUpCollector()
        self.fund_flow_collector = FundFlowCollector()
        self.dragon_tiger_collector = DragonTigerCollector()

        # 初始化预测器
        self.predictor = LimitUpPredictor(
            factor_weights=factor_weights,
            strategy_weights=strategy_weights
        )

    def run(
        self,
        trade_date: Optional[str] = None,
        top_n: int = 10,
        min_confidence: float = 70.0
    ) -> Dict[str, Any]:
        """
        运行每日预测流水线

        Args:
            trade_date: 交易日期（YYYYMMDD），默认为最近交易日
            top_n: 选择数量
            min_confidence: 最低置信度阈值

        Returns:
            预测结果字典
        """
        if trade_date is None:
            trade_date = self._get_last_trade_date()

        print(f"[{datetime.now()}] 开始预测 {trade_date} 的涨停板...")

        # 1. 采集数据
        print(f"[{datetime.now()}] 步骤 1/5: 采集数据...")
        full_data = self._get_full_history_data(trade_date)

        if full_data.height == 0:
            print(f"[{datetime.now()}] 警告: 没有数据可供预测")
            return {
                "trade_date": trade_date,
                "status": "no_data",
                "predictions": []
            }

        # 获取最新日期
        latest_date = full_data['trade_date'].max()

        # 2. 数据预处理
        print(f"[{datetime.now()}] 步骤 2/5: 数据预处理...")
        full_data = self._preprocess_data(full_data)

        # 3. 模型预测（使用完整历史数据计算技术指标）
        print(f"[{datetime.now()}] 步骤 3/5: 模型预测...")
        predictions = self.predictor.predict(full_data)

        # 4. 筛选 Top N（只保留最新日期的结果）
        print(f"[{datetime.now()}] 步骤 4/5: 筛选 Top {top_n}...")
        latest_predictions = predictions.filter(pl.col('trade_date') == latest_date)
        top_stocks = self.predictor.select_top_stocks(
            latest_predictions,
            top_n=top_n,
            min_confidence=min_confidence
        )

        # 5. 生成推荐报告
        print(f"[{datetime.now()}] 步骤 5/5: 生成推荐报告...")
        recommendations = self.predictor.generate_recommendation(top_stocks)

        # 保存结果
        result = {
            "trade_date": trade_date,
            "prediction_time": datetime.now().isoformat(),
            "total_stocks": full_data.height,
            "qualified_stocks": predictions.filter(
                pl.col("final_confidence") >= min_confidence
            ).height,
            "top_n": top_n,
            "min_confidence": min_confidence,
            "predictions": recommendations
        }

        self._save_result(result, trade_date)

        print(f"[{datetime.now()}] 预测完成！共推荐 {len(recommendations)} 只股票")

        return result

    def _collect_data(self, trade_date: str) -> pl.DataFrame:
        """
        采集数据

        Args:
            trade_date: 交易日期

        Returns:
            合并后的数据 DataFrame
        """
        # 采集全部K线数据（不只是涨停板）
        from core.data_adapter import DataAdapter
        adapter = DataAdapter()
        start = self._get_date_before(trade_date, 30)
        start_formatted = f"{start[:4]}-{start[4:6]}-{start[6:8]}"
        end_formatted = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        kline_data = adapter.read_kline_data(start_formatted, end_formatted)
        
        if kline_data.height == 0:
            print(f"[{datetime.now()}] 警告: 没有K线数据")
            return pl.DataFrame()
        
        # 计算涨跌幅
        kline_data = kline_data.sort(['ts_code', 'trade_date'])
        kline_data = kline_data.with_columns([
            pl.col('close').pct_change().over('ts_code').mul(100).alias('pct_chg')
        ])
        
        # 获取最新日期的数据（预测目标日的候选股票）
        latest_date = kline_data['trade_date'].max()
        candidate_stocks = kline_data.filter(pl.col('trade_date') == latest_date)

        # 采集涨停板数据（历史特征）
        limit_up_data = self.limit_up_collector.collect(
            start_date=self._get_date_before(trade_date, 30),
            end_date=trade_date
        )

        # 采集资金流向数据
        fund_flow_data = self.fund_flow_collector.collect(
            start_date=self._get_date_before(trade_date, 5),
            end_date=trade_date
        )

        # 采集龙虎榜数据
        dragon_tiger_data = self.dragon_tiger_collector.collect(
            start_date=self._get_date_before(trade_date, 5),
            end_date=trade_date
        )

        # 合并数据 - 使用候选股票作为基础
        data = candidate_stocks

        if fund_flow_data.height > 0:
            data = data.join(
                fund_flow_data,
                on=["ts_code", "trade_date"],
                how="left"
            )

        if dragon_tiger_data.height > 0:
            data = data.join(
                dragon_tiger_data,
                on=["ts_code", "trade_date"],
                how="left"
            )

        # 添加缺失字段的默认值（因子计算需要）
        default_columns = {
            "turnover_rate": 0.0,
            "pe": 0.0,
            "pb": 0.0,
            "total_mv": 0.0,
            "circ_mv": 0.0,
            "roe": 0.0,
            "roa": 0.0,
            "gross_profit_margin": 0.0,
            "revenue_yoy": 0.0,
            "net_profit_yoy": 0.0,
            "debt_to_assets": 0.0,
            "current_ratio": 0.0,
            "quick_ratio": 0.0,
            "vol": 0.0,
            "amount": 0.0,
            "reason": "",
            "buy_amount": 0.0,
            "sell_amount": 0.0,
            "main_net_inflow": 0.0,
            "huge_net_inflow": 0.0,
            "large_net_inflow": 0.0,
            "medium_net_inflow": 0.0,
            "small_net_inflow": 0.0,
        }
        for col, default_val in default_columns.items():
            if col not in data.columns:
                data = data.with_columns([pl.lit(default_val).alias(col)])

        return data

    def _get_full_history_data(self, trade_date: str) -> pl.DataFrame:
        """获取完整历史数据（用于因子计算需要历史数据的场景）"""
        from core.data_adapter import DataAdapter
        adapter = DataAdapter()
        start = self._get_date_before(trade_date, 30)
        start_formatted = f"{start[:4]}-{start[4:6]}-{start[6:8]}"
        end_formatted = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        kline_data = adapter.read_kline_data(start_formatted, end_formatted)

        if kline_data.height == 0:
            return pl.DataFrame()

        kline_data = kline_data.sort(['ts_code', 'trade_date'])
        kline_data = kline_data.with_columns([
            pl.col('close').pct_change().over('ts_code').mul(100).alias('pct_chg')
        ])

        default_columns = {
            "turnover_rate": 0.0, "pe": 0.0, "pb": 0.0, "total_mv": 0.0, "circ_mv": 0.0,
            "roe": 0.0, "roa": 0.0, "gross_profit_margin": 0.0, "revenue_yoy": 0.0,
            "net_profit_yoy": 0.0, "debt_to_assets": 0.0, "current_ratio": 0.0, "quick_ratio": 0.0,
            "vol": 0.0, "amount": 0.0, "reason": "", "buy_amount": 0.0, "sell_amount": 0.0,
            "main_net_inflow": 0.0, "huge_net_inflow": 0.0, "large_net_inflow": 0.0,
            "medium_net_inflow": 0.0, "small_net_inflow": 0.0,
            "buy_elg_amount": 0.0, "sell_elg_amount": 0.0,
            "buy_lg_amount": 0.0, "sell_lg_amount": 0.0,
            "buy_md_amount": 0.0, "sell_md_amount": 0.0,
            "buy_sm_amount": 0.0, "sell_sm_amount": 0.0,
            "buy_seats": "", "sell_seats": "",
        }
        for col, val in default_columns.items():
            if col not in kline_data.columns:
                kline_data = kline_data.with_columns([pl.lit(val).alias(col)])

        return kline_data

    def _preprocess_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        数据预处理

        Args:
            df: 原始数据 DataFrame

        Returns:
            预处理后的 DataFrame
        """
        # 1. 过滤 ST 股票（如果存在 name 列）
        if "name" in df.columns:
            df = df.filter(~pl.col("name").str.contains("ST"))

        # 2. 过滤停牌股票（如果存在 trade_status 列）
        if "trade_status" in df.columns:
            df = df.filter(pl.col("trade_status") == "1")

        # 3. 过滤涨跌停股票（已涨停的不再预测）
        if "pct_chg" in df.columns:
            df = df.filter(
                (pl.col("pct_chg") < 9.9) & (pl.col("pct_chg") > -9.9)
            )

        # 4. 填充缺失值
        numeric_cols = df.select(pl.col(pl.Float64)).columns
        for col in numeric_cols:
            df = df.with_columns(
                pl.col(col).fill_null(0.0)
            )

        return df

    def _save_result(self, result: Dict[str, Any], trade_date: str):
        """
        保存预测结果

        Args:
            result: 预测结果字典
            trade_date: 交易日期
        """
        # 保存 JSON
        json_path = self.output_dir / f"prediction_{trade_date}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # 保存 CSV
        if result["predictions"]:
            df = pl.DataFrame(result["predictions"])
            csv_path = self.output_dir / f"prediction_{trade_date}.csv"
            df.write_csv(csv_path)

        # 保存到 daily_prediction 表
        self._save_to_daily_prediction(result, trade_date)

        print(f"[{datetime.now()}] 结果已保存到 {json_path}")
    
    def _save_to_daily_prediction(self, result: Dict[str, Any], trade_date: str):
        """保存到 daily_prediction 表"""
        try:
            selection_service = StockSelectionDBService()

            scan_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
            selections = []
            for pred in result.get('predictions', []):
                selections.append({
                    'code': pred.get('ts_code', ''),
                    'name': pred.get('name', ''),
                    'selection_type': 'limit_up_prediction',
                    'score': pred.get('final_confidence', 0),
                    'close': pred.get('close'),
                    'price_change': pred.get('pct_chg'),
                    'rank': pred.get('rank', 0),
                })

            if selections:
                selection_service.save_selections(scan_date, selections)
                print(f"[{datetime.now()}] ✅ 已保存 {len(selections)} 条记录到 daily_prediction 表")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ 保存到 daily_prediction 失败: {e}")

    def _get_last_trade_date(self) -> str:
        """
        获取最近交易日

        Returns:
            交易日期（YYYYMMDD）
        """
        # 简化实现：返回昨天
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime("%Y%m%d")

    def _get_date_before(self, date_str: str, days: int) -> str:
        """
        获取指定日期之前的日期

        Args:
            date_str: 日期字符串（YYYYMMDD）
            days: 天数

        Returns:
            日期字符串（YYYYMMDD）
        """
        date = datetime.strptime(date_str, "%Y%m%d")
        before_date = date - timedelta(days=days)
        return before_date.strftime("%Y%m%d")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="每日涨停板预测")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="交易日期（YYYYMMDD），默认为最近交易日"
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="选择数量，默认 10"
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=70.0,
        help="最低置信度阈值，默认 70.0"
    )

    args = parser.parse_args()

    # 运行流水线
    pipeline = DailyPredictionPipeline()
    result = pipeline.run(
        trade_date=args.date,
        top_n=args.top_n,
        min_confidence=args.min_confidence
    )

    # 打印结果
    print("\n" + "=" * 80)
    print(f"预测日期: {result['trade_date']}")
    print(f"总股票数: {result['total_stocks']}")
    print(f"合格股票数: {result['qualified_stocks']}")
    print(f"推荐股票数: {len(result['predictions'])}")
    print("=" * 80)

    if result["predictions"]:
        print("\nTop 推荐:")
        for i, pred in enumerate(result["predictions"], 1):
            print(f"{i}. {pred['ts_code']} {pred['name']}")
            print(f"   置信度: {pred['final_confidence']}%")
            print(f"   最佳策略: {pred['best_strategy']} ({pred['best_probability']}%)")
            print(f"   涨幅: {pred['pct_chg']}%")
            print(f"   主力净流入: {pred['main_net_inflow']}万")
            print()


if __name__ == "__main__":
    main()
