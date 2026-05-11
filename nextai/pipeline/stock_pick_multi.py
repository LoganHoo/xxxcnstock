#!/usr/bin/env python3
"""多流程选股对比主脚本"""
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from core.logger import get_logger

logger = get_logger("stock_pick_multi")

sys.path.insert(0, 'scripts/pipeline')
sys.path.insert(0, '.')

from flows.conservative_flow import ConservativeFlow
from flows.balanced_factor_flow import BalancedFactorFlow
from flows.aggressive_signal_flow import AggressiveSignalFlow

from services.stock_selection_db_service import StockSelectionDBService


def save_to_daily_prediction(results: dict, predict_date: str = None) -> bool:
    """保存选股结果到 MySQL daily_prediction 表"""
    if not results:
        return False

    if predict_date is None:
        predict_date = datetime.now().strftime('%Y-%m-%d')

    try:
        selection_service = StockSelectionDBService()
        
        source_map = {
            "conservative": "conservative",
            "balanced_factor": "balanced_factor",
            "aggressive_signal": "aggressive_signal"
        }

        all_selections = []
        for flow_name, result in results.items():
            if result is None:
                continue
            
            source = source_map.get(flow_name, flow_name)
            category = flow_name
            for stock in result.stocks:
                all_selections.append({
                    'code': stock.code,
                    'name': getattr(stock, 'name', '') or '',
                    'selection_type': category,
                    'total_score': getattr(stock, 'score', 0),
                    'strategy_type': flow_name,
                    'source': source
                })
        
        if all_selections:
            selection_service.save_to_daily_prediction(predict_date, all_selections, source='stock_pick_multi')
            logger.info(f"✅ 成功保存 {len(all_selections)} 条选股结果到 daily_prediction 表")
            return True
        
        return False

    except Exception as e:
        logger.error(f"❌ 保存到 daily_prediction 表失败: {e}")
        return False


def save_to_predictions_table(results: dict) -> bool:
    """保存选股结果到 MySQL predictions 表 (Legacy)"""
    if not results:
        return False

    predict_date = datetime.now().date()
    conn = None
    cursor = None

    source_map = {
        "conservative": "conservative",
        "balanced_factor": "balanced_factor",
        "aggressive_signal": "aggressive_signal"
    }

    try:
        conn = get_db_connection('xcn_db')
        cursor = conn.cursor()

        insert_sql = """
        INSERT INTO predictions
        (predict_date, code, name, source, grade, score, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, 'active', NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            grade = VALUES(grade),
            score = VALUES(score),
            updated_at = NOW()
        """

        total_count = 0
        for flow_name, result in results.items():
            if result is None:
                continue

            source = source_map.get(flow_name, flow_name)
            for stock in result.stocks:
                cursor.execute(insert_sql, (
                    predict_date,
                    stock.code,
                    getattr(stock, 'name', '') or '',
                    source,
                    stock.grade,
                    stock.score
                ))
                total_count += 1

        conn.commit()
        logger.info(f"✅ 成功保存 {total_count} 条选股结果到 predictions 表 (Legacy)")
        return True

    except Exception as e:
        logger.error(f"❌ 保存到 predictions 表失败: {e}")
        if conn:
            conn.rollback()
        return False

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_all_flows(kline_dir: str, scores_path: str = None) -> dict:
    """运行所有选股流程"""
    flows = {
        "conservative": ConservativeFlow(),
        "balanced_factor": BalancedFactorFlow(),
        "aggressive_signal": AggressiveSignalFlow()
    }

    results = {}

    for name, flow in flows.items():
        logger.info(f"=" * 50)
        logger.info(f"开始运行流程: {name}")
        logger.info(f"=" * 50)

        try:
            result = flow.select(kline_dir, scores_path)
            results[name] = result
            logger.info(f"流程 {name} 完成: 选出 {len(result.stocks)} 只股票")
        except Exception as e:
            logger.error(f"流程 {name} 执行失败: {e}")
            import traceback
            traceback.print_exc()
            results[name] = None

    return results


def print_summary(results: dict):
    """打印结果摘要"""
    logger.info("\n" + "=" * 60)
    logger.info("选股结果摘要")
    logger.info("=" * 60)

    for name, result in results.items():
        if result:
            logger.info(f"  {name}: {len(result.stocks)} 只股票")
            if result.stocks:
                top5 = result.stocks[:5]
                codes = [s.code for s in top5]
                logger.info(f"    前5: {codes}")
        else:
            logger.info(f"  {name}: 执行失败")


if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    kline_dir = str(project_root / "data/kline")

    if not Path(kline_dir).exists():
        logger.error(f"K线目录不存在: {kline_dir}")
        sys.exit(1)

    logger.info("开始多流程选股...")
    results = run_all_flows(kline_dir, None)

    print_summary(results)

    logger.info("\n开始保存结果到数据库...")
    save_to_daily_prediction(results)
    save_to_predictions_table(results)

    logger.info("\n选股完成！")