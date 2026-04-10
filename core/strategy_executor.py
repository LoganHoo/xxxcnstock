"""
策略执行器
负责执行资金行为学策略并应用过滤器
"""
import polars as pl
from typing import Dict, Any, Tuple
import logging

from core.fund_behavior_strategy import FundBehaviorStrategyEngine
from core.filter_config_loader import FilterConfigLoader
from filters.filter_engine import FilterEngine

logger = logging.getLogger(__name__)


def execute_strategy(
    factor_data: pl.DataFrame, 
    config: dict, 
    pipeline = None
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    执行策略并应用过滤器
    
    Args:
        factor_data: 因子数据
        config: 配置
        pipeline: 流水线管理器
    
    Returns:
        (结果字典, 元数据)
    """
    logger.info("[EXECUTE] 开始执行策略")
    
    total_capital = config.get('backtest', {}).get('initial_capital', 1000000)
    
    strategy_engine = FundBehaviorStrategyEngine()
    
    trend_stocks = strategy_engine.select_trend_stocks(factor_data)
    short_term_stocks = strategy_engine.select_short_term_stocks(factor_data, upward_pivot=True)
    
    all_selected = list(set(trend_stocks + short_term_stocks))
    
    logger.info(f"[EXECUTE] 选出 {len(all_selected)} 只候选股票")
    
    filter_config_path = config.get('filter_config', 'config/filters/fund_behavior_filters.yaml')
    
    try:
        filter_loader = FilterConfigLoader(filter_config_path)
        filter_config = filter_loader.load_config()
        
        filter_engine = FilterEngine()
        filter_engine.load_filters(filter_config)
        
        filtered_data = factor_data.filter(pl.col("code").is_in(all_selected))
        
        filtered_data = filter_engine.apply_filters(filtered_data)
        
        filtered_codes = filtered_data["code"].unique().to_list()
        
        logger.info(f"[EXECUTE] 过滤后剩余 {len(filtered_codes)} 只股票")
        
        result = {
            'stocks': [],
            'summary': {
                'total_candidates': len(all_selected),
                'total_filtered': len(filtered_codes),
                'filters_applied': list(filter_config.get('filters', {}).keys())
            },
            'filter_stats': filter_engine.get_stats()
        }
        
        if len(filtered_codes) > 0:
            final_data = filtered_data.filter(pl.col("code").is_in(filtered_codes))
            
            stock_list = final_data.select(["code", "name", "trade_date", "close", "volume"]).to_dicts()
            
            for stock in stock_list[:20]:
                result['stocks'].append({
                    'code': stock.get('code'),
                    'name': stock.get('name'),
                    'score': 0,
                    'trade_date': stock.get('trade_date'),
                    'close': stock.get('close'),
                    'volume': stock.get('volume')
                })
        
        meta = {
            'trend_stocks': len(trend_stocks),
            'short_term_stocks': len(short_term_stocks),
            'candidates': len(all_selected),
            'filtered': len(filtered_codes)
        }
        
        logger.info("[EXECUTE] ✅ 策略执行完成")
        
        return result, meta
        
    except Exception as e:
        logger.error(f"[EXECUTE] ❌ 过滤器执行失败: {e}")
        
        result = {
            'stocks': [],
            'summary': {
                'total_candidates': len(all_selected),
                'total_filtered': 0,
                'filters_applied': []
            },
            'filter_stats': {},
            'error': str(e)
        }
        
        meta = {
            'trend_stocks': len(trend_stocks),
            'short_term_stocks': len(short_term_stocks),
            'candidates': len(all_selected),
            'filtered': 0,
            'error': str(e)
        }
        
        return result, meta
