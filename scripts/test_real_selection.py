#!/usr/bin/env python3
"""
测试真实数据选股策略 - 小批量测试
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
from datetime import datetime
from pathlib import Path

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("test_real_selection")


def calculate_stock_score(code: str, kline: pd.DataFrame) -> dict:
    """计算单只股票的综合评分"""
    if kline is None or kline.empty or len(kline) < 20:
        return None
    
    try:
        latest = kline.iloc[-1]
        
        # 1. 财务评分 (基于价格趋势估算)
        financial_score = 50.0
        if len(kline) >= 60:
            price_60d_ago = kline.iloc[-60]['close']
            price_change_60d = (latest['close'] - price_60d_ago) / price_60d_ago * 100
            financial_score += min(price_change_60d * 0.5, 20)
        
        # 波动率调整
        if 'pct_chg' in kline.columns:
            volatility = kline['pct_chg'].std()
            financial_score -= min(volatility * 2, 15)
        
        financial_score = max(0, min(100, financial_score))
        
        # 2. 市场评分
        market_score = 50.0
        if len(kline) >= 5:
            price_change_5d = (kline.iloc[-1]['close'] - kline.iloc[-5]['close']) / kline.iloc[-5]['close'] * 100
            market_score += min(price_change_5d * 2, 25)
        market_score = max(0, min(100, market_score))
        
        # 3. 公告评分
        announcement_score = 50.0
        if 'pct_chg' in kline.columns and len(kline) >= 5:
            recent_changes = kline.iloc[-5:]['pct_chg'].abs()
            if not recent_changes.empty:
                max_change = recent_changes.max()
                if max_change > 5:
                    announcement_score = 70.0
                elif max_change > 3:
                    announcement_score = 60.0
        
        # 4. 技术评分
        technical_score = 50.0
        closes = kline['close'].values
        if len(closes) >= 20:
            ma20 = closes[-20:].mean()
            current_price = closes[-1]
            ma_position = (current_price - ma20) / ma20 * 100
            technical_score += min(ma_position * 2, 20)
            
            if len(closes) >= 10:
                trend = (current_price - closes[-10]) / closes[-10] * 100
                technical_score += min(trend * 1.5, 15)
        
        technical_score = max(0, min(100, technical_score))
        
        # 综合评分
        total_score = (
            financial_score * 0.35 +
            market_score * 0.30 +
            announcement_score * 0.20 +
            technical_score * 0.15
        )
        
        return {
            'code': code,
            'total_score': round(total_score, 2),
            'financial_score': round(financial_score, 2),
            'market_score': round(market_score, 2),
            'announcement_score': round(announcement_score, 2),
            'technical_score': round(technical_score, 2),
            'close_price': round(latest['close'], 2),
            'volume': int(latest['volume']),
            'price_change_5d': round((kline.iloc[-1]['close'] - kline.iloc[-5]['close']) / kline.iloc[-5]['close'] * 100, 2) if len(kline) >= 5 else 0
        }
        
    except Exception as e:
        logger.warning(f"计算 {code} 评分失败: {e}")
        return None


def main():
    """主函数"""
    data_path = get_data_path()
    kline_dir = data_path / "kline"
    
    # 获取前100只股票的K线数据
    stock_files = list(kline_dir.glob("*.parquet"))[:100]
    
    logger.info(f"开始测试 {len(stock_files)} 只股票的选股策略")
    
    scored_stocks = []
    
    for file_path in stock_files:
        code = file_path.stem
        try:
            kline = pd.read_parquet(file_path)
            score = calculate_stock_score(code, kline)
            if score:
                scored_stocks.append(score)
        except Exception as e:
            logger.warning(f"处理 {code} 失败: {e}")
    
    logger.info(f"成功计算 {len(scored_stocks)} 只股票的评分")
    
    # 排序并选择Top 20
    scored_stocks.sort(key=lambda x: x['total_score'], reverse=True)
    top_20 = scored_stocks[:20]
    
    # 输出结果
    print("\n" + "="*80)
    print("真实数据选股策略测试结果 (Top 20)")
    print("="*80)
    print(f"\n总测试股票数: {len(stock_files)}")
    print(f"成功评分股票数: {len(scored_stocks)}")
    
    print(f"\n{'排名':<6}{'代码':<10}{'综合':<8}{'财务':<8}{'市场':<8}{'公告':<8}{'技术':<8}{'收盘价':<10}{'5日涨幅':<10}")
    print("-"*80)
    
    for i, stock in enumerate(top_20, 1):
        print(f"{i:<6}{stock['code']:<10}{stock['total_score']:<8.1f}"
              f"{stock['financial_score']:<8.1f}{stock['market_score']:<8.1f}"
              f"{stock['announcement_score']:<8.1f}{stock['technical_score']:<8.1f}"
              f"{stock['close_price']:<10.2f}{stock['price_change_5d']:<10.2f}")
    
    print("\n" + "="*80)
    
    # 统计信息
    scores = [s['total_score'] for s in scored_stocks]
    print(f"\n评分统计:")
    print(f"  平均分: {sum(scores)/len(scores):.2f}")
    print(f"  最高分: {max(scores):.2f}")
    print(f"  最低分: {min(scores):.2f}")
    print(f"  中位数: {sorted(scores)[len(scores)//2]:.2f}")


if __name__ == "__main__":
    main()
