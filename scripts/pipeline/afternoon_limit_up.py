#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘后涨停板选股脚本

整合所有策略进行盘后选股：
1. 涨停回调战法 (limitup_callback)
2. 龙回头策略 (dragon_head)
3. 尾盘突袭策略 (tail_rush)
4. 资金共振策略 (fund_resonance)

每个策略选出1只股票，共4只
用于次日跟踪和复盘

Author: AI Assistant
Date: 2026-04-27
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict

import pandas as pd
import yaml

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.pipeline.limitup_data_collect import LimitUpDataCollector

# 导入技术分析模块
try:
    sys.path.insert(0, str(project_root))
    from core.technical_analysis import TechnicalAnalyzer
    TECHNICAL_ANALYSIS = True
except ImportError:
    TECHNICAL_ANALYSIS = False
    logger.warning("技术分析模块未加载")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            project_root / 'logs' / f'afternoon_limitup_{datetime.now().strftime("%Y%m%d")}.log',
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)


class AfternoonLimitUpSelector:
    """盘后涨停板选股器"""

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.data_dir = Path(self.config.get('data', {}).get('kline_dir', 'data/kline'))
        self.output_dir = Path(self.config.get('data', {}).get('selection_dir', 'data/selection'))
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.limitup_collector = LimitUpDataCollector()

    def _load_config(self, config_path: str = None) -> dict:
        """加载配置文件"""
        if config_path is None:
            config_path = project_root / 'config' / 'limitup_config.yaml'

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Failed to load config: {e}, using defaults")
            return {
                'data': {'kline_dir': 'data/kline', 'selection_dir': 'data/selection'},
                'selection': {'stocks_per_strategy': 1}
            }

    def run_limitup_callback_strategy(self, limitup_df: pd.DataFrame, trade_date: str) -> List[Dict]:
        """运行涨停回调战法策略"""
        logger.info("=" * 60)
        logger.info("运行涨停回调战法策略")
        logger.info("=" * 60)

        if limitup_df.empty:
            logger.warning("No limitup data available")
            return []

        config = self.config.get('strategies', {}).get('limitup_callback', {})
        max_limitup = config.get('parameters', {}).get('max_consecutive_limitup', 3)
        max_turnover = config.get('parameters', {}).get('max_turnover_ratio', 0.15)

        signals = []

        candidates = limitup_df[limitup_df['consecutive_days'] <= max_limitup].copy()
        logger.info(f"Step 1 - 昨日涨停且连板<=3: {len(candidates)} 只")

        for _, stock in candidates.iterrows():
            code = stock['code']
            name = stock['name']

            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                # 兼容不同列名
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date].tail(30)

                if len(kline) < 20:
                    continue

                kline['ma_20'] = kline['close'].rolling(20).mean()
                kline['volume_20_avg'] = kline['volume'].rolling(20).mean()

                # 检查数据是否有效
                if len(kline) == 0:
                    continue

                # 重新获取最新行（包含新计算的列）
                latest = kline.iloc[-1]

                # 检查新列是否存在
                if 'ma_20' not in latest or 'volume_20_avg' not in latest:
                    continue

                turnover = latest.get('turnover_ratio', 0)
                if turnover > max_turnover:
                    continue

                close = latest['close']
                ma_20 = latest['ma_20']

                if pd.isna(ma_20) or ma_20 == 0:
                    continue

                tolerance = 0.02
                near_ma20 = (ma_20 * (1 - tolerance)) <= close <= (ma_20 * (1 + tolerance))

                if not near_ma20:
                    continue

                volume = latest['volume']
                volume_20_avg = latest['volume_20_avg']
                is_red = close > latest['open']
                volume_surge = volume > volume_20_avg * 1.5

                if not (is_red and volume_surge):
                    continue

                # 技术分析检查
                pattern_tags = []
                if TECHNICAL_ANALYSIS:
                    analyzer = TechnicalAnalyzer(kline)
                    
                    # 多头排列检查
                    is_bullish, _ = analyzer.check_bullish_alignment()
                    if not is_bullish:
                        logger.debug(f"{code} 未通过多头排列检查")
                        continue
                    pattern_tags.append("多头排列")
                    
                    # 乖离率检查
                    bias = analyzer.calculate_bias()
                    if bias > 10:
                        logger.debug(f"{code} 乖离率过高: {bias}%")
                        continue
                    
                    # ATR动态止损
                    atr_stop = analyzer.get_atr_stop_loss(multiplier=2.0)
                else:
                    atr_stop = ma_20 * 0.97

                signal = {
                    'code': code,
                    'name': name,
                    'strategy': 'limitup_callback',
                    'trade_date': trade_date,
                    'entry_price': close,
                    'stoploss_price': atr_stop,
                    'take_profit_1': close * 1.10,
                    'take_profit_2': close * 1.20,
                    'confidence': 0.8,
                    'reason': f'涨停回调至20日均线，放量阳线，{"+".join(pattern_tags) if pattern_tags else "多头排列"}',
                    'indicators': {
                        'consecutive_days': stock.get('consecutive_days', 1),
                        'turnover_ratio': turnover,
                        'ma_20': ma_20,
                        'volume_ratio': volume / volume_20_avg if volume_20_avg > 0 else 0,
                        'pattern_tags': pattern_tags
                    }
                }
                signals.append(signal)
                logger.info(f"✅ 信号: {code} {name} - {signal['reason']}")

            except Exception as e:
                import traceback
                logger.warning(f"Error analyzing {code}: {e}")
                logger.debug(f"Traceback: {traceback.format_exc()}")
                continue

        signals = sorted(signals, key=lambda x: x['confidence'], reverse=True)
        max_selections = self.config.get('selection', {}).get('stocks_per_strategy', 1)

        logger.info(f"涨停回调战法选出 {len(signals[:max_selections])} 只股票")
        return signals[:max_selections]

    def run_dragon_head_strategy(self, limitup_df: pd.DataFrame, trade_date: str) -> List[Dict]:
        """运行龙回头策略"""
        logger.info("=" * 60)
        logger.info("运行龙回头策略")
        logger.info("=" * 60)

        if limitup_df.empty:
            logger.warning("No limitup data available")
            return []

        config = self.config.get('strategies', {}).get('dragon_head', {})
        params = config.get('parameters', {})
        min_consecutive = params.get('min_consecutive_limitup', 3)
        max_pullback = params.get('max_pullback_pct', 0.15)
        min_pullback = params.get('min_pullback_pct', 0.05)

        signals = []

        candidates = limitup_df[limitup_df['consecutive_days'] >= min_consecutive].copy()
        logger.info(f"Step 1 - 连板>=3的龙头股: {len(candidates)} 只")

        for _, stock in candidates.iterrows():
            code = stock['code']
            name = stock['name']

            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                # 兼容不同列名
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date].tail(40)

                # 严格检查数据有效性
                if len(kline) < 30:
                    continue

                # 检查必要列是否存在
                required_cols = ['high', 'low', 'close', 'open', 'volume']
                if not all(col in kline.columns for col in required_cols):
                    continue

                high_price = kline['high'].max()

                # 再次检查数据不为空
                if len(kline) == 0:
                    continue

                latest = kline.iloc[-1]
                close = latest['close']

                pullback = (high_price - close) / high_price

                if not (min_pullback <= pullback <= max_pullback):
                    continue

                kline['ma_5'] = kline['close'].rolling(5).mean()
                kline['ma_10'] = kline['close'].rolling(10).mean()

                # 检查数据是否有效
                if len(kline) == 0:
                    continue

                # 重新获取最新行（包含新计算的均线）
                latest = kline.iloc[-1]

                # 检查均线是否计算成功
                if 'ma_5' not in latest or 'ma_10' not in latest:
                    continue

                ma5 = latest['ma_5']
                ma10 = latest['ma_10']

                if pd.isna(ma5) or pd.isna(ma10):
                    continue

                golden_cross = ma5 > ma10
                price_above_ma = close > ma5

                if not (golden_cross and price_above_ma):
                    continue

                # 技术分析：企稳形态 + 成交量模式
                pattern_tags = []
                if TECHNICAL_ANALYSIS:
                    analyzer = TechnicalAnalyzer(kline)
                    
                    # 检查企稳形态
                    is_stabilizing, stab_desc = analyzer.check_stabilization_pattern(days=2)
                    if not is_stabilizing:
                        logger.debug(f"{code} 未通过企稳检查: {stab_desc}")
                        continue
                    pattern_tags.append(stab_desc)
                    
                    # 检查成交量萎缩后放量（龙回头核心逻辑）
                    high_idx = kline['close'].idxmax()
                    high_position = kline.index.get_loc(high_idx)
                    if high_position >= 5 and high_position < len(kline) - 3:
                        surge_volume = kline.iloc[high_position-5:high_position]['volume'].mean()
                        pullback_volume = kline.iloc[high_position:]['volume'].mean()
                        volume_contraction = pullback_volume < surge_volume * 0.5
                        recent_volume = kline.tail(3)['volume'].mean()
                        volume_rebound = recent_volume > pullback_volume * 1.3
                        
                        if not (volume_contraction and volume_rebound):
                            logger.debug(f"{code} 成交量模式不符")
                            continue
                        pattern_tags.append(f"缩量{pullback_volume/surge_volume*100:.0f}%")
                    
                    # 使用ATR动态止损
                    atr_stop = analyzer.get_atr_stop_loss(multiplier=2.0)
                else:
                    atr_stop = close * 0.93

                signal = {
                    'code': code,
                    'name': name,
                    'strategy': 'dragon_head',
                    'trade_date': trade_date,
                    'entry_price': close,
                    'stoploss_price': atr_stop,
                    'take_profit_1': close * 1.10,
                    'take_profit_2': close * 1.20,
                    'confidence': 0.75 + (pullback - min_pullback) * 2,
                    'reason': f'龙回头：回调{pullback*100:.1f}%，{"+".join(pattern_tags)}',
                    'indicators': {
                        'consecutive_days': stock.get('consecutive_days', 3),
                        'pullback_pct': pullback,
                        'high_price': high_price,
                        'ma_5': ma5,
                        'ma_10': ma10,
                        'pattern_tags': pattern_tags
                    }
                }
                signals.append(signal)
                logger.info(f"✅ 信号: {code} {name} - {signal['reason']}")

            except Exception as e:
                import traceback
                logger.warning(f"Error analyzing {code}: {e}")
                logger.debug(f"Traceback: {traceback.format_exc()}")
                continue

        signals = sorted(signals, key=lambda x: x['confidence'], reverse=True)
        max_selections = self.config.get('selection', {}).get('stocks_per_strategy', 1)

        logger.info(f"龙回头策略选出 {len(signals[:max_selections])} 只股票")
        return signals[:max_selections]


    def run_tail_rush_strategy(self, limitup_df: pd.DataFrame, trade_date: str) -> List[Dict]:
        """
        尾盘突袭策略
        
        条件:
        1. 尾盘30分钟内涨停
        2. 当日换手率 > 5%
        3. 量能放大（相比20日均量）
        """
        logger.info("=" * 60)
        logger.info("运行尾盘突袭策略")
        logger.info("=" * 60)

        if limitup_df.empty:
            logger.warning("No limitup data available")
            return []

        config = self.config.get('strategies', {}).get('tail_rush', {})
        params = config.get('parameters', {})
        min_turnover = params.get('min_turnover_ratio', 0.05)
        min_volume_ratio = params.get('min_volume_ratio', 1.5)

        signals = []
        candidates = limitup_df.copy()
        logger.info(f"Step 1 - 涨停股票: {len(candidates)} 只")

        for _, stock in candidates.iterrows():
            code = stock['code']
            name = stock['name']

            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date].tail(30)

                if len(kline) < 20:
                    continue

                latest = kline.iloc[-1]
                turnover = latest.get('turnover_ratio', 0)
                if turnover < min_turnover:
                    continue

                kline['volume_20_avg'] = kline['volume'].rolling(20).mean()
                avg_volume = kline['volume_20_avg'].iloc[-1]
                
                if pd.isna(avg_volume) or avg_volume == 0:
                    continue
                
                volume_ratio = latest['volume'] / avg_volume
                if volume_ratio < min_volume_ratio:
                    continue

                close = latest['close']
                signal = {
                    'code': code,
                    'name': name,
                    'strategy': 'tail_rush',
                    'trigger_price': close * 1.02,
                    'stoploss_price': close * 0.95,
                    'take_profit_1': close * 1.05,
                    'take_profit_2': close * 1.10,
                    'confidence': 0.65,
                    'reason': f'尾盘突袭，换手{turnover*100:.1f}%，量比{volume_ratio:.1f}倍',
                    'trade_date': trade_date,
                    'indicators': {
                        'turnover_ratio': turnover,
                        'volume_ratio': volume_ratio
                    }
                }
                signals.append(signal)

            except Exception as e:
                logger.warning(f"Error analyzing {code}: {e}")
                continue

        signals = sorted(signals, key=lambda x: x['indicators']['volume_ratio'], reverse=True)
        signals = signals[:self.config.get('selection', {}).get('stocks_per_strategy', 1)]
        logger.info(f"尾盘突袭策略选出 {len(signals)} 只股票")
        return signals

    def run_fund_resonance_strategy(self, limitup_df: pd.DataFrame, trade_date: str) -> List[Dict]:
        """
        资金共振策略
        
        条件:
        1. 资金流入强度 > 10%
        2. 换手率 > 5%
        3. 技术突破MA20
        """
        logger.info("=" * 60)
        logger.info("运行资金共振策略")
        logger.info("=" * 60)

        if limitup_df.empty:
            logger.warning("No limitup data available")
            return []

        config = self.config.get('strategies', {}).get('fund_resonance', {})
        params = config.get('parameters', {})
        min_fund_inflow = params.get('min_fund_inflow_pct', 0.10)
        min_turnover = params.get('min_turnover_ratio', 0.05)

        signals = []
        candidates = limitup_df.copy()
        logger.info(f"Step 1 - 涨停股票: {len(candidates)} 只")

        for _, stock in candidates.iterrows():
            code = stock['code']
            name = stock['name']

            try:
                file_path = self.data_dir / f"{code}.parquet"
                if not file_path.exists():
                    continue

                kline = pd.read_parquet(file_path)
                date_col = 'trade_date' if 'trade_date' in kline.columns else 'date'
                kline[date_col] = pd.to_datetime(kline[date_col]).dt.strftime('%Y-%m-%d')
                kline = kline[kline[date_col] <= trade_date].tail(30)

                if len(kline) < 20:
                    continue

                latest = kline.iloc[-1]
                turnover = latest.get('turnover_ratio', 0)
                if turnover < min_turnover:
                    continue

                kline['ma_20'] = kline['close'].rolling(20).mean()
                ma_20 = kline['ma_20'].iloc[-1]
                
                if pd.isna(ma_20):
                    continue

                close = latest['close']
                if close <= ma_20:
                    continue

                kline['amount_20_avg'] = kline['amount'].rolling(20).mean()
                avg_amount = kline['amount_20_avg'].iloc[-1]
                
                if pd.isna(avg_amount) or avg_amount == 0:
                    continue
                
                fund_inflow_ratio = (latest['amount'] - avg_amount) / avg_amount
                if fund_inflow_ratio < min_fund_inflow:
                    continue

                signal = {
                    'code': code,
                    'name': name,
                    'strategy': 'fund_resonance',
                    'trigger_price': close * 1.02,
                    'stoploss_price': ma_20 * 0.98,
                    'take_profit_1': close * 1.08,
                    'take_profit_2': close * 1.15,
                    'confidence': 0.70,
                    'reason': f'资金共振，换手{turnover*100:.1f}%，资金流入{fund_inflow_ratio*100:.1f}%，突破MA20',
                    'trade_date': trade_date,
                    'indicators': {
                        'turnover_ratio': turnover,
                        'fund_inflow_ratio': fund_inflow_ratio,
                        'ma_20': ma_20
                    }
                }
                signals.append(signal)

            except Exception as e:
                logger.warning(f"Error analyzing {code}: {e}")
                continue

        signals = sorted(signals, key=lambda x: x['indicators']['fund_inflow_ratio'], reverse=True)
        signals = signals[:self.config.get('selection', {}).get('stocks_per_strategy', 1)]
        logger.info(f"资金共振策略选出 {len(signals)} 只股票")
        return signals
    def run_all_strategies(self, trade_date: str = None) -> List[Dict]:
        """运行所有策略"""
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        logger.info(f"\n{'='*60}")
        logger.info(f"盘后涨停板选股: {trade_date}")
        logger.info(f"{'='*60}\n")

        # 获取涨停数据
        limitup_df = self.limitup_collector.fetch_limit_up_data()
        if limitup_df.empty:
            logger.warning(f"No limitup data for {trade_date}")
            return []

        all_signals = []

        signals = self.run_limitup_callback_strategy(limitup_df, trade_date)
        all_signals.extend(signals)

        signals = self.run_dragon_head_strategy(limitup_df, trade_date)
        all_signals.extend(signals)

        signals = self.run_tail_rush_strategy(limitup_df, trade_date)
        all_signals.extend(signals)

        signals = self.run_fund_resonance_strategy(limitup_df, trade_date)
        all_signals.extend(signals)

        

        return all_signals

    def save_selection(self, signals: List[Dict], trade_date: str) -> bool:
        """保存选股结果到文件和数据库"""
        if not signals:
            logger.warning("No signals to save")
            return False

        try:
            # 保存为Parquet文件
            output_file = self.output_dir / f"selection_{trade_date}.parquet"
            df = pd.DataFrame(signals)
            df.to_parquet(output_file, index=False, compression='zstd')
            logger.info(f"选股结果已保存: {output_file}")

            # 保存到数据库
            self._save_to_database(signals, trade_date)

            return True
        except Exception as e:
            logger.error(f"Failed to save selection: {e}")
            return False

    def _save_to_database(self, signals: List[Dict], trade_date: str):
        """保存选股结果到数据库 stock_selections 表"""
        try:
            import pymysql

            # 获取数据库配置
            db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', '3306')),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', ''),
                'database': os.getenv('DB_NAME', 'xcn_db'),
                'charset': 'utf8mb4'
            }

            conn = pymysql.connect(**db_config)
            cursor = conn.cursor()

            # 插入数据
            insert_sql = """
                INSERT INTO stock_selections
                (code, name, strategy, trigger_price, stoploss_price, take_profit_1, take_profit_2,
                 confidence, reason, selection_date, selection_time, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            now = datetime.now()
            selection_time = now.strftime('%H:%M:%S')

            for signal in signals:
                cursor.execute(insert_sql, (
                    signal.get('code', ''),
                    signal.get('name', ''),
                    signal.get('strategy', ''),
                    signal.get('trigger_price', 0),
                    signal.get('stoploss_price', 0),
                    signal.get('take_profit_1', 0),
                    signal.get('take_profit_2', 0),
                    signal.get('confidence', 0),
                    signal.get('reason', ''),
                    trade_date,
                    selection_time,
                    now
                ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Saved {len(signals)} selections to database")
            return True

        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            return False

    def generate_report(self, signals: List[Dict], trade_date: str) -> str:
        """生成选股报告"""
        if not signals:
            return "无选股信号"

        report = []
        report.append("=" * 60)
        report.append(f"盘后涨停板选股报告 - {trade_date}")
        report.append("=" * 60)
        report.append("")

        for signal in signals:
            report.append(f"【{signal['strategy']}】")
            report.append(f"  股票: {signal['code']} {signal['name']}")
            report.append(f"  买入价: {signal['entry_price']:.2f}")
            report.append(f"  止损价: {signal['stoploss_price']:.2f}")
            report.append(f"  止盈1: {signal['take_profit_1']:.2f}")
            report.append(f"  止盈2: {signal['take_profit_2']:.2f}")
            report.append(f"  置信度: {signal['confidence']:.2f}")
            report.append(f"  理由: {signal['reason']}")
            report.append("")

        return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description='盘后涨停板选股')
    parser.add_argument('--date', type=str, help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--output', type=str, help='输出文件路径')

    args = parser.parse_args()

    selector = AfternoonLimitUpSelector(config_path=args.config)

    trade_date = args.date
    if trade_date is None:
        trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    signals = selector.run_all_strategies(trade_date)

    if signals:
        selector.save_selection(signals, trade_date)
        report = selector.generate_report(signals, trade_date)
        print("\n" + report)

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"\n报告已保存到: {args.output}")
    else:
        print("\n未找到符合条件的股票")

    return 0


if __name__ == '__main__':
    sys.exit(main())
