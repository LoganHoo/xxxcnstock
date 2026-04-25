#!/usr/bin/env python3
"""
历史回测引擎

实现真正的历史回测:
1. 历史数据切片 - 基于指定日期获取历史数据快照
2. 滚动回测 - 按时间窗口滚动执行选股
3. 收益计算 - 计算持仓期间的真实收益
4. 绩效分析 - 生成回测报告

使用示例:
    engine = HistoricalBacktestEngine()
    results = engine.run_backtest(
        start_date="2026-01-01",
        end_date="2026-04-17",
        lookback_days=60,  # 使用60天历史数据
        rebalance_freq="weekly",  # 每周调仓
        holding_days=5  # 持仓5天
    )
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("historical_backtest")


class RebalanceFreq(Enum):
    """调仓频率"""
    DAILY = "daily"           # 每日
    WEEKLY = "weekly"         # 每周
    MONTHLY = "monthly"       # 每月


@dataclass
class Trade:
    """交易记录"""
    date: str
    code: str
    action: str  # 'buy' or 'sell'
    price: float
    shares: int
    value: float
    reason: str = ""


@dataclass
class Position:
    """持仓记录"""
    code: str
    shares: int
    cost_price: float
    buy_date: str
    current_price: float = 0.0
    current_value: float = 0.0
    profit_pct: float = 0.0


@dataclass
class DailyRecord:
    """每日记录"""
    date: str
    cash: float
    positions_value: float
    total_value: float
    return_pct: float
    positions: List[Position] = field(default_factory=list)


@dataclass
class BacktestResult:
    """回测结果"""
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_return: float
    annualized_return: float
    max_drawdown: float
    sharpe_ratio: float
    trade_count: int
    win_rate: float
    avg_holding_days: float
    profit_factor: float
    trades: List[Trade] = field(default_factory=list)
    daily_records: List[DailyRecord] = field(default_factory=list)


class HistoricalBacktestEngine:
    """历史回测引擎"""
    
    def __init__(self, data_path: Optional[Path] = None):
        self.logger = logger
        self.data_path = data_path or get_data_path()
        self.kline_dir = self.data_path / "kline"
        
        # 回测结果保存目录
        self.results_dir = self.data_path / "backtest_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run_backtest(
        self,
        start_date: str,
        end_date: str,
        lookback_days: int = 60,
        rebalance_freq: RebalanceFreq = RebalanceFreq.WEEKLY,
        holding_days: int = 5,
        initial_capital: float = 1000000,
        position_size: int = 10,
        strategy_type: str = "comprehensive"
    ) -> BacktestResult:
        """
        运行历史回测
        
        Args:
            start_date: 回测开始日期 (YYYY-MM-DD)
            end_date: 回测结束日期 (YYYY-MM-DD)
            lookback_days: 历史数据回看天数
            rebalance_freq: 调仓频率
            holding_days: 持仓天数
            initial_capital: 初始资金
            position_size: 持仓股票数量
            strategy_type: 策略类型
        
        Returns:
            回测结果
        """
        self.logger.info(f"开始历史回测: {start_date} ~ {end_date}")
        self.logger.info(f"参数: lookback={lookback_days}天, rebalance={rebalance_freq.value}, holding={holding_days}天")
        
        # 生成调仓日期
        rebalance_dates = self._generate_rebalance_dates(start_date, end_date, rebalance_freq)
        self.logger.info(f"调仓日期数量: {len(rebalance_dates)}")
        
        # 回测状态
        cash = initial_capital
        positions: Dict[str, Position] = {}
        trades: List[Trade] = []
        daily_records: List[DailyRecord] = []
        
        # 获取所有交易日
        all_dates = self._get_all_trade_dates(start_date, end_date)
        
        for current_date in all_dates:
            # 记录每日状态
            positions_value = self._calculate_positions_value(positions, current_date)
            total_value = cash + positions_value
            
            daily_record = DailyRecord(
                date=current_date,
                cash=cash,
                positions_value=positions_value,
                total_value=total_value,
                return_pct=0.0,  # 稍后计算
                positions=list(positions.values())
            )
            daily_records.append(daily_record)
            
            # 检查是否需要调仓
            if current_date in rebalance_dates:
                self.logger.info(f"调仓日: {current_date}, 当前净值: {total_value:,.2f}")
                
                # 1. 卖出当前持仓
                for code in list(positions.keys()):
                    sell_price = self._get_price_on_date(code, current_date)
                    if sell_price:
                        pos = positions[code]
                        sell_value = pos.shares * sell_price
                        cash += sell_value
                        
                        # 记录交易
                        trades.append(Trade(
                            date=current_date,
                            code=code,
                            action='sell',
                            price=sell_price,
                            shares=pos.shares,
                            value=sell_value,
                            reason='rebalance'
                        ))
                        
                        del positions[code]
                
                # 2. 选股
                lookback_start = (datetime.strptime(current_date, '%Y-%m-%d') - 
                                 timedelta(days=lookback_days)).strftime('%Y-%m-%d')
                
                selected_stocks = self._select_stocks(
                    current_date, 
                    lookback_start, 
                    current_date,
                    position_size,
                    strategy_type
                )
                
                # 3. 买入新持仓
                if selected_stocks and cash > 0:
                    capital_per_stock = cash / len(selected_stocks)
                    
                    for stock in selected_stocks:
                        code = stock['code']
                        buy_price = self._get_price_on_date(code, current_date)
                        
                        if buy_price and buy_price > 0:
                            shares = int(capital_per_stock / buy_price / 100) * 100
                            
                            if shares > 0:
                                buy_value = shares * buy_price
                                cash -= buy_value
                                
                                positions[code] = Position(
                                    code=code,
                                    shares=shares,
                                    cost_price=buy_price,
                                    buy_date=current_date
                                )
                                
                                trades.append(Trade(
                                    date=current_date,
                                    code=code,
                                    action='buy',
                                    price=buy_price,
                                    shares=shares,
                                    value=buy_value,
                                    reason='selected'
                                ))
        
        # 计算回测结果
        result = self._calculate_backtest_result(
            initial_capital, 
            daily_records, 
            trades,
            start_date,
            end_date
        )
        
        self.logger.info(f"回测完成: 总收益率 {result.total_return:.2%}, 最终资金 {result.final_capital:,.2f}")
        
        return result
    
    def _generate_rebalance_dates(self, start_date: str, end_date: str, 
                                  freq: RebalanceFreq) -> List[str]:
        """生成调仓日期"""
        dates = []
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current <= end:
            if freq == RebalanceFreq.DAILY:
                dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            elif freq == RebalanceFreq.WEEKLY:
                if current.weekday() == 0:  # 周一
                    dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            elif freq == RebalanceFreq.MONTHLY:
                if current.day == 1:
                    dates.append(current.strftime('%Y-%m-%d'))
                # 移动到下个月
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    current = current.replace(month=current.month + 1, day=1)
        
        return dates
    
    def _get_all_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取所有交易日"""
        # 从K线数据中提取所有交易日
        dates = set()
        
        # 抽样检查前10个文件
        sample_files = list(self.kline_dir.glob("*.parquet"))[:10]
        
        for f in sample_files:
            try:
                df = pd.read_parquet(f)
                if 'trade_date' in df.columns:
                    file_dates = pd.to_datetime(df['trade_date'])
                    for d in file_dates:
                        date_str = d.strftime('%Y-%m-%d')
                        if start_date <= date_str <= end_date:
                            dates.add(date_str)
            except Exception as e:
                self.logger.warning(f"读取文件失败 {f}: {e}")
        
        return sorted(list(dates))
    
    def _get_price_on_date(self, code: str, date: str) -> Optional[float]:
        """获取指定日期的收盘价"""
        try:
            file_path = self.kline_dir / f"{code}.parquet"
            if not file_path.exists():
                return None
            
            df = pd.read_parquet(file_path)
            if df.empty or 'trade_date' not in df.columns:
                return None
            
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            target_date = pd.to_datetime(date)
            
            # 找到指定日期或之前的最新数据
            mask = df['trade_date'] <= target_date
            if not mask.any():
                return None
            
            latest = df[mask].iloc[-1]
            return float(latest['close'])
            
        except Exception as e:
            self.logger.debug(f"获取价格失败 {code} {date}: {e}")
            return None
    
    def _select_stocks(self, current_date: str, start_date: str, end_date: str,
                       position_size: int, strategy_type: str) -> List[Dict]:
        """选股 (简化版)"""
        selected = []
        
        # 获取所有股票代码
        stock_files = list(self.kline_dir.glob("*.parquet"))
        
        for f in stock_files[:100]:  # 限制数量以加速回测
            code = f.stem
            try:
                df = pd.read_parquet(f)
                if df.empty or 'trade_date' not in df.columns:
                    continue
                
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 筛选日期范围
                mask = (df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)
                hist = df[mask]
                
                if len(hist) < 20:  # 需要至少20天数据
                    continue
                
                # 计算简单评分
                latest = hist.iloc[-1]
                
                # 价格趋势评分
                if len(hist) >= 60:
                    price_change_60d = (latest['close'] - hist.iloc[-60]['close']) / hist.iloc[-60]['close'] * 100
                else:
                    price_change_60d = 0
                
                # 近期动量
                if len(hist) >= 5:
                    price_change_5d = (latest['close'] - hist.iloc[-5]['close']) / hist.iloc[-5]['close'] * 100
                else:
                    price_change_5d = 0
                
                # 波动率
                if 'pct_chg' in hist.columns:
                    volatility = hist['pct_chg'].std()
                else:
                    volatility = 0
                
                # 综合评分 (简化版)
                score = 50.0
                score += min(price_change_60d * 0.5, 20)  # 长期趋势
                score += min(price_change_5d * 2, 15)     # 短期动量
                score -= min(volatility * 2, 10)          # 波动率惩罚
                
                selected.append({
                    'code': code,
                    'score': score,
                    'price': latest['close']
                })
                
            except Exception as e:
                continue
        
        # 按评分排序并选择Top N
        selected.sort(key=lambda x: x['score'], reverse=True)
        return selected[:position_size]
    
    def _calculate_positions_value(self, positions: Dict[str, Position], 
                                   current_date: str) -> float:
        """计算持仓市值"""
        total = 0.0
        for code, pos in positions.items():
            price = self._get_price_on_date(code, current_date)
            if price:
                total += pos.shares * price
        return total
    
    def _calculate_backtest_result(self, initial_capital: float,
                                   daily_records: List[DailyRecord],
                                   trades: List[Trade],
                                   start_date: str,
                                   end_date: str) -> BacktestResult:
        """计算回测结果"""
        if not daily_records:
            return BacktestResult(
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                final_capital=initial_capital,
                total_return=0.0,
                annualized_return=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                trade_count=0,
                win_rate=0.0,
                avg_holding_days=0.0,
                profit_factor=0.0,
                trades=[],
                daily_records=[]
            )
        
        final_value = daily_records[-1].total_value
        total_return = (final_value - initial_capital) / initial_capital
        
        # 计算最大回撤
        peak = initial_capital
        max_drawdown = 0.0
        for record in daily_records:
            if record.total_value > peak:
                peak = record.total_value
            drawdown = (peak - record.total_value) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 计算年化收益率
        days = len(daily_records)
        if days > 0:
            annualized_return = (1 + total_return) ** (252 / days) - 1
        else:
            annualized_return = 0.0
        
        # 计算胜率
        sell_trades = [t for t in trades if t.action == 'sell']
        win_count = 0
        for sell in sell_trades:
            # 找到对应的买入
            for buy in trades:
                if buy.action == 'buy' and buy.code == sell.code and buy.date < sell.date:
                    if sell.price > buy.price:
                        win_count += 1
                    break
        
        win_rate = win_count / len(sell_trades) if sell_trades else 0.0
        
        # 计算平均持仓天数
        total_holding_days = 0
        for sell in sell_trades:
            for buy in trades:
                if buy.action == 'buy' and buy.code == sell.code:
                    buy_dt = datetime.strptime(buy.date, '%Y-%m-%d')
                    sell_dt = datetime.strptime(sell.date, '%Y-%m-%d')
                    total_holding_days += (sell_dt - buy_dt).days
                    break
        
        avg_holding_days = total_holding_days / len(sell_trades) if sell_trades else 0.0
        
        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            final_capital=final_value,
            total_return=total_return,
            annualized_return=annualized_return,
            max_drawdown=max_drawdown,
            sharpe_ratio=0.0,  # 简化计算
            trade_count=len(trades),
            win_rate=win_rate,
            avg_holding_days=avg_holding_days,
            profit_factor=0.0,
            trades=trades,
            daily_records=daily_records
        )
    
    def save_result(self, result: BacktestResult, filename: Optional[str] = None):
        """保存回测结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"backtest_{result.start_date}_{result.end_date}_{timestamp}.json"
        
        filepath = self.results_dir / filename
        
        # 转换为字典
        data = {
            'start_date': result.start_date,
            'end_date': result.end_date,
            'initial_capital': result.initial_capital,
            'final_capital': result.final_capital,
            'total_return': result.total_return,
            'annualized_return': result.annualized_return,
            'max_drawdown': result.max_drawdown,
            'sharpe_ratio': result.sharpe_ratio,
            'trade_count': result.trade_count,
            'win_rate': result.win_rate,
            'avg_holding_days': result.avg_holding_days,
            'profit_factor': result.profit_factor,
            'trades': [
                {
                    'date': t.date,
                    'code': t.code,
                    'action': t.action,
                    'price': t.price,
                    'shares': t.shares,
                    'value': t.value,
                    'reason': t.reason
                } for t in result.trades
            ],
            'daily_records': [
                {
                    'date': r.date,
                    'cash': r.cash,
                    'positions_value': r.positions_value,
                    'total_value': r.total_value,
                    'return_pct': r.return_pct
                } for r in result.daily_records
            ]
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"回测结果已保存: {filepath}")
        return filepath


def main():
    """主函数 - 运行示例回测"""
    engine = HistoricalBacktestEngine()
    
    # 运行回测
    result = engine.run_backtest(
        start_date="2026-01-01",
        end_date="2026-04-17",
        lookback_days=60,
        rebalance_freq=RebalanceFreq.WEEKLY,
        holding_days=5,
        initial_capital=1000000,
        position_size=10
    )
    
    # 保存结果
    engine.save_result(result)
    
    # 打印摘要
    print("\n" + "="*60)
    print("回测结果摘要")
    print("="*60)
    print(f"回测期间: {result.start_date} ~ {result.end_date}")
    print(f"初始资金: {result.initial_capital:,.2f}")
    print(f"最终资金: {result.final_capital:,.2f}")
    print(f"总收益率: {result.total_return:.2%}")
    print(f"年化收益率: {result.annualized_return:.2%}")
    print(f"最大回撤: {result.max_drawdown:.2%}")
    print(f"交易次数: {result.trade_count}")
    print(f"胜率: {result.win_rate:.2%}")
    print(f"平均持仓天数: {result.avg_holding_days:.1f}")
    print("="*60)


if __name__ == '__main__':
    main()
