#!/usr/bin/env python3
"""
量化交易系统 V2 - 专业架构重构版

核心改进：
1. 双环架构：离线环 + 在线环
2. ETL规范：先清洗后质检
3. 回测引擎：因子有效性验证
4. 风控系统：独立风控校验
5. 性能优化：9:26极速响应
6. 动态头寸：实时权重调整

架构：
┌─────────────────────────────────────────────────────────────────┐
│                        离线环 (Offline Loop)                    │
│  数据采集 → ETL清洗 → 数据仓库 → 因子回测 → 策略优化            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        在线环 (Online Loop)                     │
│  实时行情 → 风控校验 → 订单执行 → 自动复盘 → 信号生成            │
└─────────────────────────────────────────────────────────────────┘
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
import json
import asyncio
import heapq
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from abc import ABC, abstractmethod

import pandas as pd
import polars as pl
import numpy as np

from core.workflow_framework import (
    WorkflowExecutor, WorkflowStatus, DependencyCheck, DependencyStatus,
    RetryConfig, Checkpoint
)
from core.logger import setup_logger
from core.paths import get_data_path
from core.market_guardian import enforce_market_closed, is_market_closed

# 导入新实现的模块
from services.data_service.fetchers.adjustment_factor_fetcher import (
    AdjustmentFactorFetcher, get_adj_factor_fetcher
)
from services.data_service.historical_data_loader import (
    HistoricalDataLoader, get_historical_loader
)
from services.data_service.realtime.market_data_stream import (
    RealtimeMarketDataStream, MarketDataType, create_realtime_stream
)
from models.opening_predictor import (
    LimitUpOpeningPredictor, create_opening_predictor, OpeningPrediction
)
from services.attribution.attribution_analyzer import (
    AttributionAnalyzer, create_attribution_analyzer, AttributionReport
)


# ============================================================================
# 1. 数据优先级队列 (解决性能瓶颈)
# ============================================================================

class DataPriority(Enum):
    """数据优先级"""
    CRITICAL = 1    # 核心行情数据（必须优先）
    HIGH = 2        # 重要数据（K线、指数）
    MEDIUM = 3      # 一般数据（基本面）
    LOW = 4         # 非关键数据（CCTV、外盘）
    BACKGROUND = 5  # 后台数据（历史补采）


@dataclass(order=True)
class DataTask:
    """数据采集任务"""
    priority: int
    task_id: str = field(compare=False)
    source_type: str = field(compare=False)
    execute_func: callable = field(compare=False)
    params: Dict = field(default_factory=dict, compare=False)
    timeout: int = 30
    retry_count: int = 3


class PriorityDataCollector:
    """优先级数据采集器"""
    
    def __init__(self, max_workers: int = 5):
        self.logger = setup_logger("priority_collector")
        self.task_queue = []
        self.max_workers = max_workers
        self.results = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def add_task(self, task: DataTask):
        """添加任务到优先级队列"""
        heapq.heappush(self.task_queue, task)
        self.logger.debug(f"添加任务: {task.task_id}, 优先级: {task.priority}")
    
    async def execute_all(self) -> Dict[str, Any]:
        """按优先级执行所有任务"""
        self.logger.info(f"开始执行 {len(self.task_queue)} 个采集任务")
        
        # 按优先级分组
        priority_groups = {}
        while self.task_queue:
            task = heapq.heappop(self.task_queue)
            if task.priority not in priority_groups:
                priority_groups[task.priority] = []
            priority_groups[task.priority].append(task)
        
        # 按优先级顺序执行
        for priority in sorted(priority_groups.keys()):
            tasks = priority_groups[priority]
            self.logger.info(f"执行优先级 {priority} 的任务: {len(tasks)} 个")
            
            # 并发执行同优先级任务
            futures = []
            for task in tasks:
                future = self.executor.submit(self._execute_task_with_retry, task)
                futures.append((task.task_id, future))
            
            # 收集结果
            for task_id, future in futures:
                try:
                    result = future.result(timeout=60)
                    self.results[task_id] = result
                except Exception as e:
                    self.logger.error(f"任务 {task_id} 执行失败: {e}")
                    self.results[task_id] = {"status": "failed", "error": str(e)}
        
        return self.results
    
    def _execute_task_with_retry(self, task: DataTask) -> Dict:
        """带重试的任务执行"""
        for attempt in range(task.retry_count):
            try:
                result = task.execute_func(**task.params)
                return {"status": "success", "data": result, "attempts": attempt + 1}
            except Exception as e:
                self.logger.warning(f"任务 {task.task_id} 第 {attempt + 1} 次尝试失败: {e}")
                if attempt < task.retry_count - 1:
                    time.sleep(2 ** attempt)  # 指数退避
        
        return {"status": "failed", "error": "Max retries exceeded", "attempts": task.retry_count}


# ============================================================================
# 2. ETL数据清洗模块 (修复顺序问题)
# ============================================================================

class ETLProcessor:
    """ETL处理器：Extract → Transform → Load"""
    
    def __init__(self):
        self.logger = setup_logger("etl_processor")
        self.data_dir = get_data_path()
    
    def process_kline_etl(self, raw_data: pl.DataFrame, code: str) -> pl.DataFrame:
        """
        K线数据ETL处理
        
        流程: 提取 → 清洗 → 标准化 → 除权除息 → 质量标记 → 加载
        """
        self.logger.info(f"ETL处理 {code} K线数据")
        
        # Step 1: 清洗 (Transform - Clean)
        cleaned = self._clean_kline(raw_data)
        
        # Step 2: 除权除息处理 (Adjust)
        adjusted = self._apply_adjustment(cleaned, code)
        
        # Step 3: 标准化 (Normalize)
        normalized = self._normalize_kline(adjusted)
        
        # Step 4: 质量标记 (Quality Tag)
        quality_tagged = self._tag_quality(normalized)
        
        return quality_tagged
    
    def _clean_kline(self, df: pl.DataFrame) -> pl.DataFrame:
        """清洗K线数据"""
        # 检查数据是否为空
        if df.is_empty():
            return df
        
        # 检查必需的列是否存在
        required_cols = ['close', 'open', 'high', 'low', 'volume']
        if not all(col in df.columns for col in required_cols):
            self.logger.warning(f"缺少必需的列，跳过清洗: {df.columns}")
            return df
        
        # 去除异常值
        df = df.filter(
            (pl.col('close') > 0) &
            (pl.col('open') > 0) &
            (pl.col('high') > 0) &
            (pl.col('low') > 0) &
            (pl.col('volume') >= 0)
        )
        
        # 去除涨跌幅异常（>±20%可能是除权除息未处理）
        if 'pct_change' in df.columns:
            df = df.filter(pl.col('pct_change').abs() <= 20)
        
        # 去除重复数据（如果有trade_date列）
        if 'trade_date' in df.columns:
            df = df.unique(subset=['trade_date'])
            # 按日期排序
            df = df.sort('trade_date')
        
        return df
    
    def _apply_adjustment(self, df: pl.DataFrame, code: str) -> pl.DataFrame:
        """
        除权除息处理
        
        使用前复权因子调整历史价格
        """
        # 获取复权因子
        adj_factor = self._get_adjustment_factor(code)
        
        if adj_factor is not None and len(adj_factor) > 0:
            # 合并复权因子
            df = df.join(adj_factor, on='trade_date', how='left')
            
            # 应用复权
            if 'adj_factor' in df.columns:
                df = df.with_columns([
                    (pl.col('open') * pl.col('adj_factor')).alias('open_adj'),
                    (pl.col('close') * pl.col('adj_factor')).alias('close_adj'),
                    (pl.col('high') * pl.col('adj_factor')).alias('high_adj'),
                    (pl.col('low') * pl.col('adj_factor')).alias('low_adj'),
                ])
        
        return df
    
    def _get_adjustment_factor(self, code: str) -> Optional[pl.DataFrame]:
        """获取复权因子"""
        fetcher = get_adj_factor_fetcher()
        return fetcher.fetch_adj_factor(code)
    
    def _normalize_kline(self, df: pl.DataFrame) -> pl.DataFrame:
        """标准化K线数据"""
        # 统一列名
        column_mapping = {
            'ts_code': 'code',
            'trade_date': 'date',
            'vol': 'volume',
            'amount': 'turnover'
        }
        
        for old, new in column_mapping.items():
            if old in df.columns:
                df = df.rename({old: new})
        
        # 添加计算字段
        if 'close' in df.columns and 'open' in df.columns:
            df = df.with_columns([
                ((pl.col('close') - pl.col('open')) / pl.col('open') * 100).alias('pct_change')
            ])
        
        return df
    
    def _tag_quality(self, df: pl.DataFrame) -> pl.DataFrame:
        """质量标记"""
        # 标记数据质量等级
        df = df.with_columns([
            pl.lit('A').alias('quality_grade')  # A: 优秀, B: 良好, C: 可疑
        ])
        
        # 标记缺失值
        df = df.with_columns([
            pl.col('volume').is_null().alias('has_missing_volume')
        ])
        
        return df


# ============================================================================
# 3. 数据质量检查模块 (在ETL之后)
# ============================================================================

class DataQualityChecker:
    """数据质量检查器"""
    
    def __init__(self):
        self.logger = setup_logger("quality_checker")
        self.quality_rules = self._init_quality_rules()
    
    def _init_quality_rules(self) -> List[Dict]:
        """初始化质量规则"""
        return [
            {
                'name': 'completeness',
                'check': self._check_completeness,
                'threshold': 0.95
            },
            {
                'name': 'consistency',
                'check': self._check_consistency,
                'threshold': 1.0
            },
            {
                'name': 'timeliness',
                'check': self._check_timeliness,
                'threshold': 1.0
            }
        ]
    
    def _check_completeness(self, df: pl.DataFrame) -> bool:
        """检查完整性"""
        if df.is_empty():
            return True
        null_counts = df.null_count()
        total_nulls = sum(null_counts[col].item() for col in null_counts.columns)
        total_cells = len(df) * len(df.columns)
        return total_nulls / total_cells < 0.05 if total_cells > 0 else True
    
    def _check_consistency(self, df: pl.DataFrame) -> bool:
        """检查一致性"""
        if 'high' in df.columns and 'low' in df.columns:
            return df.filter(pl.col('high') < pl.col('low')).is_empty()
        return True
    
    def _check_timeliness(self, df: pl.DataFrame) -> bool:
        """检查时效性"""
        return len(df) > 0
    
    def check_quality(self, df: pl.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        检查数据质量
        
        Args:
            df: 已清洗的数据
            data_type: 数据类型
        
        Returns:
            质量报告
        """
        report = {
            'data_type': data_type,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'checks': {},
            'overall_score': 0.0,
            'passed': True
        }
        
        total_score = 0
        
        for rule in self.quality_rules:
            try:
                passed = rule['check'](df)
                score = rule['threshold'] if passed else 0
                
                report['checks'][rule['name']] = {
                    'passed': passed,
                    'score': score,
                    'threshold': rule['threshold']
                }
                
                total_score += score
                
                if not passed and rule['threshold'] == 1.0:
                    report['passed'] = False
                    
            except Exception as e:
                self.logger.error(f"规则 {rule['name']} 检查失败: {e}")
                report['checks'][rule['name']] = {'passed': False, 'error': str(e)}
        
        report['overall_score'] = total_score / len(self.quality_rules) if self.quality_rules else 0
        
        return report


# ============================================================================
# 4. 回测引擎模块 (填补战略缺口)
# ============================================================================

@dataclass
class BacktestResult:
    """回测结果"""
    strategy_name: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    ic_values: List[float] = field(default_factory=list)
    trades: List[Dict] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'strategy_name': self.strategy_name,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'initial_capital': self.initial_capital,
            'final_capital': self.final_capital,
            'total_return': f"{self.total_return:.2%}",
            'annual_return': f"{self.annual_return:.2%}",
            'max_drawdown': f"{self.max_drawdown:.2%}",
            'sharpe_ratio': f"{self.sharpe_ratio:.2f}",
            'win_rate': f"{self.win_rate:.2%}",
            'ic_mean': f"{np.mean(self.ic_values):.4f}" if self.ic_values else "N/A",
            'ic_ir': f"{np.mean(self.ic_values) / np.std(self.ic_values):.4f}" if self.ic_values and np.std(self.ic_values) > 0 else "N/A",
            'trade_count': len(self.trades)
        }


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital: float = 1000000.0):
        self.logger = setup_logger("backtest_engine")
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions = {}
        self.trades = []
        self.daily_returns = []
    
    def run_backtest(
        self,
        strategy: 'FactorStrategy',
        start_date: str,
        end_date: str,
        universe: List[str]
    ) -> BacktestResult:
        """
        运行回测
        
        Args:
            strategy: 因子策略
            start_date: 开始日期
            end_date: 结束日期
            universe: 股票池
        
        Returns:
            回测结果
        """
        self.logger.info(f"开始回测: {strategy.name} ({start_date} ~ {end_date})")
        
        # 获取历史数据
        historical_data = self._load_historical_data(universe, start_date, end_date)
        
        # 计算因子值
        factor_values = strategy.calculate_factors(historical_data)
        
        # 计算IC值（信息系数）
        ic_values = self._calculate_ic(factor_values, historical_data)
        
        # 模拟交易
        dates = pd.date_range(start=start_date, end=end_date, freq='B')
        
        for current_date in dates:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 获取当日信号
            signals = strategy.generate_signals(factor_values, date_str)
            
            # 执行交易
            self._execute_trades(signals, historical_data, date_str)
            
            # 记录收益
            daily_pnl = self._calculate_daily_pnl(historical_data, date_str)
            self.daily_returns.append(daily_pnl / self.capital if self.capital > 0 else 0)
        
        # 计算回测指标
        result = self._calculate_metrics(
            strategy.name, start_date, end_date, ic_values
        )
        
        self.logger.info(f"回测完成: 总收益 {result.total_return:.2%}, 最大回撤 {result.max_drawdown:.2%}")
        
        return result
    
    def _calculate_ic(self, factor_values: pd.DataFrame, prices: pd.DataFrame) -> List[float]:
        """计算信息系数 (IC)"""
        ic_values = []
        
        # 按日期计算因子值与未来收益的相关性
        for date in factor_values.index.unique():
            if date in prices.index:
                factor_t = factor_values.loc[date]
                # 未来5日收益
                future_return = prices.loc[date:].iloc[5] / prices.loc[date] - 1
                
                # 计算相关系数
                if len(factor_t) == len(future_return):
                    ic = np.corrcoef(factor_t, future_return)[0, 1]
                    if not np.isnan(ic):
                        ic_values.append(ic)
        
        return ic_values
    
    def _calculate_metrics(
        self,
        strategy_name: str,
        start_date: str,
        end_date: str,
        ic_values: List[float]
    ) -> BacktestResult:
        """计算回测指标"""
        total_return = (self.capital - self.initial_capital) / self.initial_capital
        
        # 年化收益
        days = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days
        annual_return = (1 + total_return) ** (365 / days) - 1 if days > 0 else 0
        
        # 最大回撤
        cumulative = np.cumprod(1 + np.array(self.daily_returns))
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0
        
        # 夏普比率
        returns_array = np.array(self.daily_returns)
        sharpe = np.mean(returns_array) / np.std(returns_array) * np.sqrt(252) if np.std(returns_array) > 0 else 0
        
        # 胜率
        win_trades = [t for t in self.trades if t.get('pnl', 0) > 0]
        win_rate = len(win_trades) / len(self.trades) if self.trades else 0
        
        return BacktestResult(
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            initial_capital=self.initial_capital,
            final_capital=self.capital,
            total_return=total_return,
            annual_return=annual_return,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe,
            win_rate=win_rate,
            ic_values=ic_values,
            trades=self.trades
        )
    
    def _load_historical_data(self, universe: List[str], start: str, end: str) -> pd.DataFrame:
        """加载历史数据"""
        loader = get_historical_loader()
        results = loader.load_batch(universe, start, end)
        
        # 合并为DataFrame
        all_data = []
        for code, df in results.items():
            if df is not None:
                df_pd = df.to_pandas()
                df_pd['code'] = code
                all_data.append(df_pd)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    def _execute_trades(self, signals: Dict, prices: pd.DataFrame, date: str):
        """执行交易"""
        # TODO: 实现交易执行逻辑
        pass
    
    def _calculate_daily_pnl(self, prices: pd.DataFrame, date: str) -> float:
        """计算日收益"""
        # TODO: 实现日收益计算
        return 0.0


class FactorStrategy(ABC):
    """因子策略基类"""
    
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def calculate_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算因子值"""
        pass
    
    @abstractmethod
    def generate_signals(self, factors: pd.DataFrame, date: str) -> Dict:
        """生成交易信号"""
        pass


# ============================================================================
# 5. 风险控制模块 (填补安全缺口)
# ============================================================================

@dataclass
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    rule_name: str
    message: str
    severity: str  # 'error', 'warning', 'info'
    action: str  # 'block', 'alert', 'log'


class RiskManager:
    """风险管理器"""
    
    def __init__(self):
        self.logger = setup_logger("risk_manager")
        self.rules = self._init_risk_rules()
        self.daily_stats = {
            'total_trades': 0,
            'total_value': 0.0,
            'sector_exposure': {}
        }
    
    def _init_risk_rules(self) -> List[Dict]:
        """初始化风控规则"""
        return [
            {
                'name': 'single_stock_limit',
                'description': '单股持仓上限10%',
                'check': self._check_single_stock_limit,
                'severity': 'error',
                'action': 'block'
            },
            {
                'name': 'sector_concentration',
                'description': '行业集中度上限30%',
                'check': self._check_sector_concentration,
                'severity': 'error',
                'action': 'block'
            },
            {
                'name': 'daily_drawdown',
                'description': '单日回撤限制5%',
                'check': self._check_daily_drawdown,
                'severity': 'error',
                'action': 'block'
            },
            {
                'name': 'total_drawdown',
                'description': '总回撤限制20%',
                'check': self._check_total_drawdown,
                'severity': 'error',
                'action': 'block'
            },
            {
                'name': 'liquidity_check',
                'description': '流动性检查（成交额>1000万）',
                'check': self._check_liquidity,
                'severity': 'warning',
                'action': 'alert'
            },
            {
                'name': 'volatility_check',
                'description': '波动率检查（ATR<5%）',
                'check': self._check_volatility,
                'severity': 'warning',
                'action': 'alert'
            }
        ]
    
    def check_order(self, order: Dict, portfolio: Dict) -> List[RiskCheckResult]:
        """
        检查订单风险
        
        Args:
            order: 订单信息 {'code': '000001', 'action': 'buy', 'quantity': 100, 'price': 10.0}
            portfolio: 当前持仓
        
        Returns:
            风控检查结果列表
        """
        results = []
        
        for rule in self.rules:
            try:
                passed, message = rule['check'](order, portfolio)
                
                results.append(RiskCheckResult(
                    passed=passed,
                    rule_name=rule['name'],
                    message=message,
                    severity=rule['severity'],
                    action=rule['action']
                ))
                
                if not passed and rule['action'] == 'block':
                    self.logger.error(f"风控拦截: {rule['name']} - {message}")
                elif not passed:
                    self.logger.warning(f"风控警告: {rule['name']} - {message}")
                    
            except Exception as e:
                self.logger.error(f"风控规则 {rule['name']} 检查失败: {e}")
        
        return results
    
    def can_execute(self, order: Dict, portfolio: Dict) -> Tuple[bool, List[str]]:
        """
        判断订单是否可执行
        
        Returns:
            (是否可执行, 拦截原因列表)
        """
        results = self.check_order(order, portfolio)
        
        block_reasons = [
            r.message for r in results
            if not r.passed and r.action == 'block'
        ]
        
        return len(block_reasons) == 0, block_reasons
    
    def _check_single_stock_limit(self, order: Dict, portfolio: Dict) -> Tuple[bool, str]:
        """检查单股持仓上限"""
        code = order.get('code')
        quantity = order.get('quantity', 0)
        price = order.get('price', 0)
        
        # 计算订单金额
        order_value = quantity * price
        
        # 计算总资金（简化处理）
        total_capital = portfolio.get('total_capital', 1000000)
        
        # 计算持仓后比例
        current_position = portfolio.get('positions', {}).get(code, {}).get('value', 0)
        new_ratio = (current_position + order_value) / total_capital
        
        if new_ratio > 0.10:  # 10%上限
            return False, f"单股持仓将超限: {new_ratio:.2%} > 10%"
        
        return True, "通过"
    
    def _check_sector_concentration(self, order: Dict, portfolio: Dict) -> Tuple[bool, str]:
        """检查行业集中度"""
        # TODO: 实现行业集中度检查
        return True, "通过"
    
    def _check_daily_drawdown(self, order: Dict, portfolio: Dict) -> Tuple[bool, str]:
        """检查单日回撤"""
        daily_pnl = portfolio.get('daily_pnl', 0)
        total_capital = portfolio.get('total_capital', 1000000)
        
        drawdown = abs(daily_pnl) / total_capital
        
        if drawdown > 0.05:  # 5%限制
            return False, f"单日回撤超限: {drawdown:.2%} > 5%"
        
        return True, "通过"
    
    def _check_total_drawdown(self, order: Dict, portfolio: Dict) -> Tuple[bool, str]:
        """检查总回撤"""
        peak_capital = portfolio.get('peak_capital', portfolio.get('total_capital', 1000000))
        current_capital = portfolio.get('total_capital', 1000000)
        
        drawdown = (peak_capital - current_capital) / peak_capital
        
        if drawdown > 0.20:  # 20%限制
            return False, f"总回撤超限: {drawdown:.2%} > 20%"
        
        return True, "通过"
    
    def _check_liquidity(self, order: Dict, portfolio: Dict) -> Tuple[bool, str]:
        """检查流动性"""
        # TODO: 实现流动性检查
        return True, "通过"
    
    def _check_volatility(self, order: Dict, portfolio: Dict) -> Tuple[bool, str]:
        """检查波动率"""
        # TODO: 实现波动率检查
        return True, "通过"


# ============================================================================
# 6. 极速盘前系统 (9:26优化)
# ============================================================================

class FastPreMarketSystem:
    """极速盘前系统 - 60秒内完成"""
    
    def __init__(self):
        self.logger = setup_logger("fast_premarket")
        self.max_execution_time = 60  # 最大执行时间60秒
        self.opening_predictor = create_opening_predictor()
    
    def generate_limit_up_report(self, date: str) -> Dict[str, Any]:
        """
        生成涨停板报告（极速版）
        
        优化策略：
        1. 只加载核心数据（行情、竞价）
        2. 跳过非关键检查
        3. 异步并行处理
        """
        start_time = time.time()
        
        self.logger.info(f"🚀 启动极速盘前系统: {date}")
        
        # Step 1: 极速采集（只采集核心数据）
        core_data = self._fast_collect_core_data(date)
        
        # Step 2: 快速分析涨停股票
        limit_up_stocks = self._analyze_limit_up_fast(core_data)
        
        # Step 3: 预测开板概率
        predictions = self._predict_open_probability(limit_up_stocks)
        
        # Step 4: 生成推荐
        recommendations = self._generate_recommendations_fast(predictions)
        
        execution_time = time.time() - start_time
        
        report = {
            'date': date,
            'time': '09:26',
            'execution_time_seconds': execution_time,
            'limit_up_count': len(limit_up_stocks),
            'limit_up_stocks': limit_up_stocks,
            'predictions': predictions,
            'recommendations': recommendations,
            'status': 'success' if execution_time < self.max_execution_time else 'timeout'
        }
        
        self.logger.info(f"✅ 盘前报告生成完成: {execution_time:.2f}秒")
        
        return report
    
    def _fast_collect_core_data(self, date: str) -> Dict:
        """极速采集核心数据"""
        # 只采集竞价数据，跳过其他所有数据源
        # TODO: 实现极速采集
        return {}
    
    def _analyze_limit_up_fast(self, data: Dict) -> List[Dict]:
        """快速分析涨停股票"""
        # TODO: 实现快速分析
        return []
    
    def _predict_open_probability(self, stocks: List[Dict]) -> List[Dict]:
        """预测开板概率"""
        predictions = []
        
        for stock in stocks:
            code = stock.get('code')
            
            # 构建市场数据
            market_data = {
                'name': stock.get('name', ''),
                'price': stock.get('price', 0),
                'limit_up_price': stock.get('limit_up_price', 0),
                'seal_amount': stock.get('seal_amount', 0),
                'market_cap': stock.get('market_cap', 1),
                'turnover_rate': stock.get('turnover_rate', 0),
                'sector_heat': stock.get('sector_heat', 0.5),
                'market_sentiment': stock.get('market_sentiment', 0.5),
                'limit_up_count': stock.get('limit_up_count', 1),
                'volume_ratio': stock.get('volume_ratio', 1.0)
            }
            
            # 使用开板预测模型
            prediction = self.opening_predictor.predict(code, market_data)
            
            if prediction:
                predictions.append(prediction.to_dict())
        
        return predictions
    
    def _generate_recommendations_fast(self, predictions: List[Dict]) -> List[Dict]:
        """快速生成推荐"""
        # TODO: 实现快速推荐
        return []


# ============================================================================
# 7. 订单执行算法 (VWAP/TWAP)
# ============================================================================

class ExecutionAlgorithm(ABC):
    """执行算法基类"""
    
    @abstractmethod
    def execute(self, order: Dict, market_data: pd.DataFrame) -> List[Dict]:
        """执行订单，返回子订单列表"""
        pass


class VWAPAlgorithm(ExecutionAlgorithm):
    """VWAP执行算法"""
    
    def __init__(self, num_slices: int = 10):
        self.num_slices = num_slices
        self.logger = setup_logger("vwap_algo")
    
    def execute(self, order: Dict, market_data: pd.DataFrame) -> List[Dict]:
        """
        VWAP拆分执行
        
        将大单拆分为多个小单，按成交量分布执行
        """
        total_quantity = order['quantity']
        code = order['code']
        
        # 计算成交量分布
        volume_profile = self._calculate_volume_profile(market_data)
        
        # 生成子订单
        child_orders = []
        for i in range(self.num_slices):
            slice_quantity = int(total_quantity * volume_profile[i])
            
            child_order = {
                'code': code,
                'quantity': slice_quantity,
                'slice_id': i + 1,
                'total_slices': self.num_slices,
                'algo': 'VWAP',
                'timestamp': datetime.now().isoformat()
            }
            
            child_orders.append(child_order)
        
        self.logger.info(f"VWAP拆分: {total_quantity}股 → {self.num_slices} 个子订单")
        
        return child_orders
    
    def _calculate_volume_profile(self, market_data: pd.DataFrame) -> List[float]:
        """计算成交量分布"""
        # 简化处理：均匀分布
        return [1.0 / self.num_slices] * self.num_slices


class TWAPAlgorithm(ExecutionAlgorithm):
    """TWAP执行算法"""
    
    def __init__(self, duration_minutes: int = 30, interval_seconds: int = 60):
        self.duration_minutes = duration_minutes
        self.interval_seconds = interval_seconds
        self.logger = setup_logger("twap_algo")
    
    def execute(self, order: Dict, market_data: pd.DataFrame) -> List[Dict]:
        """
        TWAP时间加权执行
        
        将大单拆分为多个小单，按时间均匀执行
        """
        total_quantity = order['quantity']
        code = order['code']
        
        num_slices = (self.duration_minutes * 60) // self.interval_seconds
        slice_quantity = total_quantity // num_slices
        
        child_orders = []
        for i in range(num_slices):
            child_order = {
                'code': code,
                'quantity': slice_quantity if i < num_slices - 1 else total_quantity - slice_quantity * (num_slices - 1),
                'slice_id': i + 1,
                'total_slices': num_slices,
                'algo': 'TWAP',
                'execute_time': (datetime.now() + timedelta(seconds=i * self.interval_seconds)).isoformat()
            }
            
            child_orders.append(child_order)
        
        self.logger.info(f"TWAP拆分: {total_quantity}股 → {num_slices} 个子订单")
        
        return child_orders


# ============================================================================
# 8. 动态头寸管理
# ============================================================================

class PositionManager:
    """动态头寸管理器"""
    
    def __init__(self):
        self.logger = setup_logger("position_manager")
        self.positions = {}
        self.target_weights = {}
        self.current_weights = {}
    
    def update_weights(self, selection_scores: Dict[str, float], market_data: Dict):
        """
        动态更新权重
        
        根据盘中波动率实时调整选股评分权重
        """
        # 计算市场波动率
        market_volatility = self._calculate_market_volatility(market_data)
        
        # 根据波动率调整权重
        if market_volatility > 0.03:  # 高波动
            # 降低风险敞口
            adjustment_factor = 0.7
            self.logger.info(f"高波动环境，降低仓位至 {adjustment_factor:.0%}")
        elif market_volatility < 0.01:  # 低波动
            # 增加风险敞口
            adjustment_factor = 1.2
            self.logger.info(f"低波动环境，增加仓位至 {adjustment_factor:.0%}")
        else:
            adjustment_factor = 1.0
        
        # 应用调整
        for code, score in selection_scores.items():
            self.target_weights[code] = score * adjustment_factor
        
        # 归一化
        total_weight = sum(self.target_weights.values())
        if total_weight > 0:
            self.target_weights = {
                k: v / total_weight for k, v in self.target_weights.items()
            }
    
    def _calculate_market_volatility(self, market_data: Dict) -> float:
        """计算市场波动率"""
        # TODO: 实现波动率计算
        return 0.02
    
    def generate_rebalance_orders(self) -> List[Dict]:
        """生成再平衡订单"""
        orders = []
        
        for code, target_weight in self.target_weights.items():
            current_weight = self.current_weights.get(code, 0)
            diff = target_weight - current_weight
            
            if abs(diff) > 0.01:  # 差异超过1%才调整
                orders.append({
                    'code': code,
                    'action': 'buy' if diff > 0 else 'sell',
                    'target_weight': target_weight,
                    'current_weight': current_weight,
                    'diff': diff
                })
        
        return orders


# ============================================================================
# 9. 量化交易系统 V2 (主类)
# ============================================================================

class QuantTradingSystemV2(WorkflowExecutor):
    """
    量化交易系统 V2 - 专业架构
    
    双环架构：
    - 离线环：数据采集 → ETL → 回测 → 策略优化
    - 在线环：实时行情 → 风控 → 执行 → 复盘
    """
    
    def __init__(self):
        super().__init__(
            workflow_name="quant_trading_v2",
            retry_config=RetryConfig(max_retries=3, retry_delay=1.0),
            enable_checkpoint=True,
            enable_auto_fix=True
        )
        
        # 初始化各模块
        self.collector = PriorityDataCollector()
        self.etl = ETLProcessor()
        self.quality_checker = DataQualityChecker()
        self.backtest_engine = BacktestEngine()
        self.risk_manager = RiskManager()
        self.pre_market = FastPreMarketSystem()
        self.position_manager = PositionManager()
        
        # 初始化新模块
        self.adj_factor_fetcher = get_adj_factor_fetcher()
        self.historical_loader = get_historical_loader()
        self.opening_predictor = create_opening_predictor()
        self.attribution_analyzer = create_attribution_analyzer()
        self.realtime_stream: Optional[RealtimeMarketDataStream] = None
        
        self.logger = setup_logger("quant_trading_v2")
    
    # ==================== 离线环 ====================
    
    def run_offline_loop(self, date: str) -> Dict[str, Any]:
        """
        执行离线环
        
        流程: 数据采集 → ETL清洗 → 质量检查 → 回测 → 策略优化
        """
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"🔄 启动离线环: {date}")
        self.logger.info(f"{'='*60}\n")
        
        start_time = time.time()
        
        # Step 1: 优先级数据采集
        self.logger.info("📦 Step 1: 优先级数据采集")
        raw_data = self._collect_data_priority(date)
        
        # Step 2: ETL清洗
        self.logger.info("🧹 Step 2: ETL数据清洗")
        cleaned_data = self._run_etl_pipeline(raw_data, date)
        
        # Step 3: 质量检查（在ETL之后）
        self.logger.info("🔍 Step 3: 数据质量检查")
        quality_report = self._check_data_quality(cleaned_data)
        
        # Step 4: 回测验证
        self.logger.info("📊 Step 4: 策略回测")
        backtest_result = self._run_backtest(date)
        
        # Step 5: 策略优化
        self.logger.info("⚙️  Step 5: 策略优化")
        optimization = self._optimize_strategy(backtest_result)
        
        duration = time.time() - start_time
        
        report = {
            'date': date,
            'loop_type': 'offline',
            'duration_seconds': duration,
            'data_collection': raw_data,
            'etl_status': 'completed',
            'quality_report': quality_report,
            'backtest_result': backtest_result.to_dict() if backtest_result else None,
            'optimization': optimization
        }
        
        self.logger.info(f"\n✅ 离线环完成: {duration:.2f}秒")
        
        return report
    
    def _collect_data_priority(self, date: str) -> Dict[str, Any]:
        """按优先级采集数据"""
        from services.data_service.unified_data_service import UnifiedDataService
        
        data_service = UnifiedDataService()
        
        # 添加高优先级任务（核心行情）
        self.collector.add_task(DataTask(
            priority=DataPriority.CRITICAL.value,
            task_id="stock_list",
            source_type="stock_list",
            execute_func=data_service.get_stock_list_sync
        ))
        
        # 添加中优先级任务
        self.collector.add_task(DataTask(
            priority=DataPriority.MEDIUM.value,
            task_id="market_index",
            source_type="market_index",
            execute_func=lambda: data_service.get_kline('000001', date.replace('-', ''), date.replace('-', ''))
        ))
        
        # 执行采集
        results = asyncio.run(self.collector.execute_all())
        
        return results
    
    def _run_etl_pipeline(self, raw_data: Dict, date: str) -> Dict[str, pl.DataFrame]:
        """执行ETL流程"""
        cleaned = {}
        
        for task_id, result in raw_data.items():
            if result.get('status') == 'success':
                data = result.get('data')
                
                if isinstance(data, pd.DataFrame):
                    data = pl.from_pandas(data)
                
                if isinstance(data, pl.DataFrame):
                    # 根据数据类型选择处理方式
                    if task_id == 'stock_list':
                        # 股票列表数据，简单标准化
                        cleaned[task_id] = self.etl._normalize_kline(data)
                    elif 'close' in data.columns:
                        # K线数据，完整ETL处理
                        cleaned[task_id] = self.etl.process_kline_etl(data, task_id)
                    else:
                        # 其他数据，简单处理
                        cleaned[task_id] = data
                else:
                    cleaned[task_id] = data
        
        return cleaned
    
    def _check_data_quality(self, cleaned_data: Dict) -> Dict[str, Any]:
        """检查清洗后的数据质量"""
        reports = {}
        
        for data_type, df in cleaned_data.items():
            if isinstance(df, pl.DataFrame):
                report = self.quality_checker.check_quality(df, data_type)
                reports[data_type] = report
        
        return reports
    
    def _run_backtest(self, date: str) -> Optional[BacktestResult]:
        """运行回测"""
        # TODO: 实现回测执行
        return None
    
    def _optimize_strategy(self, backtest_result: Optional[BacktestResult]) -> Dict:
        """优化策略"""
        # TODO: 实现策略优化
        return {}
    
    # ==================== 在线环 ====================
    
    def run_online_loop(self, date: str) -> Dict[str, Any]:
        """
        执行在线环
        
        流程: 实时行情 → 风控校验 → 订单执行 → 自动复盘
        """
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"🔄 启动在线环: {date}")
        self.logger.info(f"{'='*60}\n")
        
        start_time = time.time()
        
        # Step 1: 获取实时行情
        self.logger.info("📈 Step 1: 获取实时行情")
        market_data = self._get_realtime_data()
        
        # Step 2: 动态头寸调整
        self.logger.info("⚖️  Step 2: 动态头寸调整")
        self.position_manager.update_weights({}, market_data)
        
        # Step 3: 风控校验
        self.logger.info("🛡️  Step 3: 风控校验")
        risk_check = self._run_risk_check()
        
        # Step 4: 订单执行
        self.logger.info("📤 Step 4: 订单执行")
        execution = self._execute_orders()
        
        # Step 5: 自动复盘
        self.logger.info("📊 Step 5: 自动复盘")
        review = self._auto_review()
        
        duration = time.time() - start_time
        
        report = {
            'date': date,
            'loop_type': 'online',
            'duration_seconds': duration,
            'market_data': market_data,
            'risk_check': risk_check,
            'execution': execution,
            'review': review
        }
        
        self.logger.info(f"\n✅ 在线环完成: {duration:.2f}秒")
        
        return report
    
    def _get_realtime_data(self) -> Dict:
        """获取实时行情"""
        # 创建实时行情流
        stream = create_realtime_stream()
        
        # 返回流对象和缓存数据
        return {
            'stream': stream,
            'quote_cache': stream.quote_cache if hasattr(stream, 'quote_cache') else {}
        }
    
    def _run_risk_check(self) -> Dict:
        """运行风控检查"""
        # TODO: 实现风控检查
        return {}
    
    def _execute_orders(self) -> Dict:
        """执行订单"""
        # TODO: 实现订单执行
        return {}
    
    def _auto_review(self) -> Dict:
        """自动复盘"""
        # 创建归因分析器
        analyzer = create_attribution_analyzer()
        
        # 模拟交易数据（实际应从交易记录加载）
        trades = self._load_trades_for_review()
        
        if trades:
            # 执行归因分析
            report = analyzer.analyze_trades(trades)
            
            # 保存报告
            analyzer.save_report(report)
            
            # 生成摘要
            summary = analyzer.generate_summary_text(report)
            self.logger.info(f"\n{summary}")
            
            return report.to_dict()
        
        return {'status': 'no_trades', 'message': '无交易记录'}
    
    def _load_trades_for_review(self) -> List[Dict]:
        """加载交易记录用于复盘"""
        # TODO: 从数据库或文件加载实际交易记录
        # 这里返回空列表，实际应实现加载逻辑
        return []
    
    # ==================== 极速盘前 ====================
    
    def run_pre_market_fast(self, date: str) -> Dict[str, Any]:
        """
        极速盘前系统（60秒内完成）
        """
        return self.pre_market.generate_limit_up_report(date)
    
    # ==================== 抽象方法实现 ====================
    
    def check_dependencies(self) -> List[DependencyCheck]:
        """检查依赖"""
        checks = []
        
        # 检查数据源
        try:
            from services.data_service.unified_data_service import UnifiedDataService
            service = UnifiedDataService()
            stock_list = service.get_stock_list_sync()
            checks.append(DependencyCheck(
                name="数据源连接",
                status=DependencyStatus.HEALTHY,
                message=f"股票列表: {len(stock_list)}只"
            ))
        except Exception as e:
            checks.append(DependencyCheck(
                name="数据源连接",
                status=DependencyStatus.UNHEALTHY,
                message=str(e)
            ))
        
        return checks
    
    def auto_fix_dependency(self, dependency: DependencyCheck) -> bool:
        """自动修复依赖"""
        return False
    
    def execute(self, date: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """执行完整流程"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        mode = kwargs.get('mode', 'full')
        
        if mode == 'offline':
            return self.run_offline_loop(date)
        elif mode == 'online':
            return self.run_online_loop(date)
        elif mode == 'pre_market':
            return self.run_pre_market_fast(date)
        else:  # full
            offline_report = self.run_offline_loop(date)
            online_report = self.run_online_loop(date)
            
            return {
                'date': date,
                'mode': 'full',
                'offline': offline_report,
                'online': online_report
            }


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='量化交易系统 V2')
    parser.add_argument('--date', help='日期 (YYYY-MM-DD)')
    parser.add_argument('--mode', choices=['full', 'offline', 'online', 'pre_market'],
                       default='full', help='运行模式')
    
    args = parser.parse_args()
    
    system = QuantTradingSystemV2()
    
    result = system.execute(date=args.date, mode=args.mode)
    
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
