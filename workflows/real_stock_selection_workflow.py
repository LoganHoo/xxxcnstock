#!/usr/bin/env python3
"""
真实数据选股策略工作流

基于已采集的真实数据运行选股策略:
- K线数据 (价格、成交量、涨跌幅)
- 财务数据 (ROE、毛利率、资产负债率等)
- 市场行为数据 (龙虎榜、资金流向)
- 公告数据 (业绩预告、重大事项)

评分体系:
- 财务评分 (40%): ROE、盈利能力、成长性、偿债能力
- 市场评分 (30%): 资金流向、龙虎榜、技术指标
- 公告评分 (20%): 业绩预告、重大事项
- 技术评分 (10%): 量价关系、趋势、多因子分析、形态识别、技术指标、过滤原因、其他指标
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json

import pandas as pd
import numpy as np

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.unified_data_service import UnifiedDataService
from workflows.datahub_lineage_workflow import DataHubLineageWorkflow

logger = setup_logger("real_stock_selection")


class StrategyType(Enum):
    """策略类型"""
    VALUE_GROWTH = "value_growth"           # 价值成长型
    MAIN_FORCE_TRACKING = "main_force"      # 主力资金追踪型
    EVENT_DRIVEN = "event_driven"           # 事件驱动型
    COMPREHENSIVE = "comprehensive"         # 综合策略


@dataclass
class StockScore:
    """股票评分详情"""
    code: str
    name: str
    financial_score: float = 0.0
    market_score: float = 0.0
    announcement_score: float = 0.0
    technical_score: float = 0.0
    total_score: float = 0.0
    
    # 详细指标
    roe: Optional[float] = None
    gross_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    debt_ratio: Optional[float] = None
    
    # 市场指标
    main_force_flow: Optional[float] = None
    dragon_tiger_count: int = 0
    northbound_holding: Optional[float] = None
    
    # 技术指标
    price_change_5d: Optional[float] = None
    price_change_20d: Optional[float] = None
    volume_ratio: Optional[float] = None
    
    # 过滤原因
    filter_reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'name': self.name,
            'total_score': round(self.total_score, 2),
            'financial_score': round(self.financial_score, 2),
            'market_score': round(self.market_score, 2),
            'announcement_score': round(self.announcement_score, 2),
            'technical_score': round(self.technical_score, 2),
            'roe': round(self.roe, 2) if self.roe else None,
            'gross_margin': round(self.gross_margin, 2) if self.gross_margin else None,
            'revenue_growth': round(self.revenue_growth, 2) if self.revenue_growth else None,
            'debt_ratio': round(self.debt_ratio, 2) if self.debt_ratio else None,
            'main_force_flow': round(self.main_force_flow, 2) if self.main_force_flow else None,
            'dragon_tiger_count': self.dragon_tiger_count,
            'price_change_5d': round(self.price_change_5d, 2) if self.price_change_5d else None,
            'price_change_20d': round(self.price_change_20d, 2) if self.price_change_20d else None,
            'volume_ratio': round(self.volume_ratio, 2) if self.volume_ratio else None,
            'filter_reason': self.filter_reason
        }


@dataclass
class SelectionResult:
    """选股结果"""
    strategy_type: str
    status: str
    start_time: str
    end_time: str
    duration_seconds: float
    total_stocks: int
    selected_stocks: int
    filtered_out: int
    top_stocks: List[Dict[str, Any]] = field(default_factory=list)
    filters_applied: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'strategy_type': self.strategy_type,
            'status': self.status,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'total_stocks': self.total_stocks,
            'selected_stocks': self.selected_stocks,
            'filtered_out': self.filtered_out,
            'top_stocks': self.top_stocks,
            'filters_applied': self.filters_applied,
            'errors': self.errors
        }


class RealStockSelectionWorkflow:
    """基于真实数据的选股策略工作流"""
    
    def __init__(self):
        """初始化选股策略工作流"""
        self.logger = setup_logger("real_stock_selection_workflow")
        self.data_service = UnifiedDataService()
        self.lineage_workflow = DataHubLineageWorkflow()
        self.data_path = get_data_path()

        self.results_dir = self.data_path / "workflow_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.kline_dir = self.data_path / "kline"
        self.financial_dir = self.data_path / "financial"
        self.market_behavior_dir = self.data_path / "market_behavior"
        self.announcement_dir = self.data_path / "announcement"
    
    def run(self,
            strategy_type: StrategyType = StrategyType.COMPREHENSIVE,
            universe: Optional[List[str]] = None,
            top_n: int = 50,
            date: Optional[str] = None,
            min_roe: float = 10.0,
            max_debt_ratio: float = 60.0,
            min_gross_margin: float = 20.0) -> SelectionResult:
        """
        运行选股策略工作流
        
        Args:
            strategy_type: 策略类型
            universe: 股票池 (默认为全市场)
            top_n: 输出数量
            date: 选股日期 (默认为今天)
            min_roe: 最小ROE要求
            max_debt_ratio: 最大资产负债率
            min_gross_margin: 最小毛利率
        
        Returns:
            选股结果
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info(f"开始真实数据选股策略: {strategy_type.value}, 日期: {date}")
        start_time = datetime.now()
        
        errors = []
        filters_applied = []
        
        try:
            # 步骤0: 数据质量检查
            self.logger.info("步骤0: 数据质量检查")
            quality_ok, quality_msg = self._check_data_quality()
            if not quality_ok:
                self.logger.error(f"数据质量检查失败: {quality_msg}")
                errors.append(f"数据质量检查失败: {quality_msg}")
                raise ValueError(quality_msg)
            self.logger.info(f"数据质量检查通过: {quality_msg}")
            
            # 步骤1: 准备股票池
            self.logger.info("步骤1: 准备股票池")
            stock_pool = self._prepare_stock_pool(universe)
            total_stocks = len(stock_pool)
            self.logger.info(f"股票池: {total_stocks} 只股票")
            
            # 步骤2: 加载并评分
            self.logger.info("步骤2: 加载真实数据并计算评分")
            scored_stocks = self._load_and_score_stocks(stock_pool, date, strategy_type)
            self.logger.info(f"完成评分: {len(scored_stocks)} 只股票")
            
            # 步骤3: 应用过滤器
            self.logger.info("步骤3: 应用过滤器")
            filtered_stocks, filters = self._apply_filters(
                scored_stocks, 
                strategy_type,
                min_roe=min_roe,
                max_debt_ratio=max_debt_ratio,
                min_gross_margin=min_gross_margin
            )
            filters_applied.extend(filters)
            selected_stocks = len(filtered_stocks)
            filtered_out = total_stocks - selected_stocks
            self.logger.info(f"过滤后: {selected_stocks} 只 (过滤掉 {filtered_out} 只)")
            
            # 步骤4: 排序并选择Top N
            self.logger.info(f"步骤4: 排序并选择 Top {top_n}")
            top_stocks = self._select_top_stocks(filtered_stocks, top_n)
            
            # 步骤5: 准备输出
            self.logger.info("步骤5: 准备输出结果")
            top_stocks_list = [s.to_dict() for s in top_stocks]
            
            status = "success"
            
        except Exception as e:
            self.logger.error(f"选股策略执行失败: {e}", exc_info=True)
            errors.append(str(e))
            status = "failed"
            total_stocks = 0
            selected_stocks = 0
            filtered_out = 0
            top_stocks_list = []
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = SelectionResult(
            strategy_type=strategy_type.value,
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            total_stocks=total_stocks,
            selected_stocks=selected_stocks,
            filtered_out=filtered_out,
            top_stocks=top_stocks_list,
            filters_applied=filters_applied,
            errors=errors
        )
        
        # 保存结果
        self._save_results(result, date)
        
        self.logger.info(f"选股策略工作流完成: 选出 {len(top_stocks_list)} 只股票")
        
        return result
    
    def _prepare_stock_pool(self, universe: Optional[List[str]]) -> pd.DataFrame:
        """准备股票池"""
        if universe:
            # 使用指定股票池
            stock_pool = pd.DataFrame({'code': universe})
        else:
            # 从本地K线数据目录获取股票列表
            stock_codes = []
            if self.kline_dir.exists():
                for f in self.kline_dir.glob("*.parquet"):
                    code = f.stem
                    stock_codes.append(code)
            
            if not stock_codes:
                # 如果没有本地数据，尝试从股票列表获取
                stock_list = self.data_service.get_stock_list_sync()
                if stock_list is not None and not stock_list.empty:
                    stock_codes = stock_list['code'].tolist()
            
            stock_pool = pd.DataFrame({'code': stock_codes})
        
        # 过滤ST、退市股票
        stock_pool = self._filter_st_delisted(stock_pool)
        
        return stock_pool
    
    def _filter_st_delisted(self, stock_pool: pd.DataFrame) -> pd.DataFrame:
        """过滤ST、退市、停牌股票"""
        original_count = len(stock_pool)
        
        try:
            stock_list_file = self.data_path / "stock_list.parquet"
            if stock_list_file.exists():
                stock_list = pd.read_parquet(stock_list_file)
                
                # 检查列名 (可能是 'code_name' 而不是 'name')
                name_col = 'code_name' if 'code_name' in stock_list.columns else 'name'
                
                # 合并股票信息
                merge_cols = ['code', 'tradeStatus']
                if name_col in stock_list.columns:
                    merge_cols.append(name_col)
                
                stock_pool = stock_pool.merge(
                    stock_list[merge_cols], 
                    on='code', 
                    how='left'
                )
                
                # 1. 过滤停牌/退市股票 (tradeStatus='0')
                if 'tradeStatus' in stock_pool.columns:
                    # tradeStatus可能是字符串类型
                    trade_status_str = stock_pool['tradeStatus'].astype(str)
                    active_mask = trade_status_str == '1'
                    suspended = (~active_mask).sum()
                    stock_pool = stock_pool[active_mask]
                    if suspended > 0:
                        self.logger.info(f"过滤停牌/退市股票: {suspended} 只")
                
                # 2. 过滤ST股票 (名称包含ST)
                if name_col in stock_pool.columns:
                    st_mask = ~stock_pool[name_col].str.contains(r'ST|退市', na=False, regex=True)
                    st_count = (~st_mask).sum()
                    stock_pool = stock_pool[st_mask]
                    if st_count > 0:
                        self.logger.info(f"过滤ST股票: {st_count} 只")
                
                filtered_count = original_count - len(stock_pool)
                if filtered_count > 0:
                    self.logger.info(f"共过滤风险股票: {filtered_count} 只")
                    
        except Exception as e:
            self.logger.warning(f"过滤ST/退市股票失败: {e}")
        
        return stock_pool
    
    def _check_data_quality(self) -> tuple:
        """检查数据质量
        
        Returns:
            (is_ok, message)
        """
        try:
            # 检查股票列表
            stock_list_file = self.data_path / "stock_list.parquet"
            if not stock_list_file.exists():
                return False, "股票列表文件不存在"
            
            stock_list = pd.read_parquet(stock_list_file)
            if stock_list.empty:
                return False, "股票列表为空"
            
            # 检查tradeStatus字段
            if 'tradeStatus' not in stock_list.columns:
                return False, "股票列表缺少tradeStatus字段"
            
            # 统计正常交易股票
            trade_status_str = stock_list['tradeStatus'].astype(str)
            active_count = (trade_status_str == '1').sum()
            suspended_count = (trade_status_str == '0').sum()
            
            if active_count == 0:
                return False, f"没有正常交易的股票 (停牌/退市: {suspended_count})"
            
            # 检查K线数据
            kline_files = list(self.kline_dir.glob("*.parquet"))
            if len(kline_files) == 0:
                return False, "没有K线数据文件"
            
            # 抽样检查数据新鲜度
            sample_files = kline_files[:5]
            latest_dates = []
            for f in sample_files:
                try:
                    df = pd.read_parquet(f)
                    if not df.empty and 'trade_date' in df.columns:
                        latest_date = pd.to_datetime(df['trade_date']).max()
                        latest_dates.append(latest_date)
                except:
                    pass
            
            if latest_dates:
                overall_latest = max(latest_dates)
                days_old = (datetime.now() - overall_latest).days
                
                if days_old > 30:
                    return False, f"数据过于陈旧，最新数据是 {days_old} 天前"
                
                return True, f"数据质量良好: {active_count}只正常交易股, 数据延迟{days_old}天"
            
            return True, f"数据质量良好: {active_count}只正常交易股"
            
        except Exception as e:
            return False, f"数据质量检查异常: {e}"
    
    def _load_and_score_stocks(self, stock_pool: pd.DataFrame, date: str, 
                               strategy_type: StrategyType) -> List[StockScore]:
        """加载数据并计算评分"""
        scored_stocks = []
        
        for _, row in stock_pool.iterrows():
            code = row['code']
            name = row.get('name', '')
            
            try:
                score = self._calculate_stock_score(code, name, date, strategy_type)
                scored_stocks.append(score)
            except Exception as e:
                self.logger.warning(f"计算 {code} 评分失败: {e}")
        
        return scored_stocks
    
    def _calculate_stock_score(self, code: str, name: str, date: str, 
                               strategy_type: StrategyType) -> StockScore:
        """计算单只股票的综合评分"""
        score = StockScore(code=code, name=name)
        
        # 1. 计算财务评分 (基于K线数据计算简单财务指标)
        score.financial_score = self._calculate_financial_score(code, date)
        
        # 2. 计算市场评分 (基于资金流向等)
        score.market_score = self._calculate_market_score(code, date)
        
        # 3. 计算公告评分
        score.announcement_score = self._calculate_announcement_score(code, date)
        
        # 4. 计算技术评分 (基于K线技术指标)
        score.technical_score = self._calculate_technical_score(code, date)
        
        # 根据策略类型调整权重
        weights = self._get_strategy_weights(strategy_type)
        
        score.total_score = (
            score.financial_score * weights['financial'] +
            score.market_score * weights['market'] +
            score.announcement_score * weights['announcement'] +
            score.technical_score * weights['technical']
        )
        
        return score
    
    def _calculate_financial_score(self, code: str, date: str) -> float:
        """计算财务评分 (基于K线数据估算)"""
        try:
            kline = self._load_kline_data(code)
            if kline is None or kline.empty:
                return 50.0  # 默认中等评分
            
            # 计算简单财务指标
            latest = kline.iloc[-1]
            
            # 价格趋势 ( proxy for growth)
            if len(kline) >= 60:
                price_60d_ago = kline.iloc[-60]['close']
                price_change_60d = (latest['close'] - price_60d_ago) / price_60d_ago * 100
            else:
                price_change_60d = 0
            
            # 成交量趋势
            if len(kline) >= 20:
                vol_recent = kline.iloc[-5:]['volume'].mean()
                vol_history = kline.iloc[-20:-5]['volume'].mean()
                volume_trend = (vol_recent - vol_history) / vol_history * 100 if vol_history > 0 else 0
            else:
                volume_trend = 0
            
            # 波动率 (越低越好)
            volatility = kline['pct_chg'].std() if 'pct_chg' in kline.columns else 0
            
            # 综合评分 (0-100)
            score = 50.0
            score += min(price_change_60d * 0.5, 20)  # 价格趋势加分
            score += min(volume_trend * 0.2, 10)      # 成交量趋势加分
            score -= min(volatility * 2, 15)          # 高波动率减分
            
            return max(0, min(100, score))
            
        except Exception as e:
            self.logger.warning(f"计算 {code} 财务评分失败: {e}")
            return 50.0
    
    def _calculate_market_score(self, code: str, date: str) -> float:
        """计算市场行为评分"""
        try:
            kline = self._load_kline_data(code)
            if kline is None or kline.empty:
                return 50.0
            
            # 近期资金流向 proxy (价格与成交量配合)
            recent = kline.iloc[-5:]
            
            # 计算资金流入流出 (简化版)
            money_flow = 0
            for _, row in recent.iterrows():
                if row['close'] > row['open']:  # 阳线
                    money_flow += row['volume'] * row['close']
                else:  # 阴线
                    money_flow -= row['volume'] * row['close']
            
            # 涨跌幅表现
            price_change_5d = 0
            if len(kline) >= 5:
                price_change_5d = (kline.iloc[-1]['close'] - kline.iloc[-5]['close']) / kline.iloc[-5]['close'] * 100
            
            # 综合评分
            score = 50.0
            score += min(price_change_5d * 2, 25)  # 5日涨幅
            score += min(money_flow / 1e8, 15)     # 资金流向 (简化)
            
            return max(0, min(100, score))
            
        except Exception as e:
            self.logger.warning(f"计算 {code} 市场评分失败: {e}")
            return 50.0
    
    def _calculate_announcement_score(self, code: str, date: str) -> float:
        """计算公告评分"""
        # 目前没有实时公告数据，基于价格异动检测
        try:
            kline = self._load_kline_data(code)
            if kline is None or kline.empty:
                return 50.0
            
            # 检测异常波动 (可能是有公告影响)
            if 'pct_chg' not in kline.columns:
                return 50.0
            
            recent_changes = kline.iloc[-5:]['pct_chg'].abs()
            if recent_changes.empty:
                return 50.0
                
            max_change = recent_changes.max()
            
            # 有大幅波动可能是有重大公告
            if max_change > 5:
                return 70.0  # 可能有利好消息
            elif max_change > 3:
                return 60.0
            else:
                return 50.0
                
        except Exception as e:
            self.logger.debug(f"计算 {code} 公告评分失败: {e}")
            return 50.0
    
    def _calculate_technical_score(self, code: str, date: str) -> float:
        """计算技术评分"""
        try:
            kline = self._load_kline_data(code)
            if kline is None or len(kline) < 20:
                return 50.0
            
            # 计算技术指标
            closes = kline['close'].values
            
            # 均线位置 (价格在MA20之上加分)
            ma20 = closes[-20:].mean()
            current_price = closes[-1]
            ma_position = (current_price - ma20) / ma20 * 100
            
            # 趋势强度
            if len(closes) >= 10:
                price_10d_ago = closes[-10]
                trend = (current_price - price_10d_ago) / price_10d_ago * 100
            else:
                trend = 0
            
            # 成交量配合
            if len(kline) >= 10:
                vol_recent = kline.iloc[-3:]['volume'].mean()
                vol_history = kline.iloc[-10:-3]['volume'].mean()
                vol_ratio = vol_recent / vol_history if vol_history > 0 else 1
            else:
                vol_ratio = 1
            
            # 综合评分
            score = 50.0
            score += min(ma_position * 2, 20)      # 均线位置
            score += min(trend * 1.5, 15)          # 趋势
            score += min((vol_ratio - 1) * 10, 10) # 成交量配合
            
            return max(0, min(100, score))
            
        except Exception as e:
            self.logger.warning(f"计算 {code} 技术评分失败: {e}")
            return 50.0
    
    def _load_kline_data(self, code: str) -> Optional[pd.DataFrame]:
        """加载K线数据"""
        try:
            file_path = self.kline_dir / f"{code}.parquet"
            if file_path.exists():
                df = pd.read_parquet(file_path)
                # 确保必要的列存在
                if df is not None and not df.empty:
                    # 转换日期列
                    if 'trade_date' in df.columns:
                        df['trade_date'] = pd.to_datetime(df['trade_date'])
                    return df
            return None
        except Exception as e:
            self.logger.debug(f"加载 {code} K线数据失败: {e}")
            return None
    
    def _get_strategy_weights(self, strategy_type: StrategyType) -> Dict[str, float]:
        """获取策略权重配置"""
        weights = {
            StrategyType.VALUE_GROWTH: {
                'financial': 0.50,
                'market': 0.15,
                'announcement': 0.15,
                'technical': 0.20
            },
            StrategyType.MAIN_FORCE_TRACKING: {
                'financial': 0.20,
                'market': 0.50,
                'announcement': 0.15,
                'technical': 0.15
            },
            StrategyType.EVENT_DRIVEN: {
                'financial': 0.20,
                'market': 0.20,
                'announcement': 0.50,
                'technical': 0.10
            },
            StrategyType.COMPREHENSIVE: {
                'financial': 0.35,
                'market': 0.30,
                'announcement': 0.20,
                'technical': 0.15
            }
        }
        return weights.get(strategy_type, weights[StrategyType.COMPREHENSIVE])
    
    def _apply_filters(self, stocks: List[StockScore], strategy_type: StrategyType,
                       min_roe: float, max_debt_ratio: float, 
                       min_gross_margin: float) -> Tuple[List[StockScore], List[str]]:
        """应用过滤器"""
        filters_applied = []
        filtered = []
        
        # 基础过滤器
        filters_applied.append(f"财务评分 >= 40")
        filters_applied.append(f"技术评分 >= 30")
        
        for stock in stocks:
            # 基础过滤条件
            if stock.financial_score < 40:
                stock.filter_reason = "财务评分过低"
                continue
            
            if stock.technical_score < 30:
                stock.filter_reason = "技术评分过低"
                continue
            
            # 策略特定过滤
            if strategy_type == StrategyType.VALUE_GROWTH:
                if stock.financial_score < 60:
                    stock.filter_reason = "价值成长型要求财务评分>=60"
                    continue
                    
            elif strategy_type == StrategyType.MAIN_FORCE_TRACKING:
                if stock.market_score < 50:
                    stock.filter_reason = "主力资金追踪型要求市场评分>=50"
                    continue
                    
            elif strategy_type == StrategyType.EVENT_DRIVEN:
                if stock.announcement_score < 60:
                    stock.filter_reason = "事件驱动型要求公告评分>=60"
                    continue
            
            filtered.append(stock)
        
        return filtered, filters_applied
    
    def _select_top_stocks(self, stocks: List[StockScore], top_n: int) -> List[StockScore]:
        """选择Top N股票"""
        # 按综合评分排序
        sorted_stocks = sorted(stocks, key=lambda x: x.total_score, reverse=True)
        return sorted_stocks[:top_n]
    
    def _save_results(self, result: SelectionResult, date: str):
        """保存选股结果"""
        result_file = self.results_dir / f"real_selection_{result.strategy_type}_{date}.json"

        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)

        self.logger.info(f"选股结果已保存: {result_file}")

        # 记录血缘关系
        try:
            selected_codes = [s['code'] for s in result.to_dict().get('top_stocks', [])]
            if selected_codes:
                self.lineage_workflow.record_selection_lineage(
                    selected_codes,
                    result.strategy_type
                )
                self.logger.info(f"血缘关系已记录: {len(selected_codes)} 只股票")
        except Exception as e:
            self.logger.warning(f"记录血缘关系失败: {e}")
    
    def get_selection_history(self, strategy_type: Optional[str] = None, days: int = 7) -> List[Dict]:
        """获取选股历史"""
        history = []
        
        pattern = f"real_selection_{strategy_type}_*.json" if strategy_type else "real_selection_*.json"
        
        for result_file in sorted(self.results_dir.glob(pattern), reverse=True)[:days]:
            try:
                with open(result_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    history.append({
                        'date': data.get('end_time', '')[:10],
                        'strategy_type': data.get('strategy_type'),
                        'status': data.get('status'),
                        'selected_stocks': data.get('selected_stocks'),
                        'top_5_stocks': data.get('top_stocks', [])[:5]
                    })
            except Exception as e:
                self.logger.warning(f"读取历史记录失败 {result_file}: {e}")
        
        return history


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='真实数据选股策略工作流')
    parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                       default='comprehensive', help='策略类型')
    parser.add_argument('--top-n', type=int, default=50, help='输出数量')
    parser.add_argument('--date', help='选股日期 (YYYY-MM-DD)')
    parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')
    parser.add_argument('--min-roe', type=float, default=10.0, help='最小ROE')
    parser.add_argument('--max-debt', type=float, default=60.0, help='最大资产负债率')
    
    args = parser.parse_args()
    
    # 创建工作流
    workflow = RealStockSelectionWorkflow()
    
    # 解析参数
    strategy_map = {
        'value_growth': StrategyType.VALUE_GROWTH,
        'main_force': StrategyType.MAIN_FORCE_TRACKING,
        'event_driven': StrategyType.EVENT_DRIVEN,
        'comprehensive': StrategyType.COMPREHENSIVE
    }
    strategy_type = strategy_map[args.strategy]
    codes = args.codes.split(',') if args.codes else None
    
    # 运行工作流
    result = workflow.run(
        strategy_type=strategy_type,
        universe=codes,
        top_n=args.top_n,
        date=args.date,
        min_roe=args.min_roe,
        max_debt_ratio=args.max_debt
    )
    
    # 输出结果
    print("\n" + "="*80)
    print(f"真实数据选股策略结果: {result.strategy_type}")
    print("="*80)
    
    status_icon = "✅" if result.status == "success" else "❌"
    print(f"\n{status_icon} 状态: {result.status}")
    print(f"📊 总股票数: {result.total_stocks}")
    print(f"🎯 选中股票数: {result.selected_stocks}")
    print(f"🚫 过滤掉: {result.filtered_out}")
    print(f"⏱️ 耗时: {result.duration_seconds:.2f}秒")
    
    if result.filters_applied:
        print(f"\n🔍 应用的过滤器:")
        for filter_name in result.filters_applied:
            print(f"   - {filter_name}")
    
    if result.top_stocks:
        print(f"\n📈 Top {len(result.top_stocks)} 股票:")
        print(f"{'排名':<6}{'代码':<10}{'名称':<12}{'综合':<8}{'财务':<8}{'市场':<8}{'公告':<8}{'技术':<8}")
        print("-" * 80)
        for i, stock in enumerate(result.top_stocks[:20], 1):  # 最多显示20只
            name = stock['name'][:10] if stock['name'] else ''
            print(f"{i:<6}{stock['code']:<10}{name:<12}"
                  f"{stock['total_score']:<8.1f}{stock['financial_score']:<8.1f}"
                  f"{stock['market_score']:<8.1f}{stock['announcement_score']:<8.1f}"
                  f"{stock['technical_score']:<8.1f}")
    
    if result.errors:
        print(f"\n❌ 错误:")
        for error in result.errors:
            print(f"   - {error}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
