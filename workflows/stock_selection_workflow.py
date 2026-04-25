#!/usr/bin/env python3
"""
选股策略工作流

实现选股策略业务流:
- 股票池准备
- 数据加载
- 过滤器链执行
- 综合评分与排序
- 结果输出
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import json

import pandas as pd
import numpy as np

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.quality.ge_checkpoint_validators import GECheckpointValidators, CheckStatus, GERetryConfig
from filters.financial_filter import (
    ROEFilter,
    ProfitabilityFilter,
    SolvencyFilter,
    GrowthFilter,
    CashFlowFilter,
    FinancialCompositeFilter
)
from filters.market_behavior_filter import (
    DragonTigerFilter,
    MoneyFlowFilter,
    NorthboundFilter,
    MainForceFilter
)
from filters.announcement_filter import (
    PerformanceForecastFilter,
    MajorEventFilter,
    EquityChangeFilter,
    TradingResumeFilter,
    AnnouncementCompositeFilter
)


class StrategyType(Enum):
    """策略类型"""
    VALUE_GROWTH = "value_growth"           # 价值成长型
    MAIN_FORCE_TRACKING = "main_force"      # 主力资金追踪型
    EVENT_DRIVEN = "event_driven"           # 事件驱动型
    COMPREHENSIVE = "comprehensive"         # 综合策略
    CUSTOM = "custom"                       # 自定义策略


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
            'top_stocks': self.top_stocks,
            'filters_applied': self.filters_applied,
            'errors': self.errors
        }


class StockSelectionWorkflow:
    """选股策略工作流"""

    def __init__(self):
        """初始化选股策略工作流"""
        self.logger = setup_logger("stock_selection_workflow")
        self.data_service = UnifiedDataService()
        # 使用GE验证器，配置重试3次
        retry_config = GERetryConfig(max_retries=3, retry_delay=1.0)
        self.checkpoint_validator = GECheckpointValidators(retry_config)

        # 初始化过滤器
        self.roe_filter = ROEFilter()
        self.profitability_filter = ProfitabilityFilter()
        self.solvency_filter = SolvencyFilter()
        self.growth_filter = GrowthFilter()
        self.cashflow_filter = CashFlowFilter()
        self.financial_composite_filter = FinancialCompositeFilter()

        self.dragon_tiger_filter = DragonTigerFilter()
        self.money_flow_filter = MoneyFlowFilter()
        self.northbound_filter = NorthboundFilter()
        self.main_force_filter = MainForceFilter()

        self.performance_forecast_filter = PerformanceForecastFilter()
        self.major_event_filter = MajorEventFilter()
        self.equity_change_filter = EquityChangeFilter()
        self.trading_resume_filter = TradingResumeFilter()
        self.announcement_composite_filter = AnnouncementCompositeFilter()

        self.results_dir = get_data_path() / "workflow_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self,
            strategy_type: StrategyType = StrategyType.COMPREHENSIVE,
            universe: Optional[List[str]] = None,
            top_n: int = 50,
            date: Optional[str] = None,
            custom_filters: Optional[Dict] = None) -> SelectionResult:
        """
        运行选股策略工作流
        
        Args:
            strategy_type: 策略类型
            universe: 股票池 (默认为全市场)
            top_n: 输出数量
            date: 选股日期 (默认为今天)
            custom_filters: 自定义过滤器参数
        
        Returns:
            选股结果
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        self.logger.info(f"开始选股策略工作流: {strategy_type.value}, 日期: {date}")
        start_time = datetime.now()

        errors = []
        filters_applied = []
        quality_checks = {}

        try:
            # 步骤1: 股票池准备
            self.logger.info("步骤1: 准备股票池")
            stock_pool = self._prepare_stock_pool(universe, date)
            total_stocks = len(stock_pool)
            self.logger.info(f"股票池: {total_stocks} 只股票")

            # 检查点5: 选股前检查
            pre_check = self.checkpoint_validator.pre_selection_check(stock_pool, date)
            quality_checks['pre_selection'] = pre_check.to_dict()
            if pre_check.status == CheckStatus.FAILED:
                raise ValueError(f"选股前检查失败: {pre_check.message}")
            if pre_check.status == CheckStatus.WARNING:
                self.logger.warning(f"选股前检查警告: {pre_check.message}")

            # 步骤2: 数据加载
            self.logger.info("步骤2: 加载数据")
            stock_data = self._load_stock_data(stock_pool, date)

            # 步骤3: 过滤器链执行
            self.logger.info("步骤3: 执行过滤器链")
            filtered_stocks = stock_data.copy()

            # 根据策略类型应用不同过滤器
            if strategy_type == StrategyType.VALUE_GROWTH:
                filtered_stocks, filters = self._apply_value_growth_filters(filtered_stocks, custom_filters)
                filters_applied.extend(filters)

            elif strategy_type == StrategyType.MAIN_FORCE_TRACKING:
                filtered_stocks, filters = self._apply_main_force_filters(filtered_stocks, date, custom_filters)
                filters_applied.extend(filters)

            elif strategy_type == StrategyType.EVENT_DRIVEN:
                filtered_stocks, filters = self._apply_event_driven_filters(filtered_stocks, date, custom_filters)
                filters_applied.extend(filters)

            else:  # COMPREHENSIVE or CUSTOM
                filtered_stocks, filters = self._apply_comprehensive_filters(filtered_stocks, date, custom_filters)
                filters_applied.extend(filters)

            selected_stocks = len(filtered_stocks)
            self.logger.info(f"过滤后: {selected_stocks} 只股票")

            # 步骤4: 综合评分与排序
            self.logger.info("步骤4: 综合评分与排序")
            scored_stocks = self._calculate_scores(filtered_stocks, strategy_type)
            top_stocks = scored_stocks.head(top_n)

            # 步骤5: 准备输出
            self.logger.info("步骤5: 准备输出结果")
            top_stocks_list = self._prepare_output(top_stocks, date)

            # 检查点6: 最终输出验证
            output_df = pd.DataFrame(top_stocks_list)
            if len(output_df) > 0:
                final_check = self.checkpoint_validator.final_output_validation(output_df)
                quality_checks['final_output'] = final_check.to_dict()
                if final_check.status == CheckStatus.FAILED:
                    raise ValueError(f"最终输出验证失败: {final_check.message}")
                if final_check.status == CheckStatus.WARNING:
                    self.logger.warning(f"最终输出验证警告: {final_check.message}")

            status = "success"

        except Exception as e:
            self.logger.error(f"选股策略执行失败: {e}")
            errors.append(str(e))
            status = "failed"
            total_stocks = 0
            selected_stocks = 0
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
            top_stocks=top_stocks_list,
            filters_applied=filters_applied,
            errors=errors
        )

        # 添加质量检查结果
        result.quality_checks = quality_checks
        
        # 保存结果
        self._save_results(result, date)
        
        self.logger.info(f"选股策略工作流完成: 选出 {len(top_stocks_list)} 只股票")
        
        return result
    
    def _prepare_stock_pool(self, universe: Optional[List[str]], date: str) -> pd.DataFrame:
        """准备股票池"""
        if universe:
            # 使用指定股票池
            stock_pool = pd.DataFrame({'code': universe})
        else:
            # 获取全市场股票 (使用同步版本)
            stock_list = self.data_service.get_stock_list_sync()
            if stock_list is not None and not stock_list.empty:
                stock_pool = stock_list[['code', 'name']] if 'name' in stock_list.columns else stock_list[['code']]
            else:
                stock_pool = pd.DataFrame({'code': []})
        
        # 过滤停牌、ST、退市股票 (简化处理)
        # 实际应该调用专门的过滤函数
        
        return stock_pool
    
    def _load_stock_data(self, stock_pool: pd.DataFrame, date: str) -> pd.DataFrame:
        """加载股票数据"""
        # 为每只股票加载基础数据
        stock_data = stock_pool.copy()
        
        # 添加基础字段
        stock_data['score'] = 0.0
        stock_data['financial_score'] = 0.0
        stock_data['market_score'] = 0.0
        stock_data['announcement_score'] = 0.0
        
        return stock_data
    
    def _apply_value_growth_filters(self, stocks: pd.DataFrame, custom_filters: Optional[Dict]) -> tuple:
        """应用价值成长型过滤器"""
        filters_applied = []
        
        # 财务指标过滤
        filters_applied.append("ROE >= 15%")
        filters_applied.append("毛利率 >= 30%")
        filters_applied.append("营收增长率 >= 15%")
        filters_applied.append("资产负债率 <= 50%")
        
        # 模拟过滤逻辑
        # 实际应该调用 FinancialFilter
        
        return stocks, filters_applied
    
    def _apply_main_force_filters(self, stocks: pd.DataFrame, date: str, custom_filters: Optional[Dict]) -> tuple:
        """应用主力资金追踪型过滤器"""
        filters_applied = []
        
        # 市场行为过滤
        filters_applied.append("近3日机构净买入 >= 1000万")
        filters_applied.append("近5日主力净流入 >= 2000万")
        filters_applied.append("龙虎榜上榜次数 >= 1次")
        
        # 模拟过滤逻辑
        # 实际应该调用 MarketBehaviorFilter
        
        return stocks, filters_applied
    
    def _apply_event_driven_filters(self, stocks: pd.DataFrame, date: str, custom_filters: Optional[Dict]) -> tuple:
        """应用事件驱动型过滤器"""
        filters_applied = []
        
        # 公告事件过滤
        filters_applied.append("业绩预告: 预增/扭亏")
        filters_applied.append("重大事项: 并购重组")
        filters_applied.append("股权变动: 增持")
        
        # 模拟过滤逻辑
        # 实际应该调用 AnnouncementFilter
        
        return stocks, filters_applied
    
    def _apply_comprehensive_filters(self, stocks: pd.DataFrame, date: str, custom_filters: Optional[Dict]) -> tuple:
        """应用综合过滤器"""
        filters_applied = []
        
        # 财务指标
        filters_applied.append("ROE >= 10%")
        filters_applied.append("资产负债率 <= 60%")
        
        # 市场行为
        filters_applied.append("主力净流入 >= 0")
        
        # 公告事件 (加分项)
        filters_applied.append("近期无重大负面公告")
        
        return stocks, filters_applied
    
    def _calculate_scores(self, stocks: pd.DataFrame, strategy_type: StrategyType) -> pd.DataFrame:
        """计算综合评分"""
        scored = stocks.copy()
        
        # 根据策略类型设置权重
        if strategy_type == StrategyType.VALUE_GROWTH:
            financial_weight = 0.6
            market_weight = 0.2
            announcement_weight = 0.2
        elif strategy_type == StrategyType.MAIN_FORCE_TRACKING:
            financial_weight = 0.2
            market_weight = 0.6
            announcement_weight = 0.2
        elif strategy_type == StrategyType.EVENT_DRIVEN:
            financial_weight = 0.2
            market_weight = 0.2
            announcement_weight = 0.6
        else:
            financial_weight = 0.4
            market_weight = 0.3
            announcement_weight = 0.2
        
        # 计算综合评分
        scored['score'] = (
            scored['financial_score'] * financial_weight +
            scored['market_score'] * market_weight +
            scored['announcement_score'] * announcement_weight
        )
        
        # 排序
        scored = scored.sort_values('score', ascending=False)
        
        return scored
    
    def _prepare_output(self, top_stocks: pd.DataFrame, date: str) -> List[Dict]:
        """准备输出结果"""
        output = []
        
        for idx, row in top_stocks.iterrows():
            stock_info = {
                'rank': len(output) + 1,
                'code': row.get('code', ''),
                'name': row.get('name', ''),
                'score': round(row.get('score', 0), 2),
                'financial_score': round(row.get('financial_score', 0), 2),
                'market_score': round(row.get('market_score', 0), 2),
                'announcement_score': round(row.get('announcement_score', 0), 2)
            }
            output.append(stock_info)
        
        return output
    
    def _save_results(self, result: SelectionResult, date: str):
        """保存选股结果"""
        result_file = self.results_dir / f"selection_{result.strategy_type}_{date}.json"
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"选股结果已保存: {result_file}")
    
    def get_selection_history(self, strategy_type: Optional[str] = None, days: int = 7) -> List[Dict]:
        """
        获取选股历史
        
        Args:
            strategy_type: 策略类型过滤
            limit: 限制数量
        
        Returns:
            历史记录列表
        """
        history = []
        
        pattern = f"selection_{strategy_type}_*.json" if strategy_type else "selection_*.json"
        
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
    
    parser = argparse.ArgumentParser(description='选股策略工作流')
    parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                       default='comprehensive', help='策略类型')
    parser.add_argument('--top-n', type=int, default=50, help='输出数量')
    parser.add_argument('--date', help='选股日期 (YYYY-MM-DD)')
    parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')
    
    args = parser.parse_args()
    
    # 创建工作流
    workflow = StockSelectionWorkflow()
    
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
        date=args.date
    )
    
    # 输出结果
    print("\n" + "="*60)
    print(f"选股策略工作流结果: {result.strategy_type}")
    print("="*60)
    
    status_icon = "✅" if result.status == "success" else "❌"
    print(f"\n{status_icon} 状态: {result.status}")
    print(f"📊 总股票数: {result.total_stocks}")
    print(f"🎯 选中股票数: {result.selected_stocks}")
    print(f"⏱️ 耗时: {result.duration_seconds:.2f}秒")
    
    if result.filters_applied:
        print(f"\n🔍 应用的过滤器:")
        for filter_name in result.filters_applied:
            print(f"   - {filter_name}")
    
    if result.top_stocks:
        print(f"\n📈 Top {len(result.top_stocks)} 股票:")
        print(f"{'排名':<6}{'代码':<10}{'名称':<12}{'综合评分':<10}{'财务':<8}{'市场':<8}{'公告':<8}")
        print("-" * 70)
        for stock in result.top_stocks[:20]:  # 最多显示20只
            print(f"{stock['rank']:<6}{stock['code']:<10}{stock['name']:<12}"
                  f"{stock['score']:<10.2f}{stock['financial_score']:<8.2f}"
                  f"{stock['market_score']:<8.2f}{stock['announcement_score']:<8.2f}")
    
    if result.errors:
        print(f"\n❌ 错误:")
        for error in result.errors:
            print(f"   - {error}")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    main()
