#!/usr/bin/env python3
"""
模拟交易运行器
Phase 1: 模拟交易部署

使用方法:
    python scripts/simulation_runner.py --config config/simulation_config.yaml
"""
import os
import sys
import yaml
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy, EndstockConfig
from services.strategy_service.dragon_head.strategy import DragonHeadStrategy, DragonHeadConfig
from services.risk_service.position.kelly_calculator import KellyCalculator
from services.risk_service.position.livermore_manager import LivermoreManager
from services.risk_service.stoploss.manager import StopLossManager
from services.risk_service.circuit_breaker.manager import CircuitBreaker


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/simulation.log')
    ]
)
logger = logging.getLogger(__name__)


class SimulationRunner:
    """模拟交易运行器"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.portfolio = {
            'cash': self.config['risk_management']['initial_capital'],
            'positions': {},
            'history': []
        }
        self._init_strategies()
        self._init_risk_management()
        
    def _load_config(self, path: str) -> dict:
        """加载配置文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _init_strategies(self):
        """初始化策略"""
        self.strategies = {}
        
        # 尾盘选股策略
        if self.config['strategies']['endstock_pick']['enabled']:
            endstock_config = EndstockConfig(
                **self.config['strategies']['endstock_pick']['config']
            )
            self.strategies['endstock_pick'] = EndstockPickStrategy(endstock_config)
            logger.info("尾盘选股策略已初始化")
        
        # 龙回头策略
        if self.config['strategies']['dragon_head']['enabled']:
            dragon_config = DragonHeadConfig(
                **self.config['strategies']['dragon_head']['config']
            )
            self.strategies['dragon_head'] = DragonHeadStrategy(dragon_config)
            logger.info("龙回头策略已初始化")
    
    def _init_risk_management(self):
        """初始化风险管理"""
        risk_config = self.config['risk_management']
        
        # 凯利公式
        self.kelly = KellyCalculator({
            'half_kelly': risk_config['kelly']['half_kelly'],
            'max_single_position': risk_config['kelly']['max_position']
        })
        
        # 利弗莫尔仓位管理
        self.livermore = LivermoreManager()
        
        # 止损管理
        self.stoploss = StopLossManager({
            'stoploss_pct': risk_config['stop_loss_pct'],
            'take_profit_1_pct': risk_config['take_profit_1_pct'],
            'take_profit_2_pct': risk_config['take_profit_2_pct']
        })
        
        # 熔断机制
        self.circuit = CircuitBreaker({
            'market_drop_2pct': risk_config['circuit_breaker']['market_drop_2pct'],
            'market_drop_5pct': risk_config['circuit_breaker']['market_drop_5pct']
        })
        
        logger.info("风险管理模块已初始化")
    
    def _generate_mock_market_data(self, date: datetime) -> pd.DataFrame:
        """生成模拟市场数据"""
        np.random.seed(int(date.timestamp()))
        
        # 生成50只股票的模拟数据
        codes = [f'{i:06d}' for i in range(1, 51)]
        
        data = {
            'code': codes,
            'price_change': np.random.uniform(-2, 8, 50),  # 涨幅 -2% 到 8%
            'volume_ratio': np.random.uniform(0.5, 4, 50),  # 量比 0.5 到 4
            'market_cap': np.random.uniform(20, 600, 50),  # 市值 20-600亿
            'above_ma': np.random.choice([True, False], 50, p=[0.6, 0.4])  # 60%在均线上方
        }
        
        return pd.DataFrame(data)
    
    def run_daily_scan(self, market_data: pd.DataFrame, market_change: float = 0.0):
        """运行每日扫描"""
        logger.info("=" * 60)
        logger.info(f"开始每日扫描 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        signals = []
        
        # 运行尾盘选股策略
        if 'endstock_pick' in self.strategies:
            logger.info("\n📊 运行尾盘选股策略...")
            try:
                endstock_signals = self.strategies['endstock_pick'].execute(
                    market_data,
                    current_time='14:35'
                )
                signals.extend(endstock_signals)
                logger.info(f"   发现 {len(endstock_signals)} 个尾盘选股信号")
            except Exception as e:
                logger.error(f"   尾盘选股策略执行失败: {e}")
        
        # 风险管理检查
        logger.info("\n🛡️ 风险管理检查...")
        
        # 检查熔断
        circuit_result = self.circuit.check_market({
            'index_change_pct': market_change,
            'index_code': '000001.SH'
        })
        
        if circuit_result.triggered:
            logger.warning(f"   ⚠️ 熔断触发: {circuit_result.reason}")
            logger.warning(f"   建议操作: {circuit_result.action.value}")
        else:
            logger.info("   ✅ 熔断检查通过")
        
        # 计算建议仓位
        kelly_result = self.kelly.calculate(
            win_rate=0.55,  # 假设胜率55%
            win_loss_ratio=1.5  # 盈亏比1.5
        )
        logger.info(f"   凯利公式建议仓位: {kelly_result.recommended_position:.2%}")
        
        # 输出信号汇总
        logger.info("\n" + "=" * 60)
        logger.info(f"📈 今日信号汇总: 共 {len(signals)} 个")
        logger.info("=" * 60)
        
        for i, signal in enumerate(signals[:10], 1):  # 只显示前10个
            logger.info(f"{i}. {signal.get('code', 'N/A')} - "
                       f"评分: {signal.get('score', 0)}")
        
        return signals
    
    def generate_daily_report(self, day: int):
        """生成每日报告"""
        logger.info("\n" + "=" * 60)
        logger.info(f"📋 第 {day + 1} 日模拟交易报告")
        logger.info("=" * 60)
        
        total_value = self.portfolio['cash']
        for code, pos in self.portfolio['positions'].items():
            total_value += pos.get('value', 0)
        
        initial = self.config['risk_management']['initial_capital']
        return_pct = (total_value - initial) / initial
        
        logger.info(f"初始资金: ¥{initial:,.2f}")
        logger.info(f"当前总值: ¥{total_value:,.2f}")
        logger.info(f"总收益率: {return_pct:+.2%}")
        logger.info(f"持仓数量: {len(self.portfolio['positions'])}")
        logger.info(f"可用现金: ¥{self.portfolio['cash']:,.2f}")
        logger.info("=" * 60)
    
    def run(self):
        """运行模拟交易"""
        logger.info("\n" + "=" * 60)
        logger.info("🚀 量化交易系统 - Phase 1 模拟交易启动")
        logger.info("=" * 60)
        logger.info(f"模拟周期: {self.config['simulation']['duration_days']} 天")
        logger.info(f"初始资金: ¥{self.config['risk_management']['initial_capital']:,.0f}")
        logger.info("=" * 60 + "\n")
        
        # 模拟每日运行
        start_date = datetime.now()
        all_signals = []
        
        for day in range(self.config['simulation']['duration_days']):
            current_date = start_date + timedelta(days=day)
            logger.info(f"\n📅 模拟日期: {current_date.strftime('%Y-%m-%d')}")
            
            # 生成模拟市场数据
            market_data = self._generate_mock_market_data(current_date)
            market_change = np.random.uniform(-0.03, 0.03)  # 模拟大盘涨跌 -3% 到 3%
            
            # 运行每日扫描
            signals = self.run_daily_scan(market_data, market_change)
            all_signals.extend(signals)
            
            # 生成每日报告
            self.generate_daily_report(day)
            
            logger.info("\n" + "-" * 60)
        
        # 生成最终总结报告
        self._generate_final_report(all_signals)
    
    def _generate_final_report(self, all_signals: list):
        """生成最终总结报告"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 Phase 1 模拟交易最终报告")
        logger.info("=" * 60)
        
        total_value = self.portfolio['cash']
        for code, pos in self.portfolio['positions'].items():
            total_value += pos.get('value', 0)
        
        initial = self.config['risk_management']['initial_capital']
        return_pct = (total_value - initial) / initial
        
        logger.info(f"\n💰 资金状况:")
        logger.info(f"   初始资金: ¥{initial:,.2f}")
        logger.info(f"   最终资金: ¥{total_value:,.2f}")
        logger.info(f"   总收益率: {return_pct:+.2%}")
        
        logger.info(f"\n📈 信号统计:")
        logger.info(f"   总信号数: {len(all_signals)}")
        logger.info(f"   日均信号: {len(all_signals) / self.config['simulation']['duration_days']:.1f}")
        
        logger.info(f"\n📋 策略配置:")
        logger.info(f"   尾盘选股: {'启用' if 'endstock_pick' in self.strategies else '禁用'}")
        logger.info(f"   龙回头: {'启用' if 'dragon_head' in self.strategies else '禁用'}")
        
        logger.info(f"\n✅ 模拟交易完成!")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='量化交易系统模拟交易运行器')
    parser.add_argument(
        '--config',
        default='config/simulation_config.yaml',
        help='配置文件路径'
    )
    parser.add_argument(
        '--mode',
        choices=['daily', 'backtest'],
        default='daily',
        help='运行模式: daily=每日扫描, backtest=回测模式'
    )
    
    args = parser.parse_args()
    
    # 创建日志目录
    os.makedirs('logs', exist_ok=True)
    
    # 运行模拟交易
    runner = SimulationRunner(args.config)
    runner.run()


if __name__ == '__main__':
    main()
