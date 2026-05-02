"""
凯利公式仓位计算器
用于根据策略历史胜率和盈亏比动态分配仓位
"""
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class KellyCalculator:
    """凯利公式计算器"""
    
    def __init__(self, kelly_fraction: float = 0.25, min_position: float = 0.1, max_position: float = 0.3):
        """
        初始化凯利计算器
        
        Args:
            kelly_fraction: 凯利公式保守系数（0-1），建议0.25-0.5
            min_position: 最小仓位
            max_position: 最大仓位
        """
        self.kelly_fraction = kelly_fraction
        self.min_position = min_position
        self.max_position = max_position
    
    def calculate_kelly_position(self, win_rate: float, profit_loss_ratio: float) -> float:
        """
        计算凯利公式仓位
        
        凯利公式: f* = (p * b - q) / b
        其中:
            p = 胜率
            q = 1 - p (败率)
            b = 盈亏比 (平均盈利/平均亏损)
            f* = 最优仓位比例
        
        Args:
            win_rate: 策略历史胜率 (0-1)
            profit_loss_ratio: 平均盈亏比
        
        Returns:
            建议仓位比例 (0-1)
        """
        if win_rate <= 0 or win_rate >= 1:
            logger.warning(f"胜率异常: {win_rate}, 使用最小仓位")
            return self.min_position
        
        if profit_loss_ratio <= 0:
            logger.warning(f"盈亏比异常: {profit_loss_ratio}, 使用最小仓位")
            return self.min_position
        
        # 计算凯利公式
        p = win_rate
        q = 1 - win_rate
        b = profit_loss_ratio
        
        kelly_pct = (p * b - q) / b
        
        # 应用保守系数
        kelly_pct = kelly_pct * self.kelly_fraction
        
        # 限制在最小和最大仓位之间
        kelly_pct = max(self.min_position, min(kelly_pct, self.max_position))
        
        logger.info(f"凯利公式计算: 胜率={win_rate:.2%}, 盈亏比={profit_loss_ratio:.2f}, "
                   f"原始凯利={kelly_pct/self.kelly_fraction:.2%}, "
                   f"保守凯利={kelly_pct:.2%}")
        
        return kelly_pct
    
    def allocate_stocks_by_kelly(self, strategy_configs: Dict[str, Dict], total_stocks: int = 10) -> Dict[str, int]:
        """
        根据凯利公式分配各策略选股数量
        
        Args:
            strategy_configs: 策略配置字典，包含win_rate和profit_loss_ratio
            total_stocks: 总选股数量
        
        Returns:
            各策略分配的选股数量
        """
        # 计算各策略的凯利仓位
        kelly_positions = {}
        total_kelly = 0
        
        for strategy_name, config in strategy_configs.items():
            win_rate = config.get('win_rate', 0.5)
            profit_loss_ratio = config.get('profit_loss_ratio', 1.5)
            
            kelly_pct = self.calculate_kelly_position(win_rate, profit_loss_ratio)
            kelly_positions[strategy_name] = kelly_pct
            total_kelly += kelly_pct
        
        # 按凯利比例分配选股数量
        allocation = {}
        allocated_total = 0
        
        for strategy_name, kelly_pct in kelly_positions.items():
            if total_kelly > 0:
                stocks_count = int(total_stocks * kelly_pct / total_kelly)
            else:
                stocks_count = total_stocks // len(strategy_configs)
            
            # 至少分配1只
            stocks_count = max(1, stocks_count)
            allocation[strategy_name] = stocks_count
            allocated_total += stocks_count
        
        # 如果分配总数不足，补充到胜率最高的策略
        if allocated_total < total_stocks:
            best_strategy = max(strategy_configs.items(), 
                              key=lambda x: x[1].get('win_rate', 0))[0]
            allocation[best_strategy] += (total_stocks - allocated_total)
        
        # 如果分配总数超出，从胜率最低的策略减少
        elif allocated_total > total_stocks:
            worst_strategy = min(strategy_configs.items(), 
                               key=lambda x: x[1].get('win_rate', 0))[0]
            allocation[worst_strategy] -= (allocated_total - total_stocks)
            allocation[worst_strategy] = max(1, allocation[worst_strategy])
        
        logger.info(f"凯利公式分配结果: {allocation}, 总计={sum(allocation.values())}")
        return allocation


if __name__ == '__main__':
    # 测试代码
    logging.basicConfig(level=logging.INFO)
    
    calculator = KellyCalculator(kelly_fraction=0.25, min_position=0.1, max_position=0.3)
    
    # 测试单个策略
    print("\n=== 测试单个策略 ===")
    kelly_pct = calculator.calculate_kelly_position(win_rate=0.60, profit_loss_ratio=2.0)
    print(f"建议仓位: {kelly_pct:.2%}")
    
    # 测试多策略分配
    print("\n=== 测试多策略分配 ===")
    strategy_configs = {
        'limitup_callback': {'win_rate': 0.55, 'profit_loss_ratio': 1.5},
        'dragon_head': {'win_rate': 0.60, 'profit_loss_ratio': 2.0},
        'tail_rush': {'win_rate': 0.50, 'profit_loss_ratio': 1.2},
        'fund_resonance': {'win_rate': 0.70, 'profit_loss_ratio': 1.8},
    }
    
    allocation = calculator.allocate_stocks_by_kelly(strategy_configs, total_stocks=10)
    print(f"分配结果: {allocation}")
