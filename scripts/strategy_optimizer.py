#!/usr/bin/env python3
"""
策略优化分析器 - 分析回测结果并提供优化建议

用法:
    python strategy_optimizer.py --backtest-result result.json
"""
import json
import argparse
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class OptimizationSuggestion:
    """优化建议"""
    category: str  # '选股', '择时', '风控', '仓位'
    priority: str  # '高', '中', '低'
    problem: str
    suggestion: str
    expected_improvement: str


class StrategyOptimizer:
    """策略优化器"""
    
    def __init__(self, backtest_result: Dict):
        self.result = backtest_result
        self.suggestions: List[OptimizationSuggestion] = []
    
    def analyze_performance(self) -> Dict:
        """分析整体表现"""
        s = self.result.get("summary", {})
        
        analysis = {
            "profitability": {
                "score": 0,
                "issues": [],
            },
            "win_rate": {
                "score": 0,
                "issues": [],
            },
            "risk_control": {
                "score": 0,
                "issues": [],
            }
        }
        
        # 盈利能力分析
        avg_return = s.get("avg_daily_return", 0)
        if avg_return > 1:
            analysis["profitability"]["score"] = 90
        elif avg_return > 0.5:
            analysis["profitability"]["score"] = 70
        elif avg_return > 0:
            analysis["profitability"]["score"] = 50
        else:
            analysis["profitability"]["score"] = 30
            analysis["profitability"]["issues"].append("日均收益率为负，策略整体亏损")
        
        # 胜率分析
        win_rate = s.get("win_rate_days", 0)
        if win_rate > 60:
            analysis["win_rate"]["score"] = 90
        elif win_rate > 50:
            analysis["win_rate"]["score"] = 70
        elif win_rate > 40:
            analysis["win_rate"]["score"] = 50
        else:
            analysis["win_rate"]["score"] = 30
            analysis["win_rate"]["issues"].append(f"胜率仅{win_rate:.1f}%，低于40%")
        
        # 风险控制分析
        max_loss = s.get("max_daily_loss", 0)
        if max_loss > -5:
            analysis["risk_control"]["score"] = 80
        elif max_loss > -10:
            analysis["risk_control"]["score"] = 60
        else:
            analysis["risk_control"]["score"] = 40
            analysis["risk_control"]["issues"].append(f"最大单日亏损{max_loss:.2f}%，风险较高")
        
        return analysis
    
    def generate_suggestions(self) -> List[OptimizationSuggestion]:
        """生成优化建议"""
        suggestions = []
        s = self.result.get("summary", {})
        t = self.result.get("trades", {})
        b = self.result.get("by_type", {})
        
        # 1. 整体收益分析
        avg_return = s.get("avg_daily_return", 0)
        if avg_return < 0:
            suggestions.append(OptimizationSuggestion(
                category="择时",
                priority="高",
                problem=f"策略整体亏损，日均收益率{avg_return:.2f}%",
                suggestion="加强市场趋势判断，在WEAK周期减少仓位或空仓观望",
                expected_improvement="避免单边下跌中的亏损"
            ))
        
        # 2. 胜率分析
        win_rate = s.get("win_rate_days", 0)
        if win_rate < 50:
            suggestions.append(OptimizationSuggestion(
                category="选股",
                priority="高",
                problem=f"胜率仅{win_rate:.1f}%，选股准确性不足",
                suggestion="1. 增加技术面过滤条件（如MACD金叉、突破均线）\n2. 增加基本面筛选（如业绩预增、行业龙头）\n3. 减少选股数量，提高精选标准",
                expected_improvement="胜率提升至55%以上"
            ))
        
        # 3. 波段 vs 短线对比
        band_return = b.get("band", {}).get("avg_return", 0)
        short_return = b.get("short", {}).get("avg_return", 0)
        
        if band_return > short_return + 1:
            suggestions.append(OptimizationSuggestion(
                category="选股",
                priority="中",
                problem=f"波段趋势表现({band_return:+.2f}%)优于短线打板({short_return:+.2f}%)",
                suggestion="当前市场适合波段操作，可增加波段仓位比例，减少短线交易",
                expected_improvement="整体收益提升0.5-1%"
            ))
        elif short_return > band_return + 1:
            suggestions.append(OptimizationSuggestion(
                category="选股",
                priority="中",
                problem=f"短线打板表现({short_return:+.2f}%)优于波段趋势({band_return:+.2f}%)",
                suggestion="当前市场热点活跃，可增加短线仓位，但需严格止损",
                expected_improvement="抓住短期热点机会"
            ))
        
        # 4. 盈亏比分析
        avg_win = t.get("avg_win", 0)
        avg_loss = t.get("avg_loss", 0)
        
        if avg_loss != 0:
            profit_loss_ratio = abs(avg_win / avg_loss)
            if profit_loss_ratio < 1:
                suggestions.append(OptimizationSuggestion(
                    category="风控",
                    priority="高",
                    problem=f"盈亏比{profit_loss_ratio:.2f}:1，赚小亏大",
                    suggestion="1. 设置止盈点（如+5%止盈）\n2. 严格执行止损（如-3%止损）\n3. 让利润奔跑，截断亏损",
                    expected_improvement="盈亏比提升至1.5:1以上"
                ))
        
        # 5. 最大亏损分析
        max_loss = s.get("max_daily_loss", 0)
        if max_loss < -5:
            suggestions.append(OptimizationSuggestion(
                category="风控",
                priority="高",
                problem=f"最大单日亏损{max_loss:.2f}%，风险控制不足",
                suggestion="1. 单只股票仓位不超过10%\n2. 设置单日最大亏损限额\n3. 增加市场环境过滤，弱势减少仓位",
                expected_improvement="控制最大回撤在5%以内"
            ))
        
        # 6. 累计收益分析
        cumulative = s.get("cumulative_return", 0)
        total_days = s.get("total_days", 1)
        
        if cumulative < 0 and total_days > 5:
            suggestions.append(OptimizationSuggestion(
                category="择时",
                priority="中",
                problem=f"累计收益为负({cumulative:.2f}%)，策略持续失效",
                suggestion="1. 暂停策略，等待市场环境改善\n2. 反向操作（做空）\n3. 调整策略参数适应新环境",
                expected_improvement="避免持续亏损"
            ))
        
        # 7. 交易频率分析
        total_trades = t.get("total", 0)
        if total_trades > 0:
            avg_trades_per_day = total_trades / total_days
            if avg_trades_per_day > 20:
                suggestions.append(OptimizationSuggestion(
                    category="选股",
                    priority="低",
                    problem="交易过于频繁，可能产生过度交易",
                    suggestion="减少选股数量，提高入选标准，集中仓位在最有把握的股票",
                    expected_improvement="降低交易成本，提高单笔收益"
                ))
        
        return suggestions
    
    def print_optimization_report(self):
        """打印优化报告"""
        print("\n" + "="*100)
        print("🔧 策略优化分析报告")
        print("="*100)
        
        # 性能评分
        analysis = self.analyze_performance()
        print("\n【性能评分】")
        for category, data in analysis.items():
            score = data["score"]
            bar = "█" * (score // 10) + "░" * (10 - score // 10)
            print(f"  {category:12} {bar} {score}分")
            for issue in data["issues"]:
                print(f"    ⚠️  {issue}")
        
        # 优化建议
        suggestions = self.generate_suggestions()
        
        if suggestions:
            print("\n【优化建议】")
            print("-"*100)
            
            # 按优先级排序
            priority_order = {"高": 0, "中": 1, "低": 2}
            suggestions.sort(key=lambda x: priority_order.get(x.priority, 3))
            
            for i, sug in enumerate(suggestions, 1):
                priority_icon = "🔴" if sug.priority == "高" else "🟡" if sug.priority == "中" else "🟢"
                print(f"\n{i}. {priority_icon} [{sug.category}] 优先级: {sug.priority}")
                print(f"   问题: {sug.problem}")
                print(f"   建议: {sug.suggestion}")
                print(f"   预期效果: {sug.expected_improvement}")
        else:
            print("\n✅ 策略表现良好，暂无优化建议")
        
        # 行动计划
        print("\n【行动计划】")
        print("-"*100)
        high_priority = [s for s in suggestions if s.priority == "高"]
        if high_priority:
            print("立即执行（高优先级）:")
            for s in high_priority:
                print(f"  • [{s.category}] {s.suggestion.split(chr(10))[0]}")
        
        print("\n" + "="*100)
        
        return suggestions


def main():
    parser = argparse.ArgumentParser(description='策略优化分析器')
    parser.add_argument('--result', type=str, required=True,
                        help='回测结果JSON文件路径')
    
    args = parser.parse_args()
    
    with open(args.result, 'r', encoding='utf-8') as f:
        backtest_result = json.load(f)
    
    optimizer = StrategyOptimizer(backtest_result)
    optimizer.print_optimization_report()


if __name__ == "__main__":
    main()
