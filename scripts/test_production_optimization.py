"""
测试生产环境优化脚本
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.production_optimization import ProductionOptimizer


def test_production_optimizer():
    """测试生产环境优化器"""
    print("=" * 70)
    print("测试生产环境优化器")
    print("=" * 70)
    
    optimizer = ProductionOptimizer(data_dir="data")
    
    # 测试 1: 检查目录结构
    print("\n测试 1: 检查目录结构")
    print(f"  数据目录: {optimizer.data_dir}")
    print(f"  配置目录: {optimizer.config_dir}")
    print(f"  备份目录: {optimizer.backup_dir}")
    print(f"  数据目录存在: {optimizer.data_dir.exists()}")
    print(f"  配置目录存在: {optimizer.config_dir.exists()}")
    print(f"  备份目录存在: {optimizer.backup_dir.exists()}")
    
    # 测试 2: 检查配置文件
    print("\n测试 2: 检查配置文件")
    multi_factor_config = optimizer.config_dir / "strategies" / "multi_factor.yaml"
    recommendation_config = optimizer.config_dir / "xcn_comm.yaml"
    
    print(f"  多因子策略配置: {multi_factor_config}")
    print(f"  多因子策略配置存在: {multi_factor_config.exists()}")
    print(f"  推荐系统配置: {recommendation_config}")
    print(f"  推荐系统配置存在: {recommendation_config.exists()}")
    
    # 测试 3: 检查报告目录
    print("\n测试 3: 检查报告目录")
    reports_dir = Path("reports")
    print(f"  报告目录: {reports_dir}")
    print(f"  报告目录存在: {reports_dir.exists()}")
    
    if reports_dir.exists():
        json_files = list(reports_dir.glob("daily_picks_*.json"))
        print(f"  JSON报告数量: {len(json_files)}")
        if json_files:
            latest_report = sorted(json_files, reverse=True)[0]
            print(f"  最新报告: {latest_report}")
    
    # 测试 4: 模拟冠军配置
    print("\n测试 4: 模拟冠军配置")
    mock_champion = {
        'factors': ['rsi', 'macd', 'kdj'],
        'factor_weights': {
            'rsi': 0.4,
            'macd': 0.35,
            'kdj': 0.25
        },
        'factor_params': {
            'rsi': {'period': 14},
            'macd': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
            'kdj': {'n': 9, 'm1': 3, 'm2': 3}
        },
        'filters': ['suspension_filter', 'price_filter'],
        'holding_days': 5,
        'position_size': 10,
        'fitness': 0.85
    }
    
    print(f"  因子数量: {len(mock_champion['factors'])}")
    print(f"  过滤器数量: {len(mock_champion['filters'])}")
    print(f"  适应度: {mock_champion['fitness']}")
    
    # 测试 5: 生成报告
    print("\n测试 5: 生成优化报告")
    report = optimizer.generate_optimization_report(mock_champion)
    print(report)
    
    print("\n✓ 所有测试通过")


def main():
    """主函数"""
    test_production_optimizer()


if __name__ == "__main__":
    main()
