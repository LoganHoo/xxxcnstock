"""
生产环境优化和部署脚本
1. 运行因子组合优化，找到冠军策略
2. 将冠军策略更新到生产配置
3. 更新前一天选股的当天股价
"""
import sys
from pathlib import Path
import yaml
import shutil
from datetime import datetime, timedelta
import logging

sys.path.insert(0, str(Path(__file__).parent.parent))

from optimization.factor_combination_optimizer import FactorCombinationOptimizer
import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductionOptimizer:
    """生产环境优化器"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.config_dir = Path("config")
        self.backup_dir = Path("config/backups")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def run_optimization(
        self,
        population_size: int = 30,
        generations: int = 20
    ) -> dict:
        """运行优化"""
        logger.info("=" * 70)
        logger.info("步骤 1: 运行因子组合优化")
        logger.info("=" * 70)
        
        optimizer = FactorCombinationOptimizer(data_dir=str(self.data_dir))
        
        best_chromosome = optimizer.run_optimization(
            population_size=population_size,
            generations=generations,
            output_dir="optimization/results"
        )
        
        return {
            'factors': best_chromosome.factors,
            'factor_weights': best_chromosome.factor_weights,
            'factor_params': best_chromosome.factor_params,
            'filters': best_chromosome.filters,
            'holding_days': best_chromosome.holding_days,
            'position_size': best_chromosome.position_size,
            'fitness': best_chromosome.fitness
        }
    
    def backup_config(self, config_path: Path) -> Path:
        """备份配置文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"{config_path.stem}_{timestamp}.yaml"
        
        shutil.copy2(config_path, backup_path)
        logger.info(f"配置已备份: {backup_path}")
        
        return backup_path
    
    def update_production_config(self, champion_config: dict):
        """更新生产配置"""
        logger.info("=" * 70)
        logger.info("步骤 2: 更新生产配置")
        logger.info("=" * 70)
        
        # 更新多因子策略配置
        multi_factor_config = self.config_dir / "strategies" / "multi_factor.yaml"
        
        if multi_factor_config.exists():
            self.backup_config(multi_factor_config)
            
            with open(multi_factor_config, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # 更新因子和权重
            config['factors']['selected'] = champion_config['factors']
            config['factors']['weights'] = champion_config['factor_weights']
            config['factors']['params'] = champion_config['factor_params']
            
            # 更新过滤器
            config['filters']['selected'] = champion_config['filters']
            
            # 更新执行参数
            config['execution']['holding_days'] = champion_config['holding_days']
            config['execution']['position_size'] = champion_config['position_size']
            
            # 更新元数据
            config['metadata']['optimized_at'] = datetime.now().isoformat()
            config['metadata']['fitness'] = champion_config['fitness']
            config['metadata']['optimization_method'] = 'genetic_algorithm'
            
            with open(multi_factor_config, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
            
            logger.info(f"多因子策略配置已更新: {multi_factor_config}")
        
        # 更新推荐系统配置
        recommendation_config = self.config_dir / "xcn_comm.yaml"
        
        if recommendation_config.exists():
            self.backup_config(recommendation_config)
            
            with open(recommendation_config, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # 更新因子权重
            if 'recommendation' in config and 'factors' in config['recommendation']:
                config['recommendation']['factors']['weights'] = champion_config['factor_weights']
                config['recommendation']['factors']['params'] = champion_config['factor_params']
            
            # 更新过滤器
            if 'recommendation' in config and 'filters' in config['recommendation']:
                for filter_name in champion_config['filters']:
                    if filter_name not in config['recommendation']['filters']:
                        config['recommendation']['filters'][filter_name] = {
                            'enabled': True,
                            'priority': 'high'
                        }
            
            # 更新执行参数
            if 'recommendation' in config and 'execution' in config['recommendation']:
                config['recommendation']['execution']['holding_days'] = champion_config['holding_days']
                config['recommendation']['execution']['position_size'] = champion_config['position_size']
            
            # 更新元数据
            config['metadata']['last_optimization'] = datetime.now().isoformat()
            config['metadata']['best_fitness'] = champion_config['fitness']
            
            with open(recommendation_config, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
            
            logger.info(f"推荐系统配置已更新: {recommendation_config}")
    
    def update_previous_day_prices(self):
        """更新前一天选股的当天股价"""
        logger.info("=" * 70)
        logger.info("步骤 3: 更新前一天选股的当天股价")
        logger.info("=" * 70)
        
        # 获取最新选股报告
        reports_dir = Path("reports")
        
        if not reports_dir.exists():
            logger.warning("报告目录不存在")
            return
        
        # 查找最新的JSON报告
        json_files = sorted(reports_dir.glob("daily_picks_*.json"), reverse=True)
        
        if not json_files:
            logger.warning("没有找到选股报告")
            return
        
        latest_report = json_files[0]
        logger.info(f"最新报告: {latest_report}")
        
        # 读取报告
        with open(latest_report, 'r', encoding='utf-8') as f:
            report_data = yaml.safe_load(f)
        
        # 获取选中的股票代码
        selected_stocks = set()
        for filter_name, stocks in report_data.get('filters', {}).items():
            for stock in stocks.get('stocks', []):
                selected_stocks.add(stock['code'])
        
        logger.info(f"选中的股票数量: {len(selected_stocks)}")
        
        # 获取最新交易日
        now = datetime.now()
        cutoff_hour = 15
        
        if now.hour < cutoff_hour:
            effective_date = now - timedelta(days=1)
        else:
            effective_date = now
        
        effective_date_str = effective_date.strftime('%Y-%m-%d')
        logger.info(f"有效日期: {effective_date_str}")
        
        # 更新股价
        kline_dir = self.data_dir / "kline"
        updated_count = 0
        
        for code in selected_stocks:
            stock_file = kline_dir / f"{code}.parquet"
            
            if stock_file.exists():
                try:
                    df = pl.read_parquet(stock_file)
                    
                    # 查找指定日期的数据
                    price_data = df.filter(pl.col('trade_date') == effective_date_str)
                    
                    if len(price_data) > 0:
                        latest_price = price_data['close'].item()
                        logger.info(f"{code}: {latest_price:.2f}元")
                        updated_count += 1
                    else:
                        # 如果当天没有数据，使用最新数据
                        latest_data = df.sort('trade_date', descending=True).head(1)
                        if len(latest_data) > 0:
                            latest_price = latest_data['close'].item()
                            latest_date = latest_data['trade_date'].item()
                            logger.info(f"{code}: {latest_price:.2f}元 (数据日期: {latest_date})")
                            updated_count += 1
                
                except Exception as e:
                    logger.warning(f"更新 {code} 股价失败: {e}")
        
        logger.info(f"成功更新 {updated_count}/{len(selected_stocks)} 只股票的股价")
    
    def generate_optimization_report(self, champion_config: dict) -> str:
        """生成优化报告"""
        lines = []
        lines.append("=" * 70)
        lines.append("生产环境优化报告")
        lines.append("=" * 70)
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        lines.append("【冠军策略配置】")
        lines.append(f"适应度: {champion_config['fitness']:.4f}")
        lines.append("")
        
        lines.append(f"选中的因子 ({len(champion_config['factors'])} 个):")
        for factor in champion_config['factors']:
            weight = champion_config['factor_weights'].get(factor, 0)
            params = champion_config['factor_params'].get(factor, {})
            params_str = ", ".join([f"{k}={v}" for k, v in params.items()]) if params else "默认"
            lines.append(f"  - {factor}: 权重={weight:.4f}, 参数={params_str}")
        
        lines.append("")
        lines.append(f"选中的过滤器 ({len(champion_config['filters'])} 个):")
        for f in champion_config['filters']:
            lines.append(f"  - {f}")
        
        lines.append("")
        lines.append("【执行参数】")
        lines.append(f"  - 持仓天数: {champion_config['holding_days']}")
        lines.append(f"  - 持仓数量: {champion_config['position_size']}")
        
        lines.append("")
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    def run_full_workflow(
        self,
        population_size: int = 30,
        generations: int = 20,
        update_prices: bool = True
    ):
        """运行完整工作流"""
        logger.info("=" * 70)
        logger.info("生产环境优化和部署工作流")
        logger.info("=" * 70)
        logger.info(f"种群大小: {population_size}")
        logger.info(f"迭代代数: {generations}")
        logger.info(f"更新股价: {'是' if update_prices else '否'}")
        logger.info("=" * 70)
        
        try:
            # 步骤 1: 运行优化
            champion_config = self.run_optimization(population_size, generations)
            
            # 步骤 2: 更新生产配置
            self.update_production_config(champion_config)
            
            # 步骤 3: 更新股价（可选）
            if update_prices:
                self.update_previous_day_prices()
            
            # 步骤 4: 生成报告
            report = self.generate_optimization_report(champion_config)
            print(report)
            
            # 保存报告
            report_file = Path("optimization/results") / f"production_deployment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            report_file.parent.mkdir(parents=True, exist_ok=True)
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            logger.info(f"报告已保存: {report_file}")
            
            logger.info("=" * 70)
            logger.info("生产环境优化和部署完成")
            logger.info("=" * 70)
            
            return champion_config
            
        except Exception as e:
            logger.error(f"工作流执行失败: {e}")
            raise


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="生产环境优化和部署")
    parser.add_argument("--population", type=int, default=30, help="种群大小")
    parser.add_argument("--generations", type=int, default=20, help="迭代代数")
    parser.add_argument("--no-update-prices", action="store_true", help="不更新股价")
    parser.add_argument("--data-dir", type=str, default="data", help="数据目录")
    
    args = parser.parse_args()
    
    optimizer = ProductionOptimizer(data_dir=args.data_dir)
    
    optimizer.run_full_workflow(
        population_size=args.population,
        generations=args.generations,
        update_prices=not args.no_update_prices
    )


if __name__ == "__main__":
    main()
