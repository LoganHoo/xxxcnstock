"""
遗传算法优化器
用于因子组合和参数优化
"""
import random
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, field
import copy
import logging

logger = logging.getLogger(__name__)


@dataclass
class Chromosome:
    """染色体 - 表示一个完整的策略配置"""
    
    factors: List[str] = field(default_factory=list)
    factor_weights: Dict[str, float] = field(default_factory=dict)
    factor_params: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    filters: List[str] = field(default_factory=list)
    holding_days: int = 5
    position_size: int = 5
    fitness: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "factors": self.factors,
            "factor_weights": self.factor_weights,
            "factor_params": self.factor_params,
            "filters": self.filters,
            "holding_days": self.holding_days,
            "position_size": self.position_size,
            "fitness": self.fitness
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Chromosome':
        """从字典创建"""
        return cls(
            factors=data.get("factors", []),
            factor_weights=data.get("factor_weights", {}),
            factor_params=data.get("factor_params", {}),
            filters=data.get("filters", []),
            holding_days=data.get("holding_days", 5),
            position_size=data.get("position_size", 5),
            fitness=data.get("fitness", 0.0)
        )


class GeneticOptimizer:
    """遗传算法优化器"""
    
    def __init__(
        self,
        available_factors: List[str],
        available_filters: List[str],
        factor_param_ranges: Dict[str, Dict[str, List]],
        population_size: int = 50,
        generations: int = 30,
        crossover_rate: float = 0.8,
        mutation_rate: float = 0.2,
        elite_size: int = 5,
        min_factors: int = 3,
        max_factors: int = 10,
        min_filters: int = 2,
        max_filters: int = 8
    ):
        """初始化遗传算法优化器"""
        self.available_factors = available_factors
        self.available_filters = available_filters
        self.factor_param_ranges = factor_param_ranges
        self.population_size = population_size
        self.generations = generations
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.elite_size = elite_size
        self.min_factors = min_factors
        self.max_factors = max_factors
        self.min_filters = min_filters
        self.max_filters = max_filters
        
        self.population: List[Chromosome] = []
        self.best_chromosome: Optional[Chromosome] = None
        self.history: List[Dict[str, Any]] = []
    
    def initialize_population(self):
        """初始化种群"""
        self.population = []
        
        for _ in range(self.population_size):
            chromosome = self._create_random_chromosome()
            self.population.append(chromosome)
        
        logger.info(f"初始化种群完成，共 {len(self.population)} 个个体")
    
    def _create_random_chromosome(self) -> Chromosome:
        """创建随机染色体"""
        num_factors = random.randint(self.min_factors, self.max_factors)
        factors = random.sample(self.available_factors, min(num_factors, len(self.available_factors)))
        
        factor_weights = {}
        total_weight = 0.0
        
        for factor in factors:
            weight = random.uniform(0.1, 1.0)
            factor_weights[factor] = weight
            total_weight += weight
        
        for factor in factors:
            factor_weights[factor] /= total_weight
        
        factor_params = {}
        for factor in factors:
            if factor in self.factor_param_ranges:
                factor_params[factor] = {}
                for param_name, param_range in self.factor_param_ranges[factor].items():
                    if isinstance(param_range, list):
                        factor_params[factor][param_name] = random.choice(param_range)
                    elif isinstance(param_range, tuple) and len(param_range) == 2:
                        if isinstance(param_range[0], int):
                            factor_params[factor][param_name] = random.randint(param_range[0], param_range[1])
                        else:
                            factor_params[factor][param_name] = random.uniform(param_range[0], param_range[1])
        
        num_filters = random.randint(self.min_filters, self.max_filters)
        filters = random.sample(self.available_filters, min(num_filters, len(self.available_filters)))
        
        holding_days = random.randint(3, 10)
        position_size = random.randint(3, 10)
        
        return Chromosome(
            factors=factors,
            factor_weights=factor_weights,
            factor_params=factor_params,
            filters=filters,
            holding_days=holding_days,
            position_size=position_size
        )
    
    def evaluate_fitness(self, chromosome: Chromosome, backtest_func) -> float:
        """评估染色体适应度"""
        try:
            result = backtest_func(chromosome)
            
            annual_return = result.get("annual_return", 0)
            sharpe_ratio = result.get("sharpe_ratio", 0)
            max_drawdown = result.get("max_drawdown", 1)
            win_rate = result.get("win_rate", 0)
            
            fitness = (
                0.30 * max(0, annual_return) +
                0.25 * max(0, sharpe_ratio) +
                0.25 * max(0, 1 - max_drawdown) +
                0.20 * win_rate
            )
            
            chromosome.fitness = fitness
            return fitness
            
        except Exception as e:
            logger.error(f"适应度评估失败: {e}")
            chromosome.fitness = 0.0
            return 0.0
    
    def selection(self) -> List[Chromosome]:
        """选择操作 - 轮盘赌选择"""
        fitnesses = [c.fitness for c in self.population]
        total_fitness = sum(fitnesses)
        
        if total_fitness == 0:
            return random.sample(self.population, self.population_size - self.elite_size)
        
        probabilities = [f / total_fitness for f in fitnesses]
        
        selected = []
        for _ in range(self.population_size - self.elite_size):
            r = random.random()
            cumulative = 0.0
            for i, prob in enumerate(probabilities):
                cumulative += prob
                if r <= cumulative:
                    selected.append(copy.deepcopy(self.population[i]))
                    break
        
        return selected
    
    def crossover(self, parent1: Chromosome, parent2: Chromosome) -> Tuple[Chromosome, Chromosome]:
        """交叉操作"""
        if random.random() > self.crossover_rate:
            return copy.deepcopy(parent1), copy.deepcopy(parent2)
        
        all_factors = list(set(parent1.factors + parent2.factors))
        num_factors = random.randint(self.min_factors, min(self.max_factors, len(all_factors)))
        child1_factors = random.sample(all_factors, num_factors)
        child2_factors = random.sample(all_factors, num_factors)
        
        child1_weights = {}
        child2_weights = {}
        
        for factor in child1_factors:
            if factor in parent1.factor_weights and factor in parent2.factor_weights:
                child1_weights[factor] = (parent1.factor_weights[factor] + parent2.factor_weights[factor]) / 2
            elif factor in parent1.factor_weights:
                child1_weights[factor] = parent1.factor_weights[factor]
            else:
                child1_weights[factor] = random.uniform(0.1, 1.0)
        
        for factor in child2_factors:
            if factor in parent1.factor_weights and factor in parent2.factor_weights:
                child2_weights[factor] = (parent1.factor_weights[factor] + parent2.factor_weights[factor]) / 2
            elif factor in parent2.factor_weights:
                child2_weights[factor] = parent2.factor_weights[factor]
            else:
                child2_weights[factor] = random.uniform(0.1, 1.0)
        
        self._normalize_weights(child1_weights)
        self._normalize_weights(child2_weights)
        
        child1_params = self._crossover_params(parent1.factor_params, parent2.factor_params, child1_factors)
        child2_params = self._crossover_params(parent1.factor_params, parent2.factor_params, child2_factors)
        
        all_filters = list(set(parent1.filters + parent2.filters))
        num_filters = random.randint(self.min_filters, min(self.max_filters, len(all_filters)))
        child1_filters = random.sample(all_filters, min(num_filters, len(all_filters)))
        child2_filters = random.sample(all_filters, min(num_filters, len(all_filters)))
        
        child1 = Chromosome(
            factors=child1_factors,
            factor_weights=child1_weights,
            factor_params=child1_params,
            filters=child1_filters,
            holding_days=random.choice([parent1.holding_days, parent2.holding_days]),
            position_size=random.choice([parent1.position_size, parent2.position_size])
        )
        
        child2 = Chromosome(
            factors=child2_factors,
            factor_weights=child2_weights,
            factor_params=child2_params,
            filters=child2_filters,
            holding_days=random.choice([parent1.holding_days, parent2.holding_days]),
            position_size=random.choice([parent1.position_size, parent2.position_size])
        )
        
        return child1, child2
    
    def _crossover_params(
        self, 
        params1: Dict[str, Dict], 
        params2: Dict[str, Dict],
        factors: List[str]
    ) -> Dict[str, Dict]:
        """交叉参数"""
        result = {}
        
        for factor in factors:
            result[factor] = {}
            
            if factor in params1 and factor in params2:
                for param_name in params1[factor]:
                    if param_name in params2[factor]:
                        val1 = params1[factor][param_name]
                        val2 = params2[factor][param_name]
                        
                        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                            if isinstance(val1, int):
                                result[factor][param_name] = int((val1 + val2) / 2)
                            else:
                                result[factor][param_name] = (val1 + val2) / 2
                        else:
                            result[factor][param_name] = random.choice([val1, val2])
                    else:
                        result[factor][param_name] = params1[factor][param_name]
            elif factor in params1:
                result[factor] = copy.deepcopy(params1[factor])
            elif factor in params2:
                result[factor] = copy.deepcopy(params2[factor])
        
        return result
    
    def _normalize_weights(self, weights: Dict[str, float]):
        """归一化权重"""
        total = sum(weights.values())
        if total > 0:
            for key in weights:
                weights[key] /= total
    
    def mutate(self, chromosome: Chromosome) -> Chromosome:
        """变异操作"""
        if random.random() > self.mutation_rate:
            return chromosome
        
        mutation_type = random.choice(["factor", "weight", "param", "filter", "strategy"])
        
        if mutation_type == "factor":
            if random.random() < 0.5 and len(chromosome.factors) > self.min_factors:
                factor_to_remove = random.choice(chromosome.factors)
                chromosome.factors.remove(factor_to_remove)
                del chromosome.factor_weights[factor_to_remove]
                if factor_to_remove in chromosome.factor_params:
                    del chromosome.factor_params[factor_to_remove]
            elif len(chromosome.factors) < self.max_factors:
                available = [f for f in self.available_factors if f not in chromosome.factors]
                if available:
                    new_factor = random.choice(available)
                    chromosome.factors.append(new_factor)
                    chromosome.factor_weights[new_factor] = random.uniform(0.1, 1.0)
                    if new_factor in self.factor_param_ranges:
                        chromosome.factor_params[new_factor] = self._random_params(new_factor)
            
            self._normalize_weights(chromosome.factor_weights)
        
        elif mutation_type == "weight":
            if chromosome.factors:
                factor = random.choice(chromosome.factors)
                chromosome.factor_weights[factor] *= random.uniform(0.8, 1.2)
                self._normalize_weights(chromosome.factor_weights)
        
        elif mutation_type == "param":
            if chromosome.factors:
                factor = random.choice(chromosome.factors)
                if factor in self.factor_param_ranges:
                    chromosome.factor_params[factor] = self._random_params(factor)
        
        elif mutation_type == "filter":
            if random.random() < 0.5 and len(chromosome.filters) > self.min_filters:
                filter_to_remove = random.choice(chromosome.filters)
                chromosome.filters.remove(filter_to_remove)
            elif len(chromosome.filters) < self.max_filters:
                available = [f for f in self.available_filters if f not in chromosome.filters]
                if available:
                    chromosome.filters.append(random.choice(available))
        
        elif mutation_type == "strategy":
            chromosome.holding_days = max(3, min(10, chromosome.holding_days + random.randint(-2, 2)))
            chromosome.position_size = max(3, min(10, chromosome.position_size + random.randint(-2, 2)))
        
        return chromosome
    
    def _random_params(self, factor: str) -> Dict[str, Any]:
        """生成随机参数"""
        params = {}
        if factor in self.factor_param_ranges:
            for param_name, param_range in self.factor_param_ranges[factor].items():
                if isinstance(param_range, list):
                    params[param_name] = random.choice(param_range)
                elif isinstance(param_range, tuple) and len(param_range) == 2:
                    if isinstance(param_range[0], int):
                        params[param_name] = random.randint(param_range[0], param_range[1])
                    else:
                        params[param_name] = random.uniform(param_range[0], param_range[1])
        return params
    
    def evolve(self, backtest_func) -> Chromosome:
        """执行进化过程"""
        self.initialize_population()
        
        for chromosome in self.population:
            self.evaluate_fitness(chromosome, backtest_func)
        
        self.population.sort(key=lambda x: x.fitness, reverse=True)
        self.best_chromosome = copy.deepcopy(self.population[0])
        
        for gen in range(self.generations):
            logger.info(f"第 {gen + 1}/{self.generations} 代进化中...")
            
            self.population.sort(key=lambda x: x.fitness, reverse=True)
            
            generation_best = self.population[0]
            if generation_best.fitness > self.best_chromosome.fitness:
                self.best_chromosome = copy.deepcopy(generation_best)
            
            self.history.append({
                "generation": gen + 1,
                "best_fitness": generation_best.fitness,
                "avg_fitness": np.mean([c.fitness for c in self.population]),
                "best_chromosome": generation_best.to_dict()
            })
            
            logger.info(
                f"  最优适应度: {generation_best.fitness:.4f}, "
                f"平均适应度: {np.mean([c.fitness for c in self.population]):.4f}"
            )
            
            elites = [copy.deepcopy(c) for c in self.population[:self.elite_size]]
            
            selected = self.selection()
            
            new_population = elites.copy()
            
            while len(new_population) < self.population_size:
                parent1, parent2 = random.sample(selected, 2)
                child1, child2 = self.crossover(parent1, parent2)
                
                child1 = self.mutate(child1)
                child2 = self.mutate(child2)
                
                new_population.extend([child1, child2])
            
            self.population = new_population[:self.population_size]
            
            for chromosome in self.population[self.elite_size:]:
                self.evaluate_fitness(chromosome, backtest_func)
        
        logger.info(f"进化完成，最优适应度: {self.best_chromosome.fitness:.4f}")
        return self.best_chromosome
    
    def get_history(self) -> List[Dict[str, Any]]:
        """获取进化历史"""
        return self.history
