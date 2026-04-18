#!/usr/bin/env python3
"""
遗传算法参数优化
使用进化算法搜索最优参数
"""
import random
import logging
from typing import Dict, Any, List, Callable, Tuple
from dataclasses import dataclass
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class Individual:
    """个体（参数组合）"""
    genes: Dict[str, Any]
    fitness: float = 0.0


class GeneticAlgorithmOptimizer:
    """
    遗传算法参数优化器
    
    使用进化算法高效搜索参数空间
    """
    
    def __init__(
        self,
        param_bounds: Dict[str, Tuple[float, float]],
        param_types: Dict[str, str],
        population_size: int = 50,
        generations: int = 100,
        crossover_rate: float = 0.8,
        mutation_rate: float = 0.2,
        elitism: int = 5
    ):
        """
        初始化
        
        Args:
            param_bounds: 参数范围 {param: (min, max)}
            param_types: 参数类型 {param: 'int'|'float'}
            population_size: 种群大小
            generations: 迭代代数
            crossover_rate: 交叉概率
            mutation_rate: 变异概率
            elitism: 精英保留数量
        """
        self.param_bounds = param_bounds
        self.param_types = param_types
        self.population_size = population_size
        self.generations = generations
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.elitism = elitism
        
        self.population: List[Individual] = []
        self.best_individual: Individual = None
        self.history = []
    
    def optimize(
        self,
        fitness_func: Callable,
        maximize: bool = True
    ) -> Tuple[Dict[str, Any], float]:
        """
        执行遗传算法优化
        
        Args:
            fitness_func: 适应度函数
            maximize: 是否最大化
        
        Returns:
            (最优参数, 最优适应度)
        """
        # 初始化种群
        self._initialize_population()
        
        # 评估初始种群
        self._evaluate_population(fitness_func)
        
        for generation in range(self.generations):
            # 选择
            selected = self._select()
            
            # 交叉和变异
            offspring = self._crossover_and_mutate(selected)
            
            # 评估子代
            for ind in offspring:
                try:
                    ind.fitness = fitness_func(ind.genes)
                except Exception as e:
                    logger.warning(f"Fitness evaluation failed: {e}")
                    ind.fitness = float('-inf') if maximize else float('inf')
            
            # 精英保留
            self._elitism(offspring)
            
            # 更新种群
            self.population = offspring
            
            # 记录历史
            best_fitness = max(ind.fitness for ind in self.population) if maximize else \
                          min(ind.fitness for ind in self.population)
            self.history.append(best_fitness)
            
            if (generation + 1) % 10 == 0:
                logger.info(f"Generation {generation + 1}/{self.generations}, Best: {best_fitness:.4f}")
        
        # 返回最优解
        if maximize:
            self.best_individual = max(self.population, key=lambda x: x.fitness)
        else:
            self.best_individual = min(self.population, key=lambda x: x.fitness)
        
        logger.info(f"Optimization completed. Best fitness: {self.best_individual.fitness:.4f}")
        
        return self.best_individual.genes, self.best_individual.fitness
    
    def _initialize_population(self):
        """初始化种群"""
        self.population = []
        
        for _ in range(self.population_size):
            genes = {}
            for param, (min_val, max_val) in self.param_bounds.items():
                param_type = self.param_types.get(param, 'float')
                
                if param_type == 'int':
                    genes[param] = random.randint(int(min_val), int(max_val))
                else:
                    genes[param] = random.uniform(min_val, max_val)
            
            self.population.append(Individual(genes))
    
    def _evaluate_population(self, fitness_func: Callable):
        """评估种群"""
        for ind in self.population:
            try:
                ind.fitness = fitness_func(ind.genes)
            except Exception as e:
                logger.warning(f"Fitness evaluation failed: {e}")
                ind.fitness = float('-inf')
    
    def _select(self) -> List[Individual]:
        """选择（锦标赛选择）"""
        selected = []
        tournament_size = 3
        
        for _ in range(self.population_size):
            tournament = random.sample(self.population, tournament_size)
            winner = max(tournament, key=lambda x: x.fitness)
            selected.append(winner)
        
        return selected
    
    def _crossover_and_mutate(self, parents: List[Individual]) -> List[Individual]:
        """交叉和变异"""
        offspring = []
        
        for i in range(0, len(parents), 2):
            parent1 = parents[i]
            parent2 = parents[(i + 1) % len(parents)]
            
            # 交叉
            if random.random() < self.crossover_rate:
                child1_genes, child2_genes = self._crossover(parent1.genes, parent2.genes)
            else:
                child1_genes = parent1.genes.copy()
                child2_genes = parent2.genes.copy()
            
            # 变异
            child1_genes = self._mutate(child1_genes)
            child2_genes = self._mutate(child2_genes)
            
            offspring.append(Individual(child1_genes))
            offspring.append(Individual(child2_genes))
        
        return offspring[:self.population_size]
    
    def _crossover(
        self,
        genes1: Dict[str, Any],
        genes2: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """交叉操作"""
        child1 = {}
        child2 = {}
        
        for param in genes1.keys():
            if random.random() < 0.5:
                child1[param] = genes1[param]
                child2[param] = genes2[param]
            else:
                child1[param] = genes2[param]
                child2[param] = genes1[param]
        
        return child1, child2
    
    def _mutate(self, genes: Dict[str, Any]) -> Dict[str, Any]:
        """变异操作"""
        mutated = genes.copy()
        
        for param, value in mutated.items():
            if random.random() < self.mutation_rate:
                min_val, max_val = self.param_bounds[param]
                param_type = self.param_types.get(param, 'float')
                
                if param_type == 'int':
                    mutated[param] = random.randint(int(min_val), int(max_val))
                else:
                    # 高斯变异
                    mutated[param] += random.gauss(0, (max_val - min_val) * 0.1)
                    mutated[param] = max(min_val, min(max_val, mutated[param]))
        
        return mutated
    
    def _elitism(self, offspring: List[Individual]):
        """精英保留"""
        sorted_pop = sorted(self.population, key=lambda x: x.fitness, reverse=True)
        
        for i in range(min(self.elitism, len(offspring))):
            offspring[-(i+1)] = Individual(
                sorted_pop[i].genes.copy(),
                sorted_pop[i].fitness
            )
