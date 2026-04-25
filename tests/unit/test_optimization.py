#!/usr/bin/env python3
"""
参数优化模块测试
"""
import pytest
import numpy as np


class TestGridSearchOptimizer:
    """网格搜索优化器测试"""
    
    def test_optimizer_creation(self):
        """测试优化器创建"""
        from services.backtest_service.optimization.grid_search import GridSearchOptimizer
        
        param_grid = {
            'param1': [1, 2, 3],
            'param2': [0.1, 0.2]
        }
        
        optimizer = GridSearchOptimizer(param_grid, n_jobs=1)
        assert optimizer is not None
        assert len(optimizer.param_grid) == 2
    
    def test_simple_search(self):
        """测试简单搜索"""
        from services.backtest_service.optimization.grid_search import GridSearchOptimizer
        
        param_grid = {
            'x': [1, 2, 3],
            'y': [1, 2]
        }
        
        optimizer = GridSearchOptimizer(param_grid, n_jobs=1)
        
        # 目标函数: f(x, y) = x + y
        def objective(params):
            return params['x'] + params['y']
        
        best_params, best_score = optimizer.search(objective, maximize=True)
        
        assert best_params['x'] == 3
        assert best_params['y'] == 2
        assert best_score == 5
    
    def test_get_top_k(self):
        """测试获取前K个结果"""
        from services.backtest_service.optimization.grid_search import GridSearchOptimizer
        
        param_grid = {
            'x': [1, 2],
            'y': [1, 2]
        }
        
        optimizer = GridSearchOptimizer(param_grid, n_jobs=1)
        
        def objective(params):
            return params['x'] + params['y']
        
        optimizer.search(objective, maximize=True)
        top_k = optimizer.get_top_k(k=2)
        
        assert len(top_k) == 2
        assert top_k[0][1] >= top_k[1][1]


class TestGeneticAlgorithmOptimizer:
    """遗传算法优化器测试"""
    
    def test_optimizer_creation(self):
        """测试优化器创建"""
        from services.backtest_service.optimization.genetic_algorithm import GeneticAlgorithmOptimizer
        
        param_bounds = {
            'x': (0, 10),
            'y': (0, 10)
        }
        param_types = {
            'x': 'float',
            'y': 'float'
        }
        
        optimizer = GeneticAlgorithmOptimizer(
            param_bounds=param_bounds,
            param_types=param_types,
            population_size=10,
            generations=5
        )
        
        assert optimizer is not None
    
    def test_simple_optimization(self):
        """测试简单优化"""
        from services.backtest_service.optimization.genetic_algorithm import GeneticAlgorithmOptimizer
        
        param_bounds = {
            'x': (0, 10),
        }
        param_types = {
            'x': 'float',
        }
        
        optimizer = GeneticAlgorithmOptimizer(
            param_bounds=param_bounds,
            param_types=param_types,
            population_size=10,
            generations=10
        )
        
        # 目标函数: f(x) = -(x-5)^2 + 25，最大值在x=5
        def fitness(params):
            x = params['x']
            return -(x - 5) ** 2 + 25
        
        best_params, best_fitness = optimizer.optimize(fitness, maximize=True)
        
        # 最优解应该接近x=5
        assert 4 <= best_params['x'] <= 6
        assert best_fitness > 20
    
    def test_integer_parameters(self):
        """测试整数参数"""
        from services.backtest_service.optimization.genetic_algorithm import GeneticAlgorithmOptimizer
        
        param_bounds = {
            'n': (1, 10),
        }
        param_types = {
            'n': 'int',
        }
        
        optimizer = GeneticAlgorithmOptimizer(
            param_bounds=param_bounds,
            param_types=param_types,
            population_size=20,
            generations=30
        )
        
        def fitness(params):
            n = params['n']
            return -abs(n - 7)  # 最优解是n=7
        
        best_params, best_fitness = optimizer.optimize(fitness, maximize=True)
        
        assert isinstance(best_params['n'], int)
        assert abs(best_params['n'] - 7) <= 1  # 允许误差1
