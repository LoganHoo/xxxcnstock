#!/usr/bin/env python3
"""
网格搜索参数优化
穷举搜索最优参数组合
"""
import itertools
import logging
from typing import Dict, Any, List, Callable, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np

logger = logging.getLogger(__name__)


class GridSearchOptimizer:
    """
    网格搜索参数优化器
    
    通过穷举所有参数组合找到最优解
    """
    
    def __init__(self, param_grid: Dict[str, List[Any]], n_jobs: int = -1):
        """
        初始化
        
        Args:
            param_grid: 参数网格 {param_name: [values]}
            n_jobs: 并行进程数，-1表示使用所有CPU
        """
        self.param_grid = param_grid
        self.n_jobs = n_jobs if n_jobs > 0 else None
        self.results = []
    
    def search(
        self,
        objective_func: Callable,
        maximize: bool = True
    ) -> Tuple[Dict[str, Any], float]:
        """
        执行网格搜索
        
        Args:
            objective_func: 目标函数，接收参数字典，返回评估值
            maximize: 是否最大化目标函数
        
        Returns:
            (最优参数, 最优值)
        """
        # 生成所有参数组合
        param_names = list(self.param_grid.keys())
        param_values = list(self.param_grid.values())
        
        combinations = list(itertools.product(*param_values))
        total = len(combinations)
        
        logger.info(f"Grid search: {total} combinations to evaluate")
        
        # 评估所有组合
        self.results = []
        
        if self.n_jobs and self.n_jobs != 1:
            # 并行评估
            with ProcessPoolExecutor(max_workers=self.n_jobs) as executor:
                futures = {
                    executor.submit(self._evaluate, combo, param_names, objective_func): combo
                    for combo in combinations
                }
                
                for i, future in enumerate(as_completed(futures)):
                    combo, score = future.result()
                    self.results.append((combo, score))
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"Progress: {i+1}/{total}")
        else:
            # 串行评估
            for i, combo in enumerate(combinations):
                combo_dict, score = self._evaluate(combo, param_names, objective_func)
                self.results.append((combo_dict, score))
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{total}")
        
        # 找到最优解
        best_idx = np.argmax([r[1] for r in self.results]) if maximize else \
                   np.argmin([r[1] for r in self.results])
        
        best_params, best_score = self.results[best_idx]
        
        logger.info(f"Best params: {best_params}, Score: {best_score:.4f}")
        
        return best_params, best_score
    
    def _evaluate(
        self,
        combo: Tuple,
        param_names: List[str],
        objective_func: Callable
    ) -> Tuple[Dict[str, Any], float]:
        """评估单个参数组合"""
        params = dict(zip(param_names, combo))
        
        try:
            score = objective_func(params)
        except Exception as e:
            logger.warning(f"Evaluation failed for {params}: {e}")
            score = float('-inf')
        
        return params, score
    
    def get_top_k(self, k: int = 10) -> List[Tuple[Dict[str, Any], float]]:
        """获取前K个最优结果"""
        sorted_results = sorted(self.results, key=lambda x: x[1], reverse=True)
        return sorted_results[:k]


def create_param_grid(
    base_params: Dict[str, Any],
    ranges: Dict[str, Tuple[float, float, int]]
) -> Dict[str, List[Any]]:
    """
    创建参数网格
    
    Args:
        base_params: 基础参数值
        ranges: 参数范围 {param: (min, max, num)}
    
    Returns:
        参数网格
    """
    param_grid = {}
    
    for param, (min_val, max_val, num) in ranges.items():
        if isinstance(min_val, int) and isinstance(max_val, int):
            param_grid[param] = list(range(min_val, max_val + 1, 
                                          (max_val - min_val) // (num - 1) or 1))
        else:
            param_grid[param] = list(np.linspace(min_val, max_val, num))
    
    # 添加固定参数
    for param, value in base_params.items():
        if param not in param_grid:
            param_grid[param] = [value]
    
    return param_grid
