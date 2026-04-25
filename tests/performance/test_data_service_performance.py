#!/usr/bin/env python3
"""
数据服务性能测试

测试内容:
- 数据查询性能
- 批量操作性能
- 缓存性能
- 并发性能
"""
import pytest
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
import sys


class TestFinancialDataQueryPerformance:
    """财务数据查询性能测试"""
    
    def test_single_stock_query_performance(self):
        """测试单只股票查询性能"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True
            )
            
            # 准备测试数据 (12个季度)
            df = pd.DataFrame({
                'code': ['000001'] * 12,
                'report_date': pd.date_range('2021-03-31', periods=12, freq='Q').strftime('%Y-%m-%d'),
                'roe': np.random.uniform(5, 25, 12),
                'roa': np.random.uniform(3, 15, 12),
                'gross_margin': np.random.uniform(20, 50, 12),
                'net_margin': np.random.uniform(10, 30, 12),
                'debt_ratio': np.random.uniform(30, 70, 12),
                'current_ratio': np.random.uniform(1, 3, 12)
            })
            
            storage.save_financial_indicators('000001', df)
            
            # 冷启动查询 (从文件)
            start = time.time()
            result1 = storage.load_financial_indicators('000001')
            cold_time = time.time() - start
            
            # 热查询 (从缓存)
            start = time.time()
            result2 = storage.load_financial_indicators('000001')
            hot_time = time.time() - start
            
            print(f"\n单只股票查询性能:")
            print(f"  冷启动: {cold_time*1000:.2f}ms")
            print(f"  缓存: {hot_time*1000:.2f}ms")
            print(f"  缓存加速: {cold_time/hot_time:.1f}x")
            
            # 性能要求
            assert cold_time < 0.1, f"冷启动查询过慢: {cold_time}s"
            assert hot_time < 0.01, f"缓存查询过慢: {hot_time}s"
    
    def test_multi_stock_batch_query_performance(self):
        """测试多只股票批量查询性能"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True
            )
            
            # 准备100只股票的测试数据
            codes = [f'{i:06d}' for i in range(1, 101)]
            
            for code in codes:
                df = pd.DataFrame({
                    'code': [code] * 12,
                    'report_date': pd.date_range('2021-03-31', periods=12, freq='Q').strftime('%Y-%m-%d'),
                    'roe': np.random.uniform(5, 25, 12),
                    'gross_margin': np.random.uniform(20, 50, 12)
                })
                storage.save_financial_indicators(code, df)
            
            # 批量查询
            start = time.time()
            results = storage.batch_load_indicators(codes)
            elapsed = time.time() - start
            
            print(f"\n批量查询性能 (100只股票):")
            print(f"  总耗时: {elapsed*1000:.2f}ms")
            print(f"  平均每只: {elapsed/100*1000:.2f}ms")
            
            # 性能要求: 100只股票应在500ms内完成
            assert elapsed < 0.5, f"批量查询过慢: {elapsed}s"
            assert len(results) == 100


class TestMarketBehaviorQueryPerformance:
    """市场行为数据查询性能测试"""
    
    def test_dragon_tiger_query_performance(self):
        """测试龙虎榜查询性能"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FinancialStorageManager(base_path=Path(tmpdir))
            
            # 准备测试数据 (50只上榜股票)
            df = pd.DataFrame({
                'code': [f'{i:06d}' for i in range(1, 51)],
                'trade_date': ['2024-04-19'] * 50,
                'close_price': np.random.uniform(10, 100, 50),
                'change_pct': np.random.uniform(9, 11, 50),
                'institution_net': np.random.uniform(1000000, 50000000, 50)
            })
            
            storage.save_dragon_tiger('20240419', df)
            
            # 查询性能测试
            start = time.time()
            result = storage.load_dragon_tiger('20240419')
            elapsed = time.time() - start
            
            print(f"\n龙虎榜查询性能:")
            print(f"  耗时: {elapsed*1000:.2f}ms")
            
            assert elapsed < 0.05, f"龙虎榜查询过慢: {elapsed}s"
    
    def test_money_flow_query_performance(self):
        """测试资金流向查询性能"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FinancialStorageManager(base_path=Path(tmpdir))
            
            # 准备测试数据 (5000只股票)
            df = pd.DataFrame({
                'code': [f'{i:06d}' for i in range(1, 5001)],
                'trade_date': ['2024-04-19'] * 5000,
                'main_net_inflow': np.random.uniform(-10000000, 10000000, 5000),
                'retail_net_inflow': np.random.uniform(-5000000, 5000000, 5000)
            })
            
            storage.save_money_flow('20240419', df)
            
            # 查询性能测试
            start = time.time()
            result = storage.load_money_flow('20240419')
            elapsed = time.time() - start
            
            print(f"\n资金流向查询性能 (5000只股票):")
            print(f"  耗时: {elapsed*1000:.2f}ms")
            
            assert elapsed < 0.1, f"资金流向查询过慢: {elapsed}s"


class TestCachePerformance:
    """缓存性能测试"""
    
    def test_cache_hit_performance(self):
        """测试缓存命中性能"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True,
                cache_size=1000
            )
            
            # 准备测试数据
            df = pd.DataFrame({
                'code': ['000001'] * 20,
                'report_date': pd.date_range('2021-03-31', periods=20, freq='Q').strftime('%Y-%m-%d'),
                'roe': np.random.uniform(5, 25, 20)
            })
            
            storage.save_financial_indicators('000001', df)
            
            # 第一次查询 (缓存未命中)
            storage.load_financial_indicators('000001')
            
            # 第二次查询 (缓存命中)
            start = time.time()
            for _ in range(1000):
                storage.load_financial_indicators('000001')
            elapsed = time.time() - start
            
            print(f"\n缓存命中性能 (1000次查询):")
            print(f"  总耗时: {elapsed*1000:.2f}ms")
            print(f"  平均每次: {elapsed/1000*1000:.3f}ms")
            
            # 性能要求: 1000次缓存查询应在100ms内完成
            assert elapsed < 0.1, f"缓存查询过慢: {elapsed}s"
    
    def test_cache_memory_usage(self):
        """测试缓存内存占用"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True,
                cache_size=1000
            )
            
            # 记录初始内存
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            # 加载100只股票到缓存
            for i in range(1, 101):
                code = f'{i:06d}'
                df = pd.DataFrame({
                    'code': [code] * 20,
                    'report_date': pd.date_range('2021-03-31', periods=20, freq='Q').strftime('%Y-%m-%d'),
                    'roe': np.random.uniform(5, 25, 20),
                    'roa': np.random.uniform(3, 15, 20),
                    'gross_margin': np.random.uniform(20, 50, 20),
                    'net_margin': np.random.uniform(10, 30, 20),
                    'debt_ratio': np.random.uniform(30, 70, 20),
                })
                storage.save_financial_indicators(code, df)
                storage.load_financial_indicators(code)  # 加载到缓存
            
            # 记录最终内存
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = final_memory - initial_memory
            
            print(f"\n缓存内存占用 (100只股票):")
            print(f"  初始内存: {initial_memory:.2f}MB")
            print(f"  最终内存: {final_memory:.2f}MB")
            print(f"  增加: {memory_increase:.2f}MB")
            print(f"  平均每只: {memory_increase/100:.2f}MB")
            
            # 内存要求: 平均每只股票不超过1MB
            assert memory_increase / 100 < 1, f"缓存内存占用过高"


class TestConcurrentPerformance:
    """并发性能测试"""
    
    def test_concurrent_query_performance(self):
        """测试并发查询性能"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True
            )
            
            # 准备测试数据
            codes = [f'{i:06d}' for i in range(1, 51)]
            for code in codes:
                df = pd.DataFrame({
                    'code': [code] * 12,
                    'report_date': pd.date_range('2021-03-31', periods=12, freq='Q').strftime('%Y-%m-%d'),
                    'roe': np.random.uniform(5, 25, 12)
                })
                storage.save_financial_indicators(code, df)
            
            # 并发查询函数
            def query_stock(code):
                return storage.load_financial_indicators(code)
            
            # 10线程并发查询
            start = time.time()
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(query_stock, codes))
            elapsed = time.time() - start
            
            print(f"\n并发查询性能 (50只股票, 10线程):")
            print(f"  总耗时: {elapsed*1000:.2f}ms")
            print(f"  平均每只: {elapsed/50*1000:.2f}ms")
            
            assert elapsed < 0.5, f"并发查询过慢: {elapsed}s"
            assert len(results) == 50


class TestDataProcessingPerformance:
    """数据处理性能测试"""
    
    def test_indicator_calculation_performance(self):
        """测试指标计算性能"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        # 准备测试数据 (100只股票, 12个季度)
        data = pd.DataFrame({
            'code': [f'{i:06d}' for i in range(1, 101)] * 12,
            'report_date': list(pd.date_range('2021-03-31', periods=12, freq='Q').strftime('%Y-%m-%d')) * 100,
            'total_assets': np.random.uniform(1000000, 10000000, 1200),
            'total_hldr_eqy_exc_min_int': np.random.uniform(500000, 7000000, 1200),
            'net_profit': np.random.uniform(50000, 1000000, 1200),
            'total_revenue': np.random.uniform(500000, 5000000, 1200),
            'total_cogs': np.random.uniform(300000, 3000000, 1200),
            'total_liab': np.random.uniform(200000, 3000000, 1200),
        })
        
        start = time.time()
        indicators = engine.calculate_all_indicators(data)
        elapsed = time.time() - start
        
        print(f"\n指标计算性能 (100只股票, 12个季度):")
        print(f"  总耗时: {elapsed*1000:.2f}ms")
        print(f"  平均每股: {elapsed/100*1000:.2f}ms")
        
        assert elapsed < 1.0, f"指标计算过慢: {elapsed}s"
        assert len(indicators) == 1200
    
    def test_data_validation_performance(self):
        """测试数据验证性能"""
        from services.data_service.quality.financial.financial_validator import FinancialValidator
        
        validator = FinancialValidator()
        
        # 准备测试数据 (1000条记录)
        data = pd.DataFrame({
            'total_assets': np.random.uniform(1000000, 10000000, 1000),
            'total_liab': np.random.uniform(200000, 3000000, 1000),
            'total_hldr_eqy_exc_min_int': np.random.uniform(500000, 7000000, 1000),
        })
        
        # 确保会计恒等式成立
        data['total_hldr_eqy_exc_min_int'] = data['total_assets'] - data['total_liab']
        
        start = time.time()
        for _ in range(100):
            result = validator.validate_accounting_identity(data)
        elapsed = time.time() - start
        
        print(f"\n数据验证性能 (1000条记录, 100次):")
        print(f"  总耗时: {elapsed*1000:.2f}ms")
        print(f"  平均每次: {elapsed/100*1000:.2f}ms")
        
        assert elapsed < 1.0, f"数据验证过慢: {elapsed}s"


class TestStorageIOPerformance:
    """存储IO性能测试"""
    
    def test_parquet_write_performance(self):
        """测试Parquet写入性能"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 准备测试数据 (5000只股票)
            df = pd.DataFrame({
                'code': [f'{i:06d}' for i in range(1, 5001)],
                'report_date': ['2024-03-31'] * 5000,
                'roe': np.random.uniform(5, 25, 5000),
                'gross_margin': np.random.uniform(20, 50, 5000),
                'net_margin': np.random.uniform(10, 30, 5000),
                'debt_ratio': np.random.uniform(30, 70, 5000),
            })
            
            file_path = Path(tmpdir) / 'test.parquet'
            
            start = time.time()
            df.to_parquet(file_path, compression='zstd')
            write_time = time.time() - start
            
            start = time.time()
            loaded = pd.read_parquet(file_path)
            read_time = time.time() - start
            
            file_size = file_path.stat().st_size / 1024  # KB
            
            print(f"\nParquet IO性能 (5000只股票):")
            print(f"  写入耗时: {write_time*1000:.2f}ms")
            print(f"  读取耗时: {read_time*1000:.2f}ms")
            print(f"  文件大小: {file_size:.2f}KB")
            print(f"  压缩率: {df.memory_usage(deep=True).sum()/1024/file_size:.2f}x")
            
            assert write_time < 0.5, f"写入过慢: {write_time}s"
            assert read_time < 0.2, f"读取过慢: {read_time}s"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
