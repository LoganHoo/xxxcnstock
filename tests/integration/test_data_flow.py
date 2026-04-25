"""
数据流集成测试
测试从数据采集到策略执行的完整数据流
"""
import pytest
import polars as pl
import tempfile
from pathlib import Path
from datetime import datetime, timedelta


class TestDataCollectionFlow:
    """数据采集流程测试"""

    def test_kline_data_collection_pipeline(self):
        """测试K线数据采集流程"""
        # 模拟K线数据采集流程
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / 'kline'
            data_dir.mkdir(exist_ok=True)
            
            # 创建模拟K线数据
            dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                     for i in range(30, 0, -1)]
            
            kline_data = pl.DataFrame({
                'code': ['000001'] * 30,
                'trade_date': dates,
                'open': [10.0 + i * 0.01 for i in range(30)],
                'high': [10.5 + i * 0.01 for i in range(30)],
                'low': [9.5 + i * 0.01 for i in range(30)],
                'close': [10.2 + i * 0.01 for i in range(30)],
                'volume': [1000000] * 30,
                'amount': [10000000.0] * 30
            })
            
            # 保存数据
            output_path = data_dir / '000001.parquet'
            kline_data.write_parquet(output_path)
            
            # 验证数据可读
            loaded_data = pl.read_parquet(output_path)
            assert len(loaded_data) == 30
            assert 'code' in loaded_data.columns
            assert 'close' in loaded_data.columns

    def test_stock_list_update_flow(self):
        """测试股票列表更新流程"""
        with tempfile.TemporaryDirectory() as tmpdir:
            stock_list_path = Path(tmpdir) / 'stock_list.parquet'
            
            # 创建模拟股票列表
            stock_list = pl.DataFrame({
                'code': ['000001', '000002', '000003'],
                'name': ['平安银行', '万科A', '国农科技'],
                'industry': ['银行', '房地产', '科技'],
                'market': ['主板', '主板', '中小板']
            })
            
            stock_list.write_parquet(stock_list_path)
            
            # 验证股票列表可读
            loaded_list = pl.read_parquet(stock_list_path)
            assert len(loaded_list) == 3
            assert 'code' in loaded_list.columns


class TestFactorCalculationFlow:
    """因子计算流程测试"""

    def test_factor_calculation_pipeline(self):
        """测试因子计算流水线"""
        from factors.technical.macd import MacdFactor
        from factors.technical.rsi import RsiFactor
        
        # 准备测试数据
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(50, 0, -1)]
        
        prices = [10.0 + i * 0.05 for i in range(50)]
        
        df = pl.DataFrame({
            'code': ['000001'] * 50,
            'trade_date': dates,
            'open': [p * 0.99 for p in prices],
            'high': [p * 1.02 for p in prices],
            'low': [p * 0.98 for p in prices],
            'close': prices,
            'volume': [1000000] * 50
        })
        
        # 计算多个因子
        macd_factor = MacdFactor()
        rsi_factor = RsiFactor()
        
        df = macd_factor.calculate(df)
        df = rsi_factor.calculate(df)
        
        # 验证因子列存在
        assert 'factor_macd' in df.columns
        assert 'factor_rsi' in df.columns
        assert 'macd' in df.columns
        assert 'rsi' in df.columns

    def test_multi_stock_factor_calculation(self):
        """测试多股票因子计算"""
        from factors.technical.ma_trend import MaTrendFactor
        
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                 for i in range(60, 0, -1)]
        
        # 创建多股票数据
        codes = ['000001', '000002', '000003']
        data_list = []
        
        for code in codes:
            prices = [10.0 + i * 0.03 for i in range(60)]
            df = pl.DataFrame({
                'code': [code] * 60,
                'trade_date': dates,
                'open': [p * 0.99 for p in prices],
                'high': [p * 1.02 for p in prices],
                'low': [p * 0.98 for p in prices],
                'close': prices,
                'volume': [1000000] * 60
            })
            data_list.append(df)
        
        all_data = pl.concat(data_list)
        
        # 按股票分组计算因子
        factor = MaTrendFactor()
        results = []
        
        for code in codes:
            stock_data = all_data.filter(pl.col('code') == code)
            result = factor.calculate(stock_data)
            results.append(result)
        
        final_result = pl.concat(results)
        
        assert 'factor_ma_trend' in final_result.columns
        assert len(final_result.filter(pl.col('code') == '000001')) == 60


class TestStrategyExecutionFlow:
    """策略执行流程测试"""

    def test_strategy_selection_flow(self):
        """测试策略选股流程"""
        from services.stock_service.scorer import StockScorer
        
        scorer = StockScorer()
        
        # 模拟多只股票评分
        stocks = [
            {'code': '000001', 'name': '平安银行', 'fundamental': 80, 'technical': 70},
            {'code': '000002', 'name': '万科A', 'fundamental': 75, 'technical': 65},
            {'code': '000003', 'name': '国农科技', 'fundamental': 60, 'technical': 85},
        ]
        
        scores = []
        for stock in stocks:
            score_obj = scorer.create_score(
                code=stock['code'],
                name=stock['name'],
                fundamental_score=stock['fundamental'],
                volume_price_score=stock['technical'],
                fund_flow_score=70.0,
                sentiment_score=65.0
            )
            scores.append(score_obj)
        
        # 按总分排序
        scores.sort(key=lambda x: x.total_score, reverse=True)
        
        # 选择前N名
        top_n = scores[:2]
        
        assert len(top_n) == 2
        assert top_n[0].total_score >= top_n[1].total_score

    def test_filter_pipeline(self):
        """测试筛选器流水线"""
        # 创建测试数据
        df = pl.DataFrame({
            'code': ['000001', '000002', '000003', '000004', '000005'],
            'name': ['股票A', '股票B', '股票C', '股票D', '股票E'],
            'price': [10.0, 20.0, 5.0, 100.0, 50.0],
            'market_cap': [100e8, 200e8, 50e8, 500e8, 150e8],
            'pe': [10.0, 20.0, 50.0, 15.0, 30.0],
            'score': [85.0, 75.0, 60.0, 90.0, 70.0]
        })
        
        # 应用多个筛选条件
        # 1. 价格筛选
        df = df.filter(pl.col('price') >= 10.0)
        assert len(df) == 4  # 排除价格为5的股票
        
        # 2. 市值筛选
        df = df.filter(pl.col('market_cap') >= 100e8)
        assert len(df) == 4  # 排除小市值股票(50e8的000003被排除)
        
        # 3. 评分筛选
        df = df.filter(pl.col('score') >= 70.0)
        assert len(df) == 4  # 只保留高分股票(90, 85, 75, 70)


class TestDataPersistenceFlow:
    """数据持久化流程测试"""

    def test_parquet_read_write_flow(self):
        """测试Parquet读写流程"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 创建测试数据
            df = pl.DataFrame({
                'code': ['000001', '000002'],
                'score': [85.0, 75.0],
                'grade': ['S', 'A'],
                'update_time': [datetime.now(), datetime.now()]
            })
            
            # 写入Parquet
            output_path = Path(tmpdir) / 'test_data.parquet'
            df.write_parquet(output_path)
            
            # 读取Parquet
            loaded_df = pl.read_parquet(output_path)
            
            assert len(loaded_df) == 2
            assert loaded_df['code'].to_list() == ['000001', '000002']
            assert loaded_df['score'].to_list() == [85.0, 75.0]

    def test_incremental_data_update(self):
        """测试增量数据更新"""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / 'incremental_data.parquet'
            
            # 初始数据
            initial_data = pl.DataFrame({
                'code': ['000001', '000002'],
                'score': [80.0, 70.0],
                'date': ['2024-01-01', '2024-01-01']
            })
            initial_data.write_parquet(data_path)
            
            # 新数据
            new_data = pl.DataFrame({
                'code': ['000003', '000004'],
                'score': [85.0, 75.0],
                'date': ['2024-01-02', '2024-01-02']
            })
            
            # 读取旧数据并合并
            old_data = pl.read_parquet(data_path)
            merged_data = pl.concat([old_data, new_data])
            
            # 保存合并后的数据
            merged_data.write_parquet(data_path)
            
            # 验证
            final_data = pl.read_parquet(data_path)
            assert len(final_data) == 4


class TestErrorHandlingFlow:
    """错误处理流程测试"""

    def test_missing_data_handling(self):
        """测试缺失数据处理"""
        # 创建包含缺失值的数据
        df = pl.DataFrame({
            'code': ['000001', '000002', '000003'],
            'score': [85.0, None, 75.0],
            'price': [10.0, 20.0, None]
        })
        
        # 过滤掉包含None的行
        df_clean = df.filter(
            pl.col('score').is_not_null() & 
            pl.col('price').is_not_null()
        )
        
        assert len(df_clean) == 1
        assert df_clean['code'][0] == '000001'

    def test_invalid_data_handling(self):
        """测试无效数据处理"""
        df = pl.DataFrame({
            'code': ['000001', '000002', '000003'],
            'price': [10.0, -5.0, 20.0],  # 负价格无效
            'volume': [1000000, 2000000, -1000]  # 负成交量无效
        })
        
        # 过滤无效数据
        df_valid = df.filter(
            (pl.col('price') > 0) &
            (pl.col('volume') > 0)
        )

        assert len(df_valid) == 1  # 只有000001完全有效


class TestEndToEndFlow:
    """端到端流程测试"""

    def test_complete_analysis_pipeline(self):
        """测试完整分析流水线"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. 准备原始数据
            dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') 
                     for i in range(30, 0, -1)]
            
            kline_data = pl.DataFrame({
                'code': ['000001'] * 30,
                'trade_date': dates,
                'open': [10.0 + i * 0.02 for i in range(30)],
                'high': [10.5 + i * 0.02 for i in range(30)],
                'low': [9.5 + i * 0.02 for i in range(30)],
                'close': [10.2 + i * 0.02 for i in range(30)],
                'volume': [1000000] * 30
            })
            
            # 2. 计算因子
            from factors.technical.macd import MacdFactor
            from factors.technical.rsi import RsiFactor
            
            macd_factor = MacdFactor()
            rsi_factor = RsiFactor()
            
            kline_data = macd_factor.calculate(kline_data)
            kline_data = rsi_factor.calculate(kline_data)
            
            # 3. 评分
            from services.stock_service.scorer import StockScorer
            
            scorer = StockScorer()
            score_obj = scorer.create_score(
                code='000001',
                name='平安银行',
                fundamental_score=80.0,
                volume_price_score=kline_data['factor_macd'].tail(1).item(),
                fund_flow_score=70.0,
                sentiment_score=65.0
            )
            
            # 4. 保存结果
            result_df = pl.DataFrame({
                'code': [score_obj.code],
                'name': [score_obj.name],
                'total_score': [score_obj.total_score],
                'fundamental_score': [score_obj.fundamental_score],
                'volume_price_score': [score_obj.volume_price_score],
                'fund_flow_score': [score_obj.fund_flow_score],
                'sentiment_score': [score_obj.sentiment_score]
            })
            
            result_path = Path(tmpdir) / 'analysis_result.parquet'
            result_df.write_parquet(result_path)
            
            # 5. 验证结果
            loaded_result = pl.read_parquet(result_path)
            assert len(loaded_result) == 1
            assert loaded_result['code'][0] == '000001'
            assert loaded_result['total_score'][0] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
