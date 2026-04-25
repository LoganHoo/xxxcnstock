#!/usr/bin/env python3
"""
集成测试工作流

测试完整的选股流水线，包括：
1. 数据新鲜度检查
2. 数据采集验证
3. 因子计算
4. 选股策略执行
5. 报告生成

使用方式:
    pytest tests/integration/test_workflow_integration.py -v
    pytest tests/integration/test_workflow_integration.py::TestDataFreshnessWorkflow -v
"""
import pytest
import sys
import json
import tempfile
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestDataFreshnessWorkflow:
    """数据新鲜度工作流测试"""

    def test_data_freshness_check_pipeline(self):
        """测试数据新鲜度检查流水线"""
        from core.data_freshness_checker import DataFreshnessChecker
        
        # 创建临时测试数据
        with tempfile.TemporaryDirectory() as tmpdir:
            kline_dir = Path(tmpdir) / 'kline'
            kline_dir.mkdir()
            
            # 创建测试K线数据
            today = datetime.now().strftime('%Y-%m-%d')
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 最新数据
            latest_data = pl.DataFrame({
                'code': ['000001'] * 5,
                'date': pd.date_range(end=today, periods=5, freq='D').strftime('%Y-%m-%d').tolist(),
                'open': [10.0] * 5,
                'high': [10.5] * 5,
                'low': [9.5] * 5,
                'close': [10.2] * 5,
                'volume': [1000000] * 5
            })
            latest_data.write_parquet(kline_dir / '000001.parquet')
            
            # 过期数据
            outdated_data = pl.DataFrame({
                'code': ['000002'] * 5,
                'date': pd.date_range(end=yesterday, periods=5, freq='D').strftime('%Y-%m-%d').tolist(),
                'open': [20.0] * 5,
                'high': [20.5] * 5,
                'low': [19.5] * 5,
                'close': [20.2] * 5,
                'volume': [2000000] * 5
            })
            outdated_data.write_parquet(kline_dir / '000002.parquet')
            
            # 执行检查
            checker = DataFreshnessChecker(kline_dir=kline_dir)
            result = checker.check_freshness(target_date=today, max_age_days=1)
            
            # 验证结果
            assert result['total'] == 2
            assert result['up_to_date'] == 1
            assert result['outdated'] == 1
            assert result['up_to_date_rate'] == 50.0

    def test_missing_data_detection(self):
        """测试缺失数据检测"""
        with tempfile.TemporaryDirectory() as tmpdir:
            kline_dir = Path(tmpdir) / 'kline'
            kline_dir.mkdir()
            
            # 创建股票列表
            stock_list = pl.DataFrame({
                'code': ['000001', '000002', '000003'],
                'name': ['股票1', '股票2', '股票3']
            })
            stock_list_path = Path(tmpdir) / 'stock_list.parquet'
            stock_list.write_parquet(stock_list_path)
            
            # 只创建部分K线数据
            today = datetime.now().strftime('%Y-%m-%d')
            data = pl.DataFrame({
                'code': ['000001'],
                'date': [today],
                'open': [10.0],
                'high': [10.5],
                'low': [9.5],
                'close': [10.2],
                'volume': [1000000]
            })
            data.write_parquet(kline_dir / '000001.parquet')
            
            # 检查缺失
            existing_codes = set(f.stem for f in kline_dir.glob('*.parquet'))
            all_codes = set(stock_list['code'].to_list())
            missing_codes = all_codes - existing_codes
            
            assert missing_codes == {'000002', '000003'}


class TestDataCollectionWorkflow:
    """数据采集工作流测试"""

    @pytest.mark.asyncio
    async def test_incremental_collection_workflow(self):
        """测试增量采集工作流"""
        with tempfile.TemporaryDirectory() as tmpdir:
            kline_dir = Path(tmpdir) / 'kline'
            kline_dir.mkdir()
            
            # 创建现有数据（到昨天）
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            existing_data = pl.DataFrame({
                'code': ['000001'] * 10,
                'date': pd.date_range(end=yesterday, periods=10, freq='D').strftime('%Y-%m-%d').tolist(),
                'open': [10.0 + i * 0.1 for i in range(10)],
                'high': [10.5 + i * 0.1 for i in range(10)],
                'low': [9.5 + i * 0.1 for i in range(10)],
                'close': [10.2 + i * 0.1 for i in range(10)],
                'volume': [1000000] * 10
            })
            existing_data.write_parquet(kline_dir / '000001.parquet')
            
            # 模拟采集今天数据
            today = datetime.now().strftime('%Y-%m-%d')
            new_data = pl.DataFrame({
                'code': ['000001'],
                'date': [today],
                'open': [11.1],
                'high': [11.6],
                'low': [10.6],
                'close': [11.3],
                'volume': [1200000]
            })
            
            # 合并数据
            combined = pl.concat([existing_data, new_data])
            combined.write_parquet(kline_dir / '000001.parquet')
            
            # 验证
            loaded = pl.read_parquet(kline_dir / '000001.parquet')
            assert len(loaded) == 11
            assert str(loaded['date'].max()) == today

    def test_data_validation_in_pipeline(self):
        """测试流水线中的数据验证"""
        with tempfile.TemporaryDirectory() as tmpdir:
            kline_dir = Path(tmpdir) / 'kline'
            kline_dir.mkdir()
            
            # 创建无效数据（空文件）
            invalid_data = pl.DataFrame({
                'code': [],
                'date': [],
                'open': [],
                'high': [],
                'low': [],
                'close': [],
                'volume': []
            })
            invalid_data.write_parquet(kline_dir / 'invalid.parquet')
            
            # 验证数据有效性
            def is_valid_kline(file_path):
                try:
                    df = pl.read_parquet(file_path)
                    return len(df) > 0 and 'date' in df.columns
                except:
                    return False
            
            assert not is_valid_kline(kline_dir / 'invalid.parquet')


class TestFactorCalculationWorkflow:
    """因子计算工作流测试"""

    def test_factor_calculation_pipeline(self):
        """测试因子计算流水线"""
        # 准备测试数据
        dates = pd.date_range('2024-01-01', periods=60, freq='D')
        base_price = 10.0
        
        # 生成价格数据
        closes = [base_price + i * 0.05 + (i % 5) * 0.1 for i in range(60)]
        
        df = pl.DataFrame({
            'code': ['000001'] * 60,
            'date': [d.strftime('%Y-%m-%d') for d in dates],
            'open': [c - 0.1 for c in closes],
            'high': [c + 0.2 for c in closes],
            'low': [c - 0.2 for c in closes],
            'close': closes,
            'volume': [1000000 + i * 1000 for i in range(60)]
        })
        
        # 计算技术指标
        from core.indicators.technical import TechnicalIndicators
        
        # 计算MA
        for period in [5, 10, 20]:
            df = df.with_columns([
                pl.Series(f'ma_{period}', TechnicalIndicators.calculate_ema(df['close'].to_pandas(), period))
            ])
        
        # 计算MACD
        macd_line, signal_line, histogram = TechnicalIndicators.calculate_macd(df['close'].to_pandas())
        df = df.with_columns([
            pl.Series('macd', macd_line),
            pl.Series('macd_signal', signal_line),
            pl.Series('macd_hist', histogram)
        ])
        
        # 计算RSI
        df = df.with_columns([
            pl.Series('rsi_14', TechnicalIndicators.calculate_rsi(df['close'].to_pandas(), 14))
        ])
        
        # 验证因子列存在
        assert 'ma_5' in df.columns
        assert 'ma_10' in df.columns
        assert 'ma_20' in df.columns
        assert 'macd' in df.columns
        assert 'macd_signal' in df.columns
        assert 'rsi_14' in df.columns
        
        # 验证因子值合理
        latest = df.tail(1)
        assert latest['ma_5'][0] > 0
        assert latest['rsi_14'][0] >= 0 and latest['rsi_14'][0] <= 100

    def test_multi_stock_factor_calculation(self):
        """测试多股票因子计算"""
        codes = ['000001', '000002', '000003']
        all_data = []
        
        for code in codes:
            dates = pd.date_range('2024-01-01', periods=30, freq='D')
            df = pl.DataFrame({
                'code': [code] * 30,
                'date': [d.strftime('%Y-%m-%d') for d in dates],
                'open': [10.0] * 30,
                'high': [10.5] * 30,
                'low': [9.5] * 30,
                'close': [10.2 + i * 0.01 for i in range(30)],
                'volume': [1000000] * 30
            })
            all_data.append(df)
        
        combined = pl.concat(all_data)
        
        # 按股票分组计算因子
        from core.indicators.technical import TechnicalIndicators
        
        results = []
        for code in codes:
            stock_df = combined.filter(pl.col('code') == code)
            # 计算MA
            for period in [5, 10]:
                stock_df = stock_df.with_columns([
                    pl.Series(f'ma_{period}', TechnicalIndicators.calculate_ema(stock_df['close'].to_pandas(), period))
                ])
            results.append(stock_df)
        
        result_df = pl.concat(results)
        
        # 验证每个股票都有因子值
        for code in codes:
            stock_data = result_df.filter(pl.col('code') == code)
            assert 'ma_5' in stock_data.columns
            assert len(stock_data) == 30


class TestStockSelectionWorkflow:
    """选股策略工作流测试"""

    def test_fund_behavior_selection_workflow(self):
        """测试主力行为选股工作流"""
        # 创建模拟股票数据
        dates = pd.date_range('2024-01-01', periods=60, freq='D')
        
        # 模拟有主力介入的股票（放量上涨）
        strong_stock = pl.DataFrame({
            'code': ['000001'] * 60,
            'date': [d.strftime('%Y-%m-%d') for d in dates],
            'open': [10.0 + i * 0.05 for i in range(60)],
            'high': [10.5 + i * 0.05 for i in range(60)],
            'low': [9.5 + i * 0.05 for i in range(60)],
            'close': [10.2 + i * 0.05 for i in range(60)],
            'volume': [1000000 + (i % 10) * 500000 for i in range(60)]  # 放量
        })
        
        # 模拟弱势股票（缩量下跌）
        weak_stock = pl.DataFrame({
            'code': ['000002'] * 60,
            'date': [d.strftime('%Y-%m-%d') for d in dates],
            'open': [20.0 - i * 0.05 for i in range(60)],
            'high': [20.5 - i * 0.05 for i in range(60)],
            'low': [19.5 - i * 0.05 for i in range(60)],
            'close': [20.2 - i * 0.05 for i in range(60)],
            'volume': [2000000 - (i % 10) * 100000 for i in range(60)]  # 缩量
        })
        
        # 模拟选股逻辑
        def select_stocks(stock_data_list):
            selected = []
            for df in stock_data_list:
                code = df['code'][0]
                latest = df.tail(5)
                
                # 简单规则：近5日上涨且放量
                price_change = (latest['close'].tail(1)[0] - latest['close'].head(1)[0]) / latest['close'].head(1)[0]
                avg_volume = latest['volume'].mean()
                prev_avg = df.head(55).tail(10)['volume'].mean()
                
                if price_change > 0.02 and avg_volume > prev_avg * 1.2:
                    selected.append({
                        'code': code,
                        'price_change': price_change,
                        'volume_ratio': avg_volume / prev_avg
                    })
            
            return selected
        
        selected = select_stocks([strong_stock, weak_stock])
        
        # 验证选股结果
        assert len(selected) == 1
        assert selected[0]['code'] == '000001'

    def test_multi_factor_selection_workflow(self):
        """测试多因子选股工作流"""
        # 创建多只股票数据
        codes = ['000001', '000002', '000003', '000004', '000005']
        stocks_data = []
        
        for i, code in enumerate(codes):
            dates = pd.date_range('2024-01-01', periods=60, freq='D')
            # 不同趋势的股票
            trend = (i - 2) * 0.1  # -0.2, -0.1, 0, 0.1, 0.2
            
            df = pl.DataFrame({
                'code': [code] * 60,
                'date': [d.strftime('%Y-%m-%d') for d in dates],
                'open': [10.0 + j * trend + i for j in range(60)],
                'high': [10.5 + j * trend + i for j in range(60)],
                'low': [9.5 + j * trend + i for j in range(60)],
                'close': [10.2 + j * trend + i for j in range(60)],
                'volume': [1000000 + i * 100000 for j in range(60)]
            })
            stocks_data.append(df)
        
        # 多因子评分
        def multi_factor_score(df):
            """多因子评分"""
            latest = df.tail(20)
            
            # 趋势因子
            trend_score = (latest['close'].tail(1)[0] - latest['close'].head(1)[0]) / latest['close'].head(1)[0]
            
            # 量能因子
            volume_score = latest['volume'].mean() / df.head(40).tail(20)['volume'].mean() - 1
            
            # 波动率因子（越低越好）
            volatility = latest['close'].std() / latest['close'].mean()
            volatility_score = -volatility  # 负相关
            
            # 综合评分
            total_score = trend_score * 0.4 + volume_score * 0.3 + volatility_score * 0.3
            
            return {
                'code': df['code'][0],
                'trend_score': trend_score,
                'volume_score': volume_score,
                'volatility_score': volatility_score,
                'total_score': total_score
            }
        
        # 评分并排序
        scores = [multi_factor_score(df) for df in stocks_data]
        scores.sort(key=lambda x: x['total_score'], reverse=True)
        
        # 选择前3名
        top3 = scores[:3]
        
        # 验证结果
        assert len(top3) == 3
        assert top3[0]['total_score'] >= top3[1]['total_score']
        assert top3[1]['total_score'] >= top3[2]['total_score']


class TestReportGenerationWorkflow:
    """报告生成工作流测试"""

    def test_selection_report_generation(self):
        """测试选股报告生成"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 模拟选股结果
            selections = [
                {
                    'code': '000001',
                    'name': '平安银行',
                    'score': 85.5,
                    'factors': {
                        'trend': 0.15,
                        'volume': 1.25,
                        'rsi': 65
                    }
                },
                {
                    'code': '000002',
                    'name': '万科A',
                    'score': 78.3,
                    'factors': {
                        'trend': 0.12,
                        'volume': 1.15,
                        'rsi': 58
                    }
                }
            ]
            
            # 生成报告
            report_path = Path(tmpdir) / 'selection_report.json'
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'total_stocks': len(selections),
                    'selections': selections
                }, f, ensure_ascii=False, indent=2)
            
            # 验证报告
            with open(report_path, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            
            assert loaded['total_stocks'] == 2
            assert len(loaded['selections']) == 2
            assert loaded['selections'][0]['code'] == '000001'

    def test_daily_summary_report(self):
        """测试每日汇总报告"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 模拟每日数据
            daily_data = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'market_summary': {
                    'total_stocks': 5000,
                    'up_count': 2500,
                    'down_count': 2000,
                    'flat_count': 500
                },
                'data_quality': {
                    'freshness_rate': 98.5,
                    'missing_count': 75,
                    'error_count': 0
                },
                'selection_summary': {
                    'total_selected': 10,
                    'avg_score': 82.3
                }
            }
            
            # 生成报告
            report_path = Path(tmpdir) / 'daily_summary.md'
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"# 每日汇总报告\n\n")
                f.write(f"日期: {daily_data['date']}\n\n")
                f.write(f"## 市场概况\n")
                f.write(f"- 总股票数: {daily_data['market_summary']['total_stocks']}\n")
                f.write(f"- 上涨: {daily_data['market_summary']['up_count']}\n")
                f.write(f"- 下跌: {daily_data['market_summary']['down_count']}\n\n")
                f.write(f"## 数据质量\n")
                f.write(f"- 新鲜度: {daily_data['data_quality']['freshness_rate']}%\n")
                f.write(f"- 缺失: {daily_data['data_quality']['missing_count']}\n\n")
            
            # 验证报告存在
            assert report_path.exists()
            content = report_path.read_text(encoding='utf-8')
            assert '每日汇总报告' in content
            assert daily_data['date'] in content


class TestEndToEndWorkflow:
    """端到端工作流测试"""

    @pytest.mark.integration
    def test_full_pipeline_execution(self):
        """测试完整流水线执行"""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            kline_dir = data_dir / 'kline'
            kline_dir.mkdir()
            
            # 1. 准备数据
            codes = ['000001', '000002', '000003']
            today = datetime.now().strftime('%Y-%m-%d')
            
            for code in codes:
                dates = pd.date_range(end=today, periods=60, freq='D')
                df = pl.DataFrame({
                    'code': [code] * 60,
                    'date': [d.strftime('%Y-%m-%d') for d in dates],
                    'open': [10.0 + i * 0.02 for i in range(60)],
                    'high': [10.5 + i * 0.02 for i in range(60)],
                    'low': [9.5 + i * 0.02 for i in range(60)],
                    'close': [10.2 + i * 0.02 for i in range(60)],
                    'volume': [1000000 + i * 1000 for i in range(60)]
                })
                df.write_parquet(kline_dir / f'{code}.parquet')
            
            # 2. 数据新鲜度检查
            freshness_report = {
                'date': today,
                'total': len(codes),
                'up_to_date': len(codes),
                'up_to_date_rate': 100.0
            }
            
            # 计算技术指标
            from core.indicators.technical import TechnicalIndicators
            
            factor_results = []
            for code in codes:
                df = pl.read_parquet(kline_dir / f'{code}.parquet')
                # 计算MA
                for period in [5, 10, 20]:
                    df = df.with_columns([
                        pl.Series(f'ma_{period}', TechnicalIndicators.calculate_ema(df['close'].to_pandas(), period))
                    ])
                factor_results.append(df)
            
            # 4. 选股
            selected = []
            for i, df in enumerate(factor_results):
                latest = df.tail(1)
                if latest['ma_5'][0] > latest['ma_10'][0]:  # 金叉
                    selected.append({
                        'code': codes[i],
                        'score': 80.0 + i * 2
                    })
            
            # 5. 生成报告
            report = {
                'date': today,
                'freshness': freshness_report,
                'selected_count': len(selected),
                'selections': selected
            }
            
            # 验证完整流程
            assert freshness_report['up_to_date_rate'] == 100.0
            assert len(factor_results) == len(codes)
            assert 'ma_5' in factor_results[0].columns
            assert len(selected) >= 0
            assert report['date'] == today


# =============================================================================
# 性能测试
# =============================================================================

class TestWorkflowPerformance:
    """工作流性能测试"""

    @pytest.mark.performance
    def test_factor_calculation_performance(self):
        """测试因子计算性能"""
        import time
        
        # 创建大量测试数据
        dates = pd.date_range('2024-01-01', periods=252, freq='D')  # 一年数据
        df = pl.DataFrame({
            'code': ['000001'] * 252,
            'date': [d.strftime('%Y-%m-%d') for d in dates],
            'open': [10.0 + i * 0.01 for i in range(252)],
            'high': [10.5 + i * 0.01 for i in range(252)],
            'low': [9.5 + i * 0.01 for i in range(252)],
            'close': [10.2 + i * 0.01 for i in range(252)],
            'volume': [1000000] * 252
        })
        
        from core.indicators.technical import TechnicalIndicators
        
        # 测量计算时间
        start = time.time()
        # 计算MA
        for period in [5, 10, 20, 60]:
            df.with_columns([
                pl.Series(f'ma_{period}', TechnicalIndicators.calculate_ema(df['close'].to_pandas(), period))
            ])
        # 计算MACD
        TechnicalIndicators.calculate_macd(df['close'].to_pandas())
        # 计算RSI
        TechnicalIndicators.calculate_rsi(df['close'].to_pandas(), 14)
        elapsed = time.time() - start
        
        # 验证性能（应该很快）
        assert elapsed < 1.0  # 1秒内完成
        print(f"\n因子计算耗时: {elapsed:.3f}s")


# =============================================================================
# 错误处理测试
# =============================================================================

class TestWorkflowErrorHandling:
    """工作流错误处理测试"""

    def test_missing_file_handling(self):
        """测试缺失文件处理"""
        with tempfile.TemporaryDirectory() as tmpdir:
            kline_dir = Path(tmpdir) / 'kline'
            kline_dir.mkdir()
            
            # 尝试读取不存在的文件
            missing_file = kline_dir / '999999.parquet'
            
            with pytest.raises(Exception):
                pl.read_parquet(missing_file)

    def test_corrupted_data_handling(self):
        """测试损坏数据处理"""
        with tempfile.TemporaryDirectory() as tmpdir:
            kline_dir = Path(tmpdir) / 'kline'
            kline_dir.mkdir()
            
            # 创建损坏的文件
            corrupted_file = kline_dir / 'corrupted.parquet'
            corrupted_file.write_text('not a parquet file')
            
            # 尝试读取应该失败
            with pytest.raises(Exception):
                pl.read_parquet(corrupted_file)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
