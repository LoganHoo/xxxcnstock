#!/usr/bin/env python3
"""
数据源层单元测试

测试范围:
- DataSourceManager: 数据源管理器
- DataValidator: 数据验证器
- DataCollector: 数据采集器
"""
import pytest
import pandas as pd
import numpy as np
from unittest import mock
from datetime import datetime

# 被测模块
from services.data_service.datasource.manager import DataSourceManager
from services.data_service.datasource.providers import TushareProvider, AkShareProvider, BaoStockProvider
from services.data_service.quality.validator import DataValidator, ValidationResult


class TestDataSourceManager:
    """数据源管理器测试类"""
    
    def test_initialization_with_primary_source(self):
        """测试使用主源初始化 - 应正确设置当前源为主源"""
        # Arrange
        config = {'primary': 'tushare', 'backup': ['akshare', 'baostock']}
        
        # Act
        manager = DataSourceManager(config)
        
        # Assert
        assert manager.current_source == 'tushare'
        assert manager.is_primary_active == True
        assert 'akshare' in manager.backup_sources
    
    def test_failover_to_backup_when_primary_fails(self):
        """测试主源失效时切换到备源 - 应自动故障转移"""
        # Arrange
        manager = DataSourceManager()
        manager.initialize()
        
        # 模拟主源调用失败
        with mock.patch.object(
            manager.primary_provider, 
            'fetch_kline', 
            side_effect=Exception('Connection timeout')
        ):
            with mock.patch.object(
                manager.backup_providers[0],
                'fetch_kline',
                return_value=pd.DataFrame({'close': [10.0, 11.0]})
            ) as mock_backup:
                # Act
                result = manager.fetch_kline('000001', '2024-01-01', '2024-01-31')
                
                # Assert
                assert manager.current_source == 'akshare'
                assert result is not None
                assert not result.empty
                mock_backup.assert_called_once()
    
    def test_recovery_to_primary_when_available(self):
        """测试主源恢复后切回 - 应在健康检查后切回主源"""
        # Arrange
        manager = DataSourceManager()
        manager.current_source = 'akshare'  # 当前使用备源
        
        with mock.patch.object(
            manager.primary_provider,
            'health_check',
            return_value=True
        ):
            # Act
            manager.check_primary_health()
            
            # Assert
            assert manager.current_source == 'tushare'
    
    def test_all_sources_failure_raises_exception(self):
        """测试所有数据源失效时 - 应抛出DataSourceException"""
        # Arrange
        manager = DataSourceManager()
        
        # 模拟所有数据源都失败
        with mock.patch.object(
            manager.primary_provider,
            'fetch_kline',
            side_effect=Exception('Primary failed')
        ):
            for backup in manager.backup_providers:
                backup.fetch_kline = mock.MagicMock(
                    side_effect=Exception('Backup failed')
                )
            
            # Act & Assert
            with pytest.raises(Exception) as exc_info:
                manager.fetch_kline('000001', '2024-01-01', '2024-01-31')
            
            assert 'All data sources failed' in str(exc_info.value)
    
    def test_source_health_monitoring(self):
        """测试数据源健康监控 - 应记录健康状态"""
        # Arrange
        manager = DataSourceManager()
        
        # Act
        health_status = manager.get_health_status()
        
        # Assert
        assert 'tushare' in health_status
        assert 'akshare' in health_status
        assert 'status' in health_status['tushare']
        assert 'last_check' in health_status['tushare']


class TestDataValidator:
    """数据验证器测试类"""
    
    def test_validate_price_with_normal_data(self):
        """测试正常价格数据 - 应验证通过"""
        # Arrange
        df = pd.DataFrame({
            'open': [10.0, 10.5, 11.0],
            'high': [10.8, 11.0, 11.5],
            'low': [9.8, 10.2, 10.8],
            'close': [10.5, 10.8, 11.2]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_price(df)
        
        # Assert
        assert isinstance(result, ValidationResult)
        assert result.is_valid == True
        assert len(result.errors) == 0
    
    def test_validate_price_with_negative_value(self):
        """测试负价格数据 - 应验证失败并返回错误"""
        # Arrange
        df = pd.DataFrame({
            'open': [10.0, -5.0, 11.0],
            'high': [10.8, 11.0, 11.5],
            'low': [9.8, 10.2, 10.8],
            'close': [10.5, 10.8, 11.2]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_price(df)
        
        # Assert
        assert result.is_valid == False
        assert any('negative' in err.lower() for err in result.errors)
    
    def test_validate_price_with_extreme_value(self):
        """测试极端价格数据 - 应标记警告"""
        # Arrange
        df = pd.DataFrame({
            'open': [10.0],
            'high': [1000.0],  # 极端高价
            'low': [9.8],
            'close': [500.0]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_price(df)
        
        # Assert
        assert result.is_valid == True  # 仍视为有效但警告
        assert len(result.warnings) > 0
        assert any('extreme' in w.lower() for w in result.warnings)
    
    def test_validate_ohlc_logic(self):
        """测试OHLC逻辑验证 - high应>=low, high应>=open/close"""
        # Arrange - high < low 的错误数据
        df = pd.DataFrame({
            'open': [10.0],
            'high': [9.0],   # 错误: high < low
            'low': [10.5],
            'close': [10.2]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_ohlc(df)
        
        # Assert
        assert result.is_valid == False
        assert any('ohlc' in err.lower() for err in result.errors)
    
    def test_validate_volume_with_normal_data(self):
        """测试正常成交量数据 - 应验证通过"""
        # Arrange
        df = pd.DataFrame({
            'volume': [10000, 15000, 20000]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_volume(df)
        
        # Assert
        assert result.is_valid == True
    
    def test_validate_volume_with_zero(self):
        """测试成交量为零 - 应标记为停牌警告"""
        # Arrange
        df = pd.DataFrame({
            'volume': [10000, 0, 15000]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_volume(df)
        
        # Assert
        assert result.is_valid == True  # 停牌是正常情况
        assert any('suspension' in w.lower() for w in result.warnings)
    
    def test_validate_volume_with_extreme_value(self):
        """测试极端成交量 - 应标记警告"""
        # Arrange
        df = pd.DataFrame({
            'volume': [10000, 999999999, 15000]  # 极端成交量
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_volume(df)
        
        # Assert
        assert len(result.warnings) > 0
        assert any('volume' in w.lower() for w in result.warnings)
    
    def test_validate_continuity(self):
        """测试数据连续性 - 应检测缺失日期"""
        # Arrange - 缺失一天的K线数据
        dates = pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-04'])  # 跳过1月3日
        df = pd.DataFrame({
            'date': dates,
            'close': [10.0, 10.5, 11.0]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_continuity(df, expected_freq='D')
        
        # Assert
        assert result.is_valid == True  # 非交易日缺失是正常的
        assert len(result.warnings) > 0
        assert any('gap' in w.lower() for w in result.warnings)
    
    def test_validate_all_with_comprehensive_check(self):
        """测试完整验证流程 - 应执行所有验证项"""
        # Arrange
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5),
            'open': [10.0, 10.5, 11.0, 11.5, 12.0],
            'high': [10.8, 11.0, 11.5, 12.0, 12.5],
            'low': [9.8, 10.2, 10.8, 11.2, 11.8],
            'close': [10.5, 10.8, 11.2, 11.8, 12.2],
            'volume': [10000, 12000, 15000, 13000, 16000]
        })
        validator = DataValidator()
        
        # Act
        result = validator.validate_all(df)
        
        # Assert
        assert result.is_valid == True
        assert 'price' in result.checks_passed
        assert 'ohlc' in result.checks_passed
        assert 'volume' in result.checks_passed
        assert 'continuity' in result.checks_passed


class TestTushareProvider:
    """Tushare数据源提供者测试"""
    
    def test_fetch_kline_with_valid_code(self):
        """测试获取有效股票K线 - 应返回DataFrame"""
        # Arrange
        provider = TushareProvider(token='test_token')
        
        with mock.patch.object(provider, 'pro') as mock_pro:
            mock_pro.daily.return_value = pd.DataFrame({
                'trade_date': ['20240101', '20240102'],
                'open': [10.0, 10.5],
                'high': [10.8, 11.0],
                'low': [9.8, 10.2],
                'close': [10.5, 10.8],
                'vol': [10000, 12000]
            })
            
            # Act
            result = provider.fetch_kline('000001.SZ', '2024-01-01', '2024-01-02')
            
            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert 'open' in result.columns
            assert 'close' in result.columns
    
    def test_fetch_kline_with_invalid_code(self):
        """测试获取无效股票K线 - 应返回空DataFrame"""
        # Arrange
        provider = TushareProvider(token='test_token')
        
        with mock.patch.object(provider, 'pro') as mock_pro:
            mock_pro.daily.return_value = pd.DataFrame()
            
            # Act
            result = provider.fetch_kline('INVALID', '2024-01-01', '2024-01-02')
            
            # Assert
            assert isinstance(result, pd.DataFrame)
            assert result.empty
    
    def test_health_check_with_valid_token(self):
        """测试健康检查 - 有效token应返回True"""
        # Arrange
        provider = TushareProvider(token='valid_token')
        
        with mock.patch.object(provider, 'pro') as mock_pro:
            mock_pro.trade_cal.return_value = pd.DataFrame({'cal_date': ['20240101']})
            
            # Act
            is_healthy = provider.health_check()
            
            # Assert
            assert is_healthy == True
    
    def test_health_check_with_invalid_token(self):
        """测试健康检查 - 无效token应返回False"""
        # Arrange
        provider = TushareProvider(token='invalid_token')
        
        with mock.patch.object(provider, 'pro') as mock_pro:
            mock_pro.trade_cal.side_effect = Exception('Invalid token')
            
            # Act
            is_healthy = provider.health_check()
            
            # Assert
            assert is_healthy == False


class TestAkShareProvider:
    """AkShare数据源提供者测试"""
    
    def test_fetch_kline_with_valid_code(self):
        """测试获取有效股票K线"""
        # Arrange
        provider = AkShareProvider()
        
        with mock.patch('akshare.stock_zh_a_hist') as mock_hist:
            mock_hist.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02'],
                '开盘': [10.0, 10.5],
                '收盘': [10.5, 10.8],
                '最高': [10.8, 11.0],
                '最低': [9.8, 10.2],
                '成交量': [10000, 12000]
            })
            
            # Act
            result = provider.fetch_kline('000001', '2024-01-01', '2024-01-02')
            
            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
    
    def test_fetch_kline_with_rate_limit(self):
        """测试频率限制处理 - 应自动重试"""
        # Arrange
        provider = AkShareProvider()
        
        with mock.patch('akshare.stock_zh_a_hist') as mock_hist:
            # 第一次调用触发频率限制，第二次成功
            mock_hist.side_effect = [
                Exception('Rate limit exceeded'),
                pd.DataFrame({
                    '日期': ['2024-01-01'],
                    '开盘': [10.0],
                    '收盘': [10.5],
                    '最高': [10.8],
                    '最低': [9.8],
                    '成交量': [10000]
                })
            ]
            
            with mock.patch('time.sleep'):  # 模拟等待
                # Act
                result = provider.fetch_kline('000001', '2024-01-01', '2024-01-02')
                
                # Assert
                assert isinstance(result, pd.DataFrame)
                assert mock_hist.call_count == 2


class TestBaoStockProvider:
    """BaoStock数据源提供者测试"""
    
    def test_fetch_kline_with_valid_code(self):
        """测试获取有效股票K线"""
        # Arrange
        provider = BaoStockProvider()
        
        with mock.patch('baostock.login') as mock_login, \
             mock.patch('baostock.query_history_k_data_plus') as mock_query:
            
            mock_login.return_value = mock.MagicMock(error_code='0')
            mock_query.return_value = mock.MagicMock(
                error_code='0',
                next=lambda: True,
                get_row_data=lambda: ['2024-01-01', '10.0', '10.8', '9.8', '10.5', '10000']
            )
            
            # Act
            result = provider.fetch_kline('sh.600000', '2024-01-01', '2024-01-02')
            
            # Assert
            assert isinstance(result, pd.DataFrame)
            mock_login.assert_called_once()
    
    def test_login_failure_handling(self):
        """测试登录失败处理 - 应抛出异常"""
        # Arrange
        provider = BaoStockProvider()
        
        with mock.patch('baostock.login') as mock_login:
            mock_login.return_value = mock.MagicMock(
                error_code='1',
                error_msg='Login failed'
            )
            
            # Act & Assert
            with pytest.raises(Exception) as exc_info:
                provider.login()
            
            assert 'Login failed' in str(exc_info.value)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
