"""
测试资金行为学策略运行脚本
"""
import importlib
import sys
import os
import tempfile
from pathlib import Path

import polars as pl
import yaml
import pytest

MODULE_NAME = "scripts.run_fund_behavior_strategy"


def load_module():
    """加载待测模块"""
    if MODULE_NAME in sys.modules:
        return importlib.reload(sys.modules[MODULE_NAME])
    return importlib.import_module(MODULE_NAME)


def test_load_config_defaults_to_strategy_config():
    """默认应加载 strategies 目录下的配置"""
    module = load_module()

    config = module.load_config()

    assert config["indicators"]["10am_pivot"]["price_threshold"] == 15.0


def test_load_config_supports_relative_path_from_any_cwd(monkeypatch, tmp_path):
    """相对路径应基于项目根目录解析，而不是当前工作目录"""
    module = load_module()
    monkeypatch.chdir(tmp_path)

    config = module.load_config("config/strategies/fund_behavior_config.yaml")

    assert config["indicators"]["10am_pivot"]["price_threshold"] == 15.0


def test_load_config_syncs_runtime_config_manager():
    """脚本加载的配置应同步到运行时配置管理器"""
    module = load_module()

    module.load_config("config/strategies/fund_behavior_config.yaml")

    assert module.config_manager.get("indicators.10am_pivot.price_threshold") == 15.0


def test_main_allows_zero_capital_override(monkeypatch):
    """命令行传入 0 资金时也应覆盖默认资金"""
    module = load_module()
    captured = {}

    monkeypatch.setattr(
        module,
        "load_config",
        lambda path: {"backtest": {"initial_capital": 1000000}, "factors": {}},
    )
    monkeypatch.setattr(
        module,
        "load_data",
        lambda data_path: pl.DataFrame(
            {
                "code": [],
                "trade_date": [],
                "open": [],
                "close": [],
                "high": [],
                "low": [],
                "volume": [],
            }
        ),
    )
    monkeypatch.setattr(module, "calculate_factors", lambda data, config: data)
    monkeypatch.setattr(
        module,
        "execute_strategy",
        lambda factor_data, config: captured.setdefault(
            "capital", config["backtest"]["initial_capital"]
        ) or {},
    )
    monkeypatch.setattr(module, "print_strategy_result", lambda result, data, config: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_fund_behavior_strategy.py", "--capital", "0"],
    )

    module.main()

    assert captured["capital"] == 0.0


def test_strategy_config_declares_hedge_v_total_threshold():
    """对冲配置应显式声明 v_total_threshold，避免运行时 KeyError"""
    with open(
        "/Volumes/Xdata/workstation/xxxcnstock/config/strategies/fund_behavior_config.yaml",
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    assert config["indicators"]["hedge"]["v_total_threshold"] == 1800


class TestPipelineStateManager:
    """测试 PipelineStateManager 功能"""

    def test_state_transition(self, monkeypatch):
        """测试状态转换"""
        module = load_module()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv('CHECKPOINT_DIR', tmpdir)
            
            pipeline = module.PipelineStateManager("2024-01-01")
            
            assert pipeline.state == module.PipelineState.START
            
            pipeline.transition(module.PipelineState.LOADED, "load", {"rows": 100})
            assert pipeline.state == module.PipelineState.LOADED

    def test_checkpoint_persistence(self, monkeypatch):
        """测试断点持久化"""
        module = load_module()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv('CHECKPOINT_DIR', tmpdir)
            
            pipeline = module.PipelineStateManager("2024-01-01")
            
            pipeline.transition(module.PipelineState.LOADED, "load", {"rows": 100})
            
            checkpoint_path = pipeline.get_checkpoint_path("load", ".json")
            assert checkpoint_path.exists()

    def test_resume_from_checkpoint(self, monkeypatch):
        """测试从断点恢复"""
        module = load_module()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv('CHECKPOINT_DIR', tmpdir)
            
            pipeline = module.PipelineStateManager("2024-01-01")
            
            pipeline.transition(module.PipelineState.LOADED, "load", {"rows": 100})
            
            pipeline.transition(module.PipelineState.VALIDATED, "validate", {"valid": True})
            
            new_pipeline = module.PipelineStateManager("2024-01-01")
            assert new_pipeline.state == module.PipelineState.VALIDATED


class TestEmailNotification:
    """测试邮件通知功能"""

    def test_email_notification_with_nacos_config(self, monkeypatch):
        """测试使用Nacos配置发送邮件"""
        module = load_module()
        
        mock_nacos_client = type('MockNacosClient', (), {
            'get_config': lambda self, *args: yaml.dump({
                'email': {
                    'notification': {'emails': ['test@example.com']},
                    'smtp': {
                        'server': 'smtp.example.com',
                        'port': 465
                    }
                }
            })
        })()
        
        monkeypatch.setattr(module, 'get_nacos_client', lambda: mock_nacos_client)
        monkeypatch.setattr(module, 'generate_fund_behavior_html', lambda x: '<html></html>')
        
        result = module.send_email_notification(
            result={'stocks': []},
            report_text="测试报告"
        )
        
        assert result is False

    def test_email_notification_with_env_fallback(self, monkeypatch):
        """测试Nacos不存在时使用.env配置"""
        module = load_module()
        
        mock_nacos_client = type('MockNacosClient', (), {
            'get_config': lambda self, *args: None
        })()
        
        monkeypatch.setattr(module, 'get_nacos_client', lambda: mock_nacos_client)
        monkeypatch.setenv('NOTIFICATION_EMAILS', 'test@example.com')
        monkeypatch.setenv('EMAIL_SMTP_SERVER', 'smtp.qq.com')
        monkeypatch.setenv('EMAIL_SMTP_PORT', '465')
        monkeypatch.setenv('EMAIL_USERNAME', 'test@qq.com')
        monkeypatch.setenv('EMAIL_PASSWORD', 'password')
        monkeypatch.setattr(module, 'generate_fund_behavior_html', lambda x: '<html></html>')
        
        result = module.send_email_notification(
            result={'stocks': []},
            report_text="测试报告"
        )
        
        assert result is False


class TestBufferAndLoad:
    """测试缓冲和加载功能"""

    def test_buffer_factors(self, monkeypatch):
        """测试因子数据缓冲"""
        module = load_module()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            buffer_dir = Path(tmpdir) / 'buffer'
            buffer_dir.mkdir(parents=True, exist_ok=True)
            
            mock_pipeline = type('MockPipeline', (), {
                'report_date': '2024-01-01',
                'get_checkpoint_path': lambda self, step, ext: buffer_dir / f"{step}_2024-01-01{ext}"
            })()
            
            factor_data = pl.DataFrame({
                'code': ['000001', '000002'],
                'trade_date': ['2024-01-01', '2024-01-01'],
                'factor1': [1.0, 2.0],
                'factor2': [0.5, 1.5]
            })
            
            buffer_path = module.buffer_factors(factor_data, mock_pipeline)
            
            assert buffer_path is not None
            assert Path(buffer_path).exists()

    def test_load_buffered_factors(self, monkeypatch):
        """测试加载缓冲的因子数据"""
        module = load_module()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            buffer_dir = Path(tmpdir) / 'buffer'
            buffer_dir.mkdir(parents=True, exist_ok=True)
            
            buffer_path = buffer_dir / "fund_behavior_2024-01-01.parquet"
            
            factor_data = pl.DataFrame({
                'code': ['000001', '000002'],
                'trade_date': ['2024-01-01', '2024-01-01'],
                'factor1': [1.0, 2.0],
                'factor2': [0.5, 1.5]
            })
            factor_data.write_parquet(buffer_path)
            
            mock_pipeline = type('MockPipeline', (), {
                'report_date': '2024-01-01'
            })()
            
            loaded_data = module.load_buffered_factors(mock_pipeline)
            
            assert loaded_data is not None
            assert len(loaded_data) == 2


class TestDistributeResults:
    """测试结果分发功能"""

    def test_distribute_results_creates_files(self, monkeypatch):
        """测试结果分发创建文件"""
        module = load_module()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir) / 'reports'
            reports_dir.mkdir(parents=True, exist_ok=True)
            
            html_dir = reports_dir / 'html'
            html_dir.mkdir(exist_ok=True)
            
            monkeypatch.setenv('REPORTS_DIR', str(reports_dir))
            
            mock_pipeline = type('MockPipeline', (), {
                'report_date': '2024-01-01',
                'get_checkpoint_path': lambda self, step, ext: reports_dir / f"{step}_2024-01-01{ext}"
            })()
            
            result = {
                'stocks': [
                    {'code': '000001', 'name': '平安银行', 'score': 95},
                    {'code': '000002', 'name': '万科A', 'score': 90}
                ],
                'summary': {
                    'total_stocks': 2,
                    'avg_score': 92.5
                }
            }
            
            report_text = "测试报告内容"
            
            success, meta = module.distribute_results(result, report_text, mock_pipeline)
            
            assert success is True
            assert meta['success_count'] >= 3
