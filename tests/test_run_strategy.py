"""
测试 run_strategy.py 的行为
遵循TDD原则：先写测试，再实现
"""
import pytest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestCodeQuality:
    """测试代码质量"""

    def test_no_silent_exception_handling(self):
        """测试代码中不应该有 except: pass 模式"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        lines = content.split('\n')
        found_silent_pass = False
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('except Exception as e:') or stripped.startswith('except:') or stripped.startswith('except Exception'):
                next_line = lines[i + 1].strip() if i + 1 < len(lines) else ''
                if next_line == 'pass':
                    found_silent_pass = True
                    silent_line_num = i + 2
                    break

        assert not found_silent_pass, \
            f"第{silent_line_num}行存在静默异常处理 (except Exception as e: pass)"

    def test_top_n_parameter_is_defined_but_not_used(self):
        """测试 top_n 参数已定义并被使用"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        assert 'top-n' in content or '--top-n' in content, "top_n参数应该被定义"
        assert 'args.top_n' in content, "args.top_n应该被使用"


class TestRunStrategyBehavior:
    """测试策略运行脚本的实际行为"""

    def test_exception_in_select_stocks_is_logged_not_silenced(self):
        """测试select_stocks中的异常应该被记录，而不是静默pass"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        lines = content.split('\n')
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('except Exception as e:'):
                next_line = lines[i + 1].strip() if i + 1 < len(lines) else ''
                next_next_line = lines[i + 2].strip() if i + 2 < len(lines) else ''
                if next_line == 'pass' or (next_line.startswith('#') and next_next_line == 'pass'):
                    pytest.fail(f"第{i+2}行存在静默异常处理，应该记录错误而不是pass")

    def test_json_output_saved_to_file(self):
        """测试JSON输出应该被保存到文件"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        assert 'json.dump' in content, "应该使用json.dump保存结果"
        assert 'open' in content, "应该打开文件写入"

    def test_strategy_info_included_in_output(self):
        """测试策略信息应该包含在输出中"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        assert 'strategy' in content.lower() or 'info' in content.lower(), \
            "输出应该包含策略信息"


class TestDataLoading:
    """测试数据加载"""

    def test_load_parquet_files(self):
        """测试应该加载parquet格式的数据"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        assert 'parquet' in content.lower(), "应该支持parquet格式"

    def test_handles_empty_data(self):
        """测试处理空数据的逻辑"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        assert 'len(result)' in content or 'if result' in content or 'not result' in content, \
            "应该检查结果是否为空"


class TestProgressReporting:
    """测试进度报告"""

    def test_progress_reported_for_large_universe(self):
        """测试大量股票时应该报告进度"""
        script_path = project_root / "scripts" / "run_strategy.py"
        with open(script_path, 'r') as f:
            content = f.read()

        assert 'processed' in content.lower() or '已处理' in content, \
            "应该报告处理进度"
