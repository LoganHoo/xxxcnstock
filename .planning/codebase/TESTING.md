# Testing Patterns

**Analysis Date:** 2026-05-12

## Test Framework

**Runner:**
- pytest (>=7.0.0)
- Config: `pyproject.toml` `[tool.pytest.ini_options]`
- Test paths: `["tests"]`
- Custom markers defined in `tests/conftest.py`:
  - `unit` -- unit tests
  - `integration` -- integration tests
  - `e2e` -- end-to-end tests
  - `slow` -- slow tests
  - `data` -- data-related tests

**Assertion Library:**
- Plain `assert` statements (pytest style, not unittest)
- 978 total assertions across `tests/` directory
- `unittest.TestCase` used in a few legacy tests (`tests/test_workflow_scheduler.py`, `tests/test_workflow_state_db.py`)

**Run Commands:**
```bash
pytest tests/                          # Run all tests in tests/
pytest tests/unit/                     # Unit tests only
pytest tests/integration/              # Integration tests only
pytest tests/ -v --tb=short            # Verbose with short tracebacks
pytest tests/ --cov=core               # Coverage for core/
pytest tests/unit/ -m unit             # By marker
```

**CI Pipeline:** `.github/workflows/integration-tests.yml`
- Runs on push to `main`/`develop` and daily at 02:00 UTC
- Matrix: Python 3.11, 3.12
- Steps: unit tests with coverage, integration tests, data freshness check
- Performance tests: only on schedule or `[perf]` commit tag
- Coverage upload to Codecov

## Test File Organization

**Location:**
- Primary: `tests/` directory (structured)
  - `tests/unit/` -- 16 unit test files
  - `tests/integration/` -- 6 integration test files
  - `tests/performance/` -- 1 performance test file
  - `tests/` root -- ~37 mixed test files
- Secondary (ad-hoc): root-level `test_*.py` files (~30 files) -- NOT part of pytest discovery
- Tertiary: `scripts/test_*.py` files (~23 files) -- ad-hoc verification scripts

**Naming:**
- Pattern: `test_<module_or_feature>.py`
- Examples: `test_factor_engine.py`, `test_scheduler.py`, `test_data_service_integration.py`

**Structure:**
```
tests/
  conftest.py                    # Shared fixtures and pytest hooks
  unit/                          # Unit tests (16 files)
    test_scheduler.py
    test_dragon_head_strategy.py
    test_endstock_strategy.py
    ...
  integration/                   # Integration tests (6 files)
    test_data_service_integration.py
    test_workflow_integration.py
    ...
  performance/                   # Performance tests (1 file)
    test_data_service_performance.py
  test_*.py                      # ~37 test files at this level
```

**Ad-hoc test sprawl (see CONCERNS):**
```
<project-root>/
  test_*.py                      # ~30 ad-hoc test/debug files (NOT in tests/)
  check_*.py                     # ~4 ad-hoc check scripts
  scripts/test_*.py              # ~23 ad-hoc verification scripts
  scripts/check_*.py             # ~28 check/verification scripts
```

## Test Structure

**Suite Organization:**
```python
# Standard pattern (tests/ directory):
class TestFactorEngine:
    """FactorEngine tests"""

    @pytest.fixture
    def engine(self):
        return FactorEngine(config_dir="config/factors")

    @pytest.fixture
    def sample_data(self):
        return pl.DataFrame({...})

    def test_load_configs(self, engine):
        assert len(engine.factor_configs) > 0

    def test_calculate_factor_missing(self, engine, sample_data):
        result = engine.calculate_factor(sample_data, "nonexistent_factor")
        assert "factor_nonexistent_factor" in result.columns
```

**Patterns:**
- **Setup:** `@pytest.fixture` for test dependencies, class-level fixtures common
- **Teardown:** `autouse=True` cleanup fixture in `conftest.py` (currently a no-op `pass`)
- **Assertion:** Plain `assert` with descriptive error messages
- **Test isolation:** `tmp_path` / `tempfile.TemporaryDirectory()` for file-based tests
- **Monkeypatching:** `monkeypatch` fixture for environment variables and module-level functions

## Mocking

**Framework:** `unittest.mock` (Mock, patch, MagicMock, AsyncMock)

**Patterns:**
```python
# Pattern 1: Patch external dependencies
from unittest.mock import Mock, patch

with patch('core.cache.multi_level_cache.RedisCache') as mock_redis:
    mock_redis_instance = Mock()
    # ... test code

# Pattern 2: Inject mock objects via constructor
service = DailyUpdateTask()
task.market_behavior_task = Mock()
task.market_behavior_task.run_daily_update.return_value = {'success': True}

# Pattern 3: Monkeypatch for functions
def fake_subprocess(cmd, timeout, cwd, env=None, task_name=None):
    return 0, "ok", ""

monkeypatch.setattr(scheduler, "run_subprocess_with_timeout", fake_subprocess)
```

**What to Mock:**
- External API calls (Tushare, AKShare, Redis, Kafka)
- File system operations (use `tmp_path` instead when possible)
- Network requests (use `patch` on `requests.post`, etc.)
- Subprocess execution (scheduler tests mock `run_subprocess_with_timeout`)

**What NOT to Mock:**
- Factor calculation logic (test with real DataFrames)
- Data validation logic (test with real data structures)
- Configuration loading from YAML files in `config/`

## Fixtures and Factories

**Test Data:**
```python
# From tests/conftest.py -- comprehensive financial data fixtures
@pytest.fixture
def sample_kline_data():
    """30-day daily K-line data with trend"""
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    closes = [10.0 + i * 0.1 + np.sin(i * 0.5) * 0.5 for i in range(30)]
    df = pd.DataFrame({
        'date': dates,
        'open': [...], 'high': [...], 'low': [...], 'close': closes,
        'volume': [...]
    })
    return df

@pytest.fixture
def sample_kline_limitup():
    """Consecutive limit-up K-line data (10% daily gains)"""
    ...

@pytest.fixture
def sample_financial_data():
    """Financial statement data dict"""
    return {'code': '000001', 'roe': 15.5, 'pe': 15.0, ...}
```

**Key fixtures in `tests/conftest.py`:**
- `sample_kline_data` -- 30-day OHLCV data
- `sample_kline_with_gap` -- gap/jump data
- `sample_kline_limitup` -- consecutive limit-up data
- `sample_financial_data` -- financial metrics dict
- `sample_financial_dataframe` -- multi-stock financial DataFrame
- `mock_bullish_market` / `mock_bearish_market` / `mock_neutral_market` -- market regime data
- `sample_position` / `sample_position_profit` / `sample_position_loss` -- position data
- `sample_buy_signal` / `sample_sell_signal` -- strategy signals
- `sample_stock_list` -- stock universe DataFrame
- `sample_strategy_config` / `sample_risk_config` -- configuration dicts
- `mock_tushare_response` -- parameterized API response factory
- `temp_directory` -- alias for pytest's `tmp_path`

**Location:**
- Global fixtures: `tests/conftest.py` (single file)
- Per-test fixtures: defined within test classes using `@pytest.fixture`
- Helper builders: `build_task()` pattern in `tests/unit/test_scheduler.py`

## Coverage

**Requirements:** No minimum enforced locally. CI generates coverage reports via `--cov=core --cov-report=xml` and uploads to Codecov.

**Coverage file:** `.coverage.SimonsQuantdeMac-mini.local.37770.Xcqxfrsx` present at project root (local artifact, should be gitignored).

**View Coverage:**
```bash
pytest tests/ --cov=core --cov-report=term-missing
pytest tests/ --cov=core --cov-report=html    # HTML report in htmlcov/
```

## Test Types

**Unit Tests:**
- Location: `tests/unit/` (16 files)
- Scope: Individual classes/functions tested in isolation
- External dependencies mocked
- 516 test functions total across `tests/` directory
- Examples: `test_scheduler.py` (circuit breaker, progress check, env merge), `test_dragon_head_strategy.py`, `test_endstock_strategy.py`

**Integration Tests:**
- Location: `tests/integration/` (6 files)
- Scope: Multi-module interactions, data flow end-to-end
- Some use real file I/O with `tmp_path`
- Examples: `test_data_service_integration.py` (financial data flow, error recovery), `test_workflow_integration.py`
- Include performance benchmarks: query latency < 1s for 100 queries, batch load < 500ms for 10 stocks

**E2E Tests:**
- Not formally implemented
- Some integration tests approach E2E scope (full data collection -> processing -> storage -> query)
- CI pipeline has `workflow-test` job that runs pipeline scripts with `--dry-run`

**Performance Tests:**
- Location: `tests/performance/` (1 file)
- CI: Runs only on schedule or `[perf]` tag
- Benchmarks defined in integration tests too (e.g., `TestDataServicePerformance`)

## Common Patterns

**Async Testing:**
```python
from unittest.mock import AsyncMock

async def test_async_client():
    mock_response = Mock()
    mock_response.json = AsyncMock(return_value={"data": "test"})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)
```

**Error Testing:**
```python
# Pattern 1: pytest.raises for expected exceptions
with pytest.raises(ValueError, match="重复键"):
    scheduler.load_cron_tasks()

# Pattern 2: Assert error conditions in results
result = validator.validate_accounting_identity(invalid_data)
assert not result['valid']
assert 'error' in result
```

**Static Analysis Tests (meta-testing):**
```python
# tests/test_run_strategy.py -- tests code quality, not runtime behavior
class TestCodeQuality:
    def test_no_silent_exception_handling(self):
        """Code should not have except: pass patterns"""
        with open(script_path, 'r') as f:
            content = f.read()
        # ... parse and assert no silent handlers
```

**Scheduler Testing (advanced patterns):**
```python
# tests/unit/test_scheduler.py -- shows best practices
def test_circuit_breaker_opens_after_threshold(tmp_path, monkeypatch):
    """State-based testing with file persistence"""
    state_file = tmp_path / "breaker.json"
    monkeypatch.setattr(scheduler, "CIRCUIT_BREAKER_STATE_FILE", state_file)
    # ... exercise and assert state changes
```

## Test-to-Source Coverage Analysis

| Source Area | Source Files | Test Files | Gap Assessment |
|-------------|-------------|------------|----------------|
| `core/` | 70 | ~25 (in tests/) | Moderate -- major modules covered, many smaller files untested |
| `services/` | ~35 subdirs | ~5 | Low -- only data_service and notify_service tested |
| `scripts/` | ~244 | ~23 (ad-hoc) | Very low -- ad-hoc scripts, not real unit tests |
| `workflows/` | ~9 | 0 | None -- no dedicated test files for workflow modules |
| `filters/` | ~12 | 2 | Low |
| `factors/` | ~6 | 3 | Moderate |

**Total test functions:**
- Structured tests (`tests/`): 516
- Root-level ad-hoc scripts: 26 (most are exploration scripts, not real tests)

## Anti-Patterns in Tests

1. **Root-level test sprawl:** ~30 `test_*.py` files at project root that are NOT in pytest discovery path and often contain exploration/debug code rather than proper tests (e.g., `test_v_total.py` connects to real data)

2. **Static code checking as tests:** `tests/test_run_strategy.py` opens source files and checks string patterns -- fragile, breaks on refactoring

3. **No-test test methods:** Empty `pass` bodies in `tests/test_data_quality.py` (`test_check_data_completeness`, `test_check_data_accuracy`)

4. **Cleanup fixture is a no-op:** `tests/conftest.py` has `autouse=True` cleanup fixture that does nothing

---

*Testing analysis: 2026-05-12*
