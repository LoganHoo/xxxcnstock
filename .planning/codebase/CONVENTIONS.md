# Coding Conventions

**Analysis Date:** 2026-05-12

## Naming Patterns

**Files:**
- Python source files: `snake_case.py` (e.g., `factor_engine.py`, `data_quality_guardian.py`)
- Test files: `test_*.py` prefix (e.g., `test_factor_engine.py`, `test_scheduler.py`)
- Config files: `snake_case.yaml` or `snake_case.json` in `config/`
- Scripts: `snake_case.py` with verbs (e.g., `check_data_freshness.py`, `send_review_report.py`)
- Ad-hoc test/debug scripts at root: `test_*.py`, `check_*.py` (not in `tests/` -- see CONCERNS)

**Functions:**
- Public functions: `snake_case` (e.g., `calculate_factor`, `get_settings`, `setup_logger`)
- Private/internal: `_leading_underscore` (e.g., `_load_factor_configs`, `_update_success_rate`)
- Test functions: `test_` prefix required by pytest (e.g., `test_load_configs`, `test_health_handler_stats`)

**Variables:**
- `snake_case` throughout (e.g., `task_name`, `success_rate`, `dragon_tiger_count`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `TASK_DURATION`, `PIPELINE_STAGE_DURATION`, `REDIS_HOST`)

**Types:**
- Dataclasses used for structured data (e.g., `TaskMetrics` in `core/pipeline_monitor.py`)
- Pydantic `BaseSettings` for configuration (`core/config.py`)
- Enums for finite states (e.g., `OrderSide`, `OrderType` in `core/broker_adapter.py`)
- Type annotations used in ~467 locations across `core/` (moderate adoption, not universal)

## Code Style

**Formatting:**
- Ruff configured in `pyproject.toml`: `line-length = 100`, `target-version = "py310"`
- No `black`, `isort`, or `flake8` configuration detected -- Ruff is the sole linter/formatter
- Ruff is declared in config but **no CI enforcement** of Ruff rules was found (no lint step in CI workflow)

**Linting:**
- Ruff only (see `[tool.ruff]` in `pyproject.toml`)
- Only 1 `# noqa` / `# type: ignore` found across `core/` and `services/` -- very low suppression rate
- No `mypy` or type-checking enforcement

## Import Organization

**Order (observed in source files):**
1. Standard library (`os`, `sys`, `logging`, `json`, `pathlib`, `datetime`)
2. Third-party (`pandas`, `numpy`, `polars`, `loguru`, `pytest`, `redis`, `yaml`)
3. Local/project (`core.*`, `services.*`, `scripts.*`)

**Path resolution pattern (IMPORTANT -- see CONCERNS):**
- Files throughout `core/`, `services/`, and `scripts/` use `sys.path.insert(0, ...)` to resolve project imports
- Two variants observed:
  - Relative: `sys.path.insert(0, str(Path(__file__).parent.parent))`
  - Hardcoded (broken): `sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')`
- Files in `tests/` directory do NOT use `sys.path.insert` -- they rely on `pyproject.toml` `testpaths` config

## Error Handling

**Patterns:**
- Broad `except Exception as e:` is the dominant pattern -- **1,401 occurrences** across the codebase
- Specific exceptions used rarely: `FileNotFoundError`, `ValueError`, `TimeoutError`, `NotImplementedError`
- Custom exception classes: **not found** (no `class *Error` or `class *Exception` definitions)
- Retry logic exists in `core/workflow_framework.py` (`run_with_retry` method) and `core/async_http.py`
- Graceful degradation pattern in scheduler: Redis lock falls back to no-lock mode on connection failure

**Anti-patterns:**
- Silent exception swallowing via `except: pass` found in root-level test scripts (e.g., `test_v_total.py:23`)
- Many `except Exception` blocks log the error but then return `None` or `{}` without re-raising
- No structured error codes or error classification system

## Logging

**Framework:** Loguru (primary) + stdlib `logging` (secondary)

**Dual logging issue (see CONCERNS):**
- `core/logger.py` configures Loguru with structured format, rotation, and compression
- Many files in `core/` import stdlib `logging` directly instead of using `core.logger`:
  - `core/factor_calculator.py`, `core/data_quality_checker.py`, `core/data_loader.py`
  - `core/data_validator.py`, `core/trading_calendar.py`, `core/strategy_executor.py`
  - `core/report_validator.py`, `core/report_config.py`, `core/data_adapter.py`
- Other files correctly use `from loguru import logger` or `from core.logger import setup_logger`

**Log structure:**
```
2024-01-01 12:00:00 | INFO     | module:function:line | message
```

**Log files:** Stored in `logs/` directory with subdirectories: `logs/pipeline/`, `logs/signals/`, `logs/scheduler/`, `logs/system/`

**Production logging:** Rotation at `00:00`, 30-day retention, ZIP compression

**When to log:**
- Task start/completion with duration: `logger.info(f"Task started: {task_name}")`
- Errors with context: `logger.error(f"Failed to...: {e}")`
- Warnings for degraded operation: `logger.warning(f"Redis connection failed: {e}")`

## Comments

**When to Comment:**
- Chinese comments are standard (matches team language)
- Module-level docstrings in triple-quote format with purpose description
- Function docstrings include `Args:` and `Returns:` sections (Google-style)
- Inline comments for business logic explanation (e.g., "涨停回调至20日均线")

**JSDoc/TSDoc:** Not applicable (Python project). Docstrings follow Google style.

## Function Design

**Size:** No enforced limit. Largest files exceed 1,000 lines (e.g., `scripts/run_fund_behavior_strategy.py` at 2,061 lines). The project's `CLAUDE.md` specifies a 300-line limit but it is not enforced by tooling.

**Parameters:** Mix of positional and keyword arguments. Type hints used inconsistently.

**Return Values:**
- `Dict[str, Any]` is the most common return type for complex results
- `Optional[T]` used for functions that may fail
- Tuple returns for status pairs: `(bool, str)` (e.g., `check_kline_data_alignment`)
- `pd.DataFrame` or `pl.DataFrame` for data operations

## Module Design

**Exports:**
- Public classes and functions exported directly from modules
- Some modules use `__all__` pattern (inconsistent)
- Package-level imports available via `__init__.py` files in most packages

**Barrel Files:** Limited usage. Most `__init__.py` files are empty or minimal. No comprehensive barrel exports.

## Configuration Conventions

**Environment variables:** Managed via `.env` file, loaded by `pydantic-settings` in `core/config.py`
- Singleton pattern: `get_settings()` with `@lru_cache()`
- All config fields have defaults -- no hard-coded secrets in source (except `sys.path` issues)

**YAML configs:** Used for strategy definitions (`config/strategies/`), factor definitions (`config/factors/`), filter definitions (`config/filters/`), scheduler config (`config/scheduler.yaml`)

## New Code Guidelines

When writing new code for this project:
1. Use `from core.logger import setup_logger` / `from core.logger import get_logger` -- do NOT import stdlib `logging` directly
2. Use `sys.path` manipulation only in standalone scripts, never in `core/` or `services/` modules
3. Raise specific exceptions (`ValueError`, `FileNotFoundError`) rather than bare `except Exception`
4. Use `polars` for new data processing code (project is migrating from `pandas`)
5. Type-annotate all public function signatures
6. Keep files under 300 lines; refactor when exceeding
7. Use `pytest` for tests, placed in `tests/unit/` or `tests/integration/` with proper mocking
8. Chinese comments and docstrings are acceptable and expected

---

*Convention analysis: 2026-05-12*
