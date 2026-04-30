# Cron Tasks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 重构 `config/cron_tasks.yaml` 为“当前可执行任务 + 主文件内规划保留区 + 阶段视图”，并完成脚本存在性校正、业务流评审与加载验证。

**Architecture:** 保持 `scripts/scheduler.py` 对 `tasks` 的现有消费模型不变，避免引入运行时兼容风险。通过重排 YAML 结构、标记规划态任务、修正失效脚本引用和输出评审结论，让配置文件同时承担执行契约与架构评审视图两种职责，但边界清晰。

**Tech Stack:** YAML, Python 3.11+, pytest, pathlib, scripts/scheduler.py

---

### Task 1: 建立 `cron_tasks.yaml` 结构回归测试

**Files:**
- Create: `tests/unit/test_cron_tasks_config.py`
- Modify: `config/cron_tasks.yaml`

**Step 1: Write the failing test**

```python
def test_enabled_tasks_reference_existing_scripts():
    tasks = scheduler.load_cron_tasks()
    for task in tasks:
        script = PROJECT_ROOT / task["script"]
        assert script.exists(), task["name"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cron_tasks_config.py::test_enabled_tasks_reference_existing_scripts -q`
Expected: FAIL，指出 `kline_collect.py` 缺失或其他启用任务配置问题

**Step 3: Write minimal implementation**

```python
# 仅通过最小配置修正让启用任务不再指向缺失脚本
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_cron_tasks_config.py::test_enabled_tasks_reference_existing_scripts -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/unit/test_cron_tasks_config.py config/cron_tasks.yaml
git commit -m "test: validate enabled cron task script paths"
```

### Task 2: 建立依赖闭合与规划态边界测试

**Files:**
- Modify: `tests/unit/test_cron_tasks_config.py`
- Modify: `config/cron_tasks.yaml`

**Step 1: Write the failing test**

```python
def test_enabled_task_dependencies_resolve_to_real_enabled_tasks():
    ...

def test_planned_tasks_are_disabled_and_marked():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cron_tasks_config.py -q`
Expected: FAIL，因为当前文件还没有显式规划区或依赖边界不够清晰

**Step 3: Write minimal implementation**

```python
# 引入 planned_tasks（或等价规划区）
# 将未来/未接线任务统一 enabled: false + status: planned
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_cron_tasks_config.py -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/unit/test_cron_tasks_config.py config/cron_tasks.yaml
git commit -m "feat: separate active and planned cron tasks"
```

### Task 3: 重构 `cron_tasks.yaml` 主结构

**Files:**
- Modify: `config/cron_tasks.yaml`
- Modify: `docs/plans/2026-04-30-cron-tasks-design.md`

**Step 1: Write the failing test**

```python
def test_cron_config_keeps_execution_contract_in_tasks_only():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cron_tasks_config.py::test_cron_config_keeps_execution_contract_in_tasks_only -q`
Expected: FAIL，因为当前 `groups` 与执行面仍混杂

**Step 3: Write minimal implementation**

```yaml
global:
  ...
tasks:
  ...
planned_tasks:
  ...
groups:
  ...
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_cron_tasks_config.py::test_cron_config_keeps_execution_contract_in_tasks_only -q`
Expected: PASS

**Step 5: Commit**

```bash
git add config/cron_tasks.yaml docs/plans/2026-04-30-cron-tasks-design.md
git commit -m "refactor: restructure cron tasks config"
```

### Task 4: 输出配置评审结论并同步项目文档

**Files:**
- Modify: `说明文档.md`
- Create: `docs/plans/2026-04-30-cron-tasks-review.md`

**Step 1: Write the failing test**

```python
def test_cron_review_lists_active_and_planned_tasks():
    review = REVIEW_FILE.read_text(encoding="utf-8")
    assert "当前可执行任务" in review
    assert "规划态任务" in review
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cron_tasks_config.py::test_cron_review_lists_active_and_planned_tasks -q`
Expected: FAIL，因为评审文档还不存在

**Step 3: Write minimal implementation**

```markdown
# Cron Tasks Review
- 当前可执行任务
- 规划态任务
- 高风险问题
- 已修复问题
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_cron_tasks_config.py::test_cron_review_lists_active_and_planned_tasks -q`
Expected: PASS

**Step 5: Commit**

```bash
git add docs/plans/2026-04-30-cron-tasks-review.md 说明文档.md tests/unit/test_cron_tasks_config.py
git commit -m "docs: add cron tasks review report"
```

### Task 5: 全量验证

**Files:**
- Modify: `tests/unit/test_cron_tasks_config.py`

**Step 1: Write the regression tests**

```python
def test_scheduler_can_load_cron_tasks_config():
    tasks = scheduler.load_cron_tasks()
    assert tasks
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_cron_tasks_config.py tests/unit/test_scheduler.py -q`
Expected: 至少部分 FAIL

**Step 3: Write minimal implementation / cleanup**

```python
# 只做必要调整，不修改 scheduler.py 的消费模型
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_cron_tasks_config.py tests/unit/test_scheduler.py tests/unit/test_progress_helper.py -q`
Expected: PASS

Run: `python -m py_compile tests/unit/test_cron_tasks_config.py`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/unit/test_cron_tasks_config.py
git commit -m "test: lock cron task config contract"
```
