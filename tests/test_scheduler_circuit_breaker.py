"""Tests for CircuitBreaker: state transitions (closed/open/half_open)."""

import json
import time
from pathlib import Path

import pytest

from services.data_service.scheduler.circuit_breaker import CircuitBreaker


@pytest.fixture
def circuit_breaker(tmp_path: Path) -> CircuitBreaker:
    """Create a CircuitBreaker with a temp state directory."""
    state_dir = tmp_path / "cb_state"
    return CircuitBreaker(state_dir=state_dir)


def _make_config(
    enabled: bool = True,
    failure_threshold: int = 3,
    recovery_timeout: int = 1,
) -> dict:
    """Helper to create a circuit breaker config dict."""
    return {
        "enabled": enabled,
        "failure_threshold": failure_threshold,
        "recovery_timeout": recovery_timeout,
    }


def test_starts_closed(circuit_breaker: CircuitBreaker):
    """New breaker should_skip returns False (closed state)."""
    assert circuit_breaker.should_skip("test_task", _make_config()) is False


def test_opens_after_threshold(circuit_breaker: CircuitBreaker):
    """Record N failures (threshold=3), verify should_skip returns True."""
    config = _make_config(failure_threshold=3)

    for i in range(3):
        circuit_breaker.record_failure("test_task", config)

    assert circuit_breaker.should_skip("test_task", config) is True


def test_recovers_after_timeout(circuit_breaker: CircuitBreaker):
    """Open breaker, wait past recovery_timeout, verify should_skip returns False."""
    config = _make_config(failure_threshold=2, recovery_timeout=1)

    # Open the breaker
    circuit_breaker.record_failure("test_task", config)
    circuit_breaker.record_failure("test_task", config)

    # Should skip immediately after opening
    assert circuit_breaker.should_skip("test_task", config) is True

    # Wait for recovery timeout to pass
    time.sleep(1.1)
    # Now should allow one attempt (half-open)
    assert circuit_breaker.should_skip("test_task", config) is False


def test_success_resets(circuit_breaker: CircuitBreaker):
    """Record failures up to threshold-1, then record success, verify reset."""
    config = _make_config(failure_threshold=3)

    circuit_breaker.record_failure("test_task", config)
    circuit_breaker.record_failure("test_task", config)

    # Success resets
    circuit_breaker.record_success("test_task")

    # State file should show closed with failure_count=0
    state_path = circuit_breaker._get_state_path("test_task")
    state = json.loads(state_path.read_text())
    assert state["state"] == "closed"
    assert state["failure_count"] == 0

    # Should not skip
    assert circuit_breaker.should_skip("test_task", config) is False


def test_half_open_reopens_on_failure(circuit_breaker: CircuitBreaker):
    """Half-open state, record failure, verify goes back to open."""
    config = _make_config(failure_threshold=2, recovery_timeout=1)

    # Open the breaker
    circuit_breaker.record_failure("test_task", config)
    circuit_breaker.record_failure("test_task", config)
    assert circuit_breaker.should_skip("test_task", config) is True

    # Wait to transition to half-open
    time.sleep(1.1)
    assert circuit_breaker.should_skip("test_task", config) is False

    # Failure during half-open re-opens
    circuit_breaker.record_failure("test_task", config)

    state_path = circuit_breaker._get_state_path("test_task")
    state = json.loads(state_path.read_text())
    assert state["state"] == "open"

    # Should skip again
    assert circuit_breaker.should_skip("test_task", config) is True
