"""
Subprocess executor with timeout and force-kill.

Runs a script as subprocess.Popen with timeout handling:
terminate -> wait(5) -> kill() on timeout.
Returns (returncode, stdout, stderr).
"""

import os
import subprocess
import time
from typing import Optional

from loguru import logger

MAX_OUTPUT_SIZE = 10 * 1024 * 1024  # 10MB


def run_subprocess(
    cmd: list[str],
    timeout: int,
    cwd: str,
    env: Optional[dict[str, str]] = None,
) -> tuple[int, str, str]:
    """Execute a command as subprocess with timeout and force-kill.

    Args:
        cmd: Command and arguments.
        timeout: Maximum execution time in seconds.
        cwd: Working directory.
        env: Optional environment variables.

    Returns:
        Tuple of (returncode, stdout, stderr).
    """
    process = None
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
            env=env,
            text=True,
            start_new_session=True,
        )

        start_time = time.time()
        while True:
            if process.poll() is not None:
                break

            if time.time() - start_time > timeout:
                raise subprocess.TimeoutExpired(cmd, timeout)

            time.sleep(1)

        stdout, stderr = process.communicate()
        stdout = _truncate(stdout)
        stderr = _truncate(stderr)
        return process.returncode, stdout, stderr

    except subprocess.TimeoutExpired:
        logger.warning(f"Subprocess timeout after {timeout}s: {' '.join(cmd)}")
        if process:
            _kill_process(process)
        raise

    except Exception:
        if process:
            try:
                process.kill()
                process.wait()
            except Exception:
                pass
        raise


def _kill_process(process: subprocess.Popen) -> None:
    """Terminate -> wait(5) -> kill a subprocess."""
    try:
        process.terminate()
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        logger.warning("Terminate failed, sending SIGKILL")
        try:
            process.kill()
            process.wait()
        except Exception:
            pass
    except Exception:
        try:
            process.kill()
            process.wait()
        except Exception:
            pass


def _truncate(text: str) -> str:
    """Truncate text to MAX_OUTPUT_SIZE."""
    if len(text) > MAX_OUTPUT_SIZE:
        return text[:MAX_OUTPUT_SIZE] + f"\n... [truncated, total: {len(text)} bytes]"
    return text
