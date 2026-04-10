import os
from pathlib import Path
import subprocess
import shutil
import sys


def build_pythonwarnings(existing: str | None = None) -> str:
    warning_parts = []
    if existing:
        warning_parts.append(existing)

    warning_parts.extend(
        [
            "ignore:'asyncio.AbstractEventLoopPolicy' is deprecated and slated for removal in Python 3.16:DeprecationWarning",
            "ignore:Due to '_pack_'.*_RawValue.*:DeprecationWarning:py_mini_racer._dll",
            "ignore:Due to '_pack_'.*_ArrayBufferByte.*:DeprecationWarning:py_mini_racer._objects",
            "error::DeprecationWarning",
        ]
    )
    return ",".join(warning_parts)


def build_environment(existing_env: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(existing_env or os.environ)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    env["PYTHONWARNINGS"] = build_pythonwarnings(env.get("PYTHONWARNINGS"))
    return env


def resolve_pytest_python() -> str:
    pytest_path = shutil.which("pytest")
    if pytest_path:
        first_line = Path(pytest_path).read_text(encoding="utf-8").splitlines()[0]
        if first_line.startswith("#!"):
            return first_line[2:].strip()
    return sys.executable


def build_pytest_command(pytest_args: list[str]) -> list[str]:
    return [resolve_pytest_python(), "-m", "pytest", "-p", "anyio.pytest_plugin", *pytest_args]


def main(argv: list[str] | None = None) -> int:
    pytest_args = argv if argv is not None else sys.argv[1:]
    return subprocess.call(build_pytest_command(pytest_args), env=build_environment())


if __name__ == "__main__":
    raise SystemExit(main())
