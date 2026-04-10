from scripts.run_strict_pytest import (
    build_environment,
    build_pythonwarnings,
    build_pytest_command,
    resolve_pytest_python,
)


def test_build_pythonwarnings_appends_known_ignores_before_error():
    result = build_pythonwarnings("default")

    assert result.startswith("default,")
    assert "ignore:'asyncio.AbstractEventLoopPolicy' is deprecated and slated for removal in Python 3.16:DeprecationWarning" in result
    assert "ignore:Due to '_pack_'.*_RawValue.*:DeprecationWarning:py_mini_racer._dll" in result
    assert result.endswith("error::DeprecationWarning")


def test_build_environment_disables_plugin_autoload():
    env = build_environment({"PYTHONWARNINGS": "default"})

    assert env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert env["PYTHONWARNINGS"].startswith("default,")


def test_resolve_pytest_python_uses_pytest_shebang(tmp_path, monkeypatch):
    pytest_executable = tmp_path / "pytest"
    pytest_executable.write_text("#!/custom/python\nprint('pytest')\n", encoding="utf-8")
    monkeypatch.setattr("scripts.run_strict_pytest.shutil.which", lambda name: str(pytest_executable))

    assert resolve_pytest_python() == "/custom/python"


def test_build_pytest_command_disables_pytest_asyncio_plugin():
    command = build_pytest_command(["-q"])

    assert command[1:5] == ["-m", "pytest", "-p", "anyio.pytest_plugin"]
    assert command[-1] == "-q"
