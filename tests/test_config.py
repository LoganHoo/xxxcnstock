from core.config import Settings, get_settings


def test_settings_default_values():
    """测试配置默认值"""
    settings = Settings()
    assert settings.APP_NAME == "XCNStock"
    assert settings.DEBUG is False


def test_settings_environment_override(monkeypatch):
    """测试环境变量覆盖"""
    monkeypatch.setenv("DEBUG", "true")
    settings = Settings()
    assert settings.DEBUG is True


def test_get_settings_singleton():
    """测试单例模式"""
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2


def test_settings_ignore_unmodeled_env_vars(monkeypatch):
    """测试未建模环境变量不会导致配置加载失败"""
    monkeypatch.setenv("DB_URL", "mysql+pymysql://user:pass@localhost:3306/test")

    settings = Settings()

    assert settings.DB_HOST == "49.233.10.199"
