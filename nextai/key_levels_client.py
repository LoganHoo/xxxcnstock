class KeyLevelsClient:
    def __init__(self, host: str = None, port: int = None):
        self.host = host or "localhost"
        self.port = port or 8000

    def health_check(self) -> bool:
        raise ConnectionError("key_levels服务不可用")

    def get_key_levels(self, code: str, price: float = None) -> dict:
        return {}

    def get_support_resistance(self, code: str) -> dict:
        return {}
