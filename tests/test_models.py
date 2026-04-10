from core.models import StockQuote, LimitUpSignal, SignalLevel


def test_stock_quote_creation():
    """测试股票行情模型"""
    quote = StockQuote(
        code="000001",
        name="平安银行",
        price=10.5,
        change_pct=2.5,
        volume=1000000,
        turnover_rate=5.5
    )
    assert quote.code == "000001"
    assert quote.name == "平安银行"
    assert quote.price == 10.5


def test_limit_up_signal_creation():
    """测试涨停信号模型"""
    signal = LimitUpSignal(
        code="000001",
        name="平安银行",
        change_pct=10.0,
        limit_time="09:30:00",
        seal_amount=100000000,
        signal_level=SignalLevel.S
    )
    assert signal.signal_level == SignalLevel.S
    assert signal.continuous_limit == 1


def test_signal_level_enum():
    """测试信号等级枚举"""
    assert SignalLevel.S.value == "S"
    assert SignalLevel.A.value == "A"
    assert SignalLevel.B.value == "B"
    assert SignalLevel.C.value == "C"
