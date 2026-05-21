import config


def test_intraday_aggressive_threshold_and_risk_limits():
    intraday = config.PAPER_TRAINING_PROFILES["INTRADAY_AGGRESSIVE"]["intraday"]
    assert intraday["min_score_to_buy"] == 60
    assert intraday["risk_per_trade_percent"] == 1.25
    assert intraday["max_daily_trades"] == 20
    assert intraday["max_open_positions"] == 5


def test_real_trading_still_disabled_in_paper_mode():
    assert config.IBKR_ENABLE_REAL_TRADING is False
