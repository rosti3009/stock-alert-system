import asyncio

import pytest

import config
import database
import strategy_portfolio
from position_sizing_engine import PositionSizingInput, evaluate_position_sizing


BASE_ROW = {
    "symbol": "AAPL",
    "price": 100.0,
    "stop_loss": 90.0,
    "atr": 2.0,
    "avg_volume": 2_000_000,
    "volume": 2_000_000,
    "relative_volume": 1.2,
    "bid": 99.95,
    "ask": 100.05,
}

BASE_CONTEXT = {
    "account_equity": 10_000.0,
    "market_regime": {"regime": "BULL", "position_size_factor": 1.0},
    "execution_quality": {"state": "EXECUTION_SAFE", "blocks_buy": False, "metrics": {"spread_percent": 0.1}},
    "portfolio_risk": {
        "total_portfolio_exposure_percent": 0.0,
        "total_open_risk_percent": 0.0,
        "daily_drawdown_percent": 0.0,
        "unrealized_drawdown_percent": 0.0,
        "exposure_by_sector": [],
        "exposure_by_symbol": [],
    },
}


@pytest.fixture(autouse=True)
def capital_config(monkeypatch):
    monkeypatch.setattr(config, "POSITION_LIMIT_MODE", config.POSITION_LIMIT_MODE_CAPITAL_BASED, raising=False)
    monkeypatch.setattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 10_000.0)
    monkeypatch.setattr(config, "SWING_CAPITAL_PERCENT", 50.0)
    monkeypatch.setattr(config, "INTRADAY_CAPITAL_PERCENT", 40.0)
    monkeypatch.setattr(config, "RESERVE_CAPITAL_PERCENT", 10.0)
    monkeypatch.setattr(config, "MAX_POSITION_PERCENT", 10.0)
    monkeypatch.setattr(config, "MIN_POSITION_SIZE_USD", 500.0)
    monkeypatch.setattr(config, "MAX_TOTAL_EXPOSURE_PERCENT", 90.0)
    monkeypatch.setattr(config, "MAX_DAILY_DRAWDOWN_PERCENT", 5.0)
    monkeypatch.setattr(config, "IBKR_PAPER_TRADING", False)
    monkeypatch.setattr(config, "IBKR_ENABLE_REAL_TRADING", False)
    monkeypatch.setattr(config, "ALLOW_FRACTIONAL_SHARES", False)


def sizing(row=None, open_positions=None, portfolio_risk=None):
    payload = dict(BASE_CONTEXT)
    payload["open_positions"] = open_positions or []
    if portfolio_risk is not None:
        payload["portfolio_risk"] = {**BASE_CONTEXT["portfolio_risk"], **portfolio_risk}
    return evaluate_position_sizing(PositionSizingInput(row=row or BASE_ROW, **payload))


def test_capital_based_allows_more_than_old_max_position_count():
    open_positions = [
        {"symbol": f"OLD{i}", "status": "OPEN", "strategy_type": "SWING", "buy_price": 100.0, "quantity": 1.0}
        for i in range(10)
    ]

    result = sizing(open_positions=open_positions)

    assert result["blocks_buy"] is False
    assert result["recommended_position_size_usd"] >= 500.0


def test_fixed_count_still_respects_max_open_positions(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "positions.db"))
    monkeypatch.setattr(database, "DB_PATH", str(tmp_path / "positions.db"))
    monkeypatch.setattr(config, "POSITION_LIMIT_MODE", config.POSITION_LIMIT_MODE_FIXED_COUNT, raising=False)

    async def run():
        await database.add_position({"symbol": "AAA", "buy_price": 10.0, "quantity": 1.0}, max_open_positions=1)
        with pytest.raises(ValueError, match="Maximum open positions reached"):
            await database.add_position({"symbol": "BBB", "buy_price": 10.0, "quantity": 1.0}, max_open_positions=1)

    asyncio.run(run())


def test_reserve_capital_is_not_used():
    # Swing bucket is $5,000; reserve is $1,000. Once swing usage leaves less than the
    # $500 minimum, sizing must not dip into reserve capital.
    open_positions = [{"status": "OPEN", "strategy_type": "SWING", "buy_price": 100.0, "quantity": 46.0}]

    result = sizing(open_positions=open_positions)

    assert result["blocks_buy"] is True
    assert "Position size below minimum threshold" in result["block_reasons"]


def test_swing_cannot_use_intraday_capital():
    open_positions = [{"status": "OPEN", "strategy_type": "SWING", "buy_price": 100.0, "quantity": 46.0}]

    result = sizing(row={**BASE_ROW, "strategy_type": "SWING"}, open_positions=open_positions)

    assert result["blocks_buy"] is True
    assert result["allocated_equity"] == 5000.0
    assert "Position size below minimum threshold" in result["block_reasons"]


def test_intraday_cannot_use_swing_capital():
    open_positions = [{"status": "OPEN", "strategy_type": "INTRADAY", "buy_price": 100.0, "quantity": 36.0}]

    result = sizing(row={**BASE_ROW, "strategy_type": "INTRADAY"}, open_positions=open_positions)

    assert result["blocks_buy"] is True
    assert result["allocated_equity"] == 4000.0
    assert "Position size below minimum threshold" in result["block_reasons"]


def test_micro_position_below_500_is_rejected():
    result = sizing(row={**BASE_ROW, "price": 400.0, "stop_loss": 360.0, "atr": 4.0})

    assert result["blocks_buy"] is True
    assert "Position size below minimum threshold" in result["block_reasons"]


def test_portfolio_exposure_blocks_when_exceeded():
    result = sizing(portfolio_risk={"total_portfolio_exposure_percent": 85.0})

    assert result["blocks_buy"] is True
    assert any("Portfolio exposure limit exceeded" in reason for reason in result["block_reasons"])


def test_strategy_allocation_status_exposes_capital_based_settings(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "allocation.db"))
    monkeypatch.setattr(database, "DB_PATH", str(tmp_path / "allocation.db"))

    status = asyncio.run(strategy_portfolio.build_strategy_allocation_status())

    assert status["position_limit_mode"] == "CAPITAL_BASED"
    assert status["min_position_size_usd"] == 500.0
    assert status["max_portfolio_exposure"] == 90.0
    assert status["max_position_size"] == 10.0
    assert status["allocations"] == {"SWING": 50.0, "INTRADAY": 40.0, "RESERVE": 10.0}
