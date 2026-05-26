from fastapi.testclient import TestClient
import main


def _async_return(value):
    async def _fn(*_args, **_kwargs):
        return value
    return _fn


def test_broker_three_tracker_two_returns_three(monkeypatch):
    monkeypatch.setattr(main.database, "get_all_positions", _async_return([
        {"symbol": "TSCO", "status": "OPEN", "quantity": 10},
        {"symbol": "AZTA", "status": "OPEN", "quantity": 20},
        {"symbol": "CLDX", "status": "OPEN", "quantity": 30},
    ]))
    monkeypatch.setattr(main.database, "get_latest_broker_sync_snapshot", _async_return({
        "positions": [
            {"symbol": "TSCO", "position": 10},
            {"symbol": "AZTA", "position": 20},
            {"symbol": "CLDX", "position": 30},
        ]
    }))
    monkeypatch.setattr(main.live_position_tracker, "get_tracker_status", _async_return({
        "positions": [
            {"symbol": "TSCO", "source": "live_position_tracker", "profit_amount": 1},
            {"symbol": "AZTA", "source": "live_position_tracker", "profit_amount": 2},
        ]
    }))
    payload = TestClient(main.app).get("/api/positions").json()
    assert len(payload["positions"]) == 3
    assert payload["missing_tracker_enrichment_symbols"] == ["CLDX"]


def test_tracker_empty_broker_positions_still_returned(monkeypatch):
    monkeypatch.setattr(main.database, "get_all_positions", _async_return([]))
    monkeypatch.setattr(main.database, "get_latest_broker_sync_snapshot", _async_return({
        "positions": [{"symbol": "TSCO", "position": 10}]
    }))
    monkeypatch.setattr(main.live_position_tracker, "get_tracker_status", _async_return({"positions": []}))
    payload = TestClient(main.app).get("/api/positions").json()
    assert [p["symbol"] for p in payload["positions"]] == ["TSCO"]
    assert payload["positions"][0]["enrichment_stale"] is True


def test_tracker_enrichment_updates_but_does_not_delete(monkeypatch):
    monkeypatch.setattr(main.database, "get_all_positions", _async_return([
        {"symbol": "TSCO", "status": "OPEN", "quantity": 10, "profit_amount": 0},
        {"symbol": "AZTA", "status": "OPEN", "quantity": 20, "profit_amount": 0},
    ]))
    monkeypatch.setattr(main.database, "get_latest_broker_sync_snapshot", _async_return({
        "positions": [{"symbol": "TSCO", "position": 10}, {"symbol": "AZTA", "position": 20}]
    }))
    monkeypatch.setattr(main.live_position_tracker, "get_tracker_status", _async_return({
        "positions": [{"symbol": "TSCO", "source": "live_position_tracker", "profit_amount": 12.5}]
    }))
    payload = TestClient(main.app).get("/api/positions").json()
    by_symbol = {p["symbol"]: p for p in payload["positions"]}
    assert set(by_symbol) == {"TSCO", "AZTA"}
    assert by_symbol["TSCO"]["profit_amount"] == 12.5
    assert by_symbol["AZTA"]["enrichment_stale"] is True


def test_open_count_stable_across_partial_refreshes(monkeypatch):
    monkeypatch.setattr(main.database, "get_all_positions", _async_return([
        {"symbol": "TSCO", "status": "OPEN", "quantity": 10},
        {"symbol": "AZTA", "status": "OPEN", "quantity": 20},
        {"symbol": "CLDX", "status": "OPEN", "quantity": 30},
    ]))
    monkeypatch.setattr(main.database, "get_latest_broker_sync_snapshot", _async_return({
        "positions": [
            {"symbol": "TSCO", "position": 10},
            {"symbol": "AZTA", "position": 20},
            {"symbol": "CLDX", "position": 30},
        ]
    }))

    states = iter([
        {"positions": [{"symbol": "TSCO", "source": "live_position_tracker"}, {"symbol": "AZTA", "source": "live_position_tracker"}, {"symbol": "CLDX", "source": "live_position_tracker"}]},
        {"positions": [{"symbol": "TSCO", "source": "live_position_tracker"}, {"symbol": "AZTA", "source": "live_position_tracker"}]},
    ])

    async def _tracker_status():
        return next(states)

    monkeypatch.setattr(main.live_position_tracker, "get_tracker_status", _tracker_status)
    client = TestClient(main.app)
    first = client.get('/api/positions').json()
    second = client.get('/api/positions').json()
    assert len(first["positions"]) == 3
    assert len(second["positions"]) == 3
