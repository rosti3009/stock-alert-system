from fastapi.testclient import TestClient

import main


class _DummyDB:
    async def fetch_all(self, *_args, **_kwargs):
        return []

    async def fetch_one(self, *_args, **_kwargs):
        return {
            "connected": 0,
            "synced_at": None,
            "positions_json": None,
            "open_orders_json": "bad-json",
            "executions_json": "[]",
            "errors_json": None,
        }

    async def save_broker_sync_snapshot(self, *_args, **_kwargs):
        return None


def test_orders_endpoint_returns_stable_json(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())
    payload = TestClient(main.app).get("/api/orders").json()
    assert isinstance(payload, dict)
    assert "ok" in payload
    assert "errors" in payload
    assert payload["count"] == 0


def test_broker_sync_status_handles_malformed_payload(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())
    payload = TestClient(main.app).get("/api/broker-sync/status").json()
    assert isinstance(payload, dict)
    assert payload["open_orders_count"] == 0
    assert payload["executions_count"] == 0
    assert payload["positions_count"] == 0
    assert "source" in payload


def test_broker_sync_run_handles_none_result(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())

    async def _none_result():
        return None

    monkeypatch.setattr(main.broker_sync, "run_broker_sync_once", _none_result)
    payload = TestClient(main.app).post("/api/broker-sync/run").json()
    assert isinstance(payload, dict)
    assert payload["ok"] is False
    assert isinstance(payload["errors"], list)
    assert "source" in payload
