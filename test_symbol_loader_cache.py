from __future__ import annotations

import symbol_loader


def test_cached_symbol_universe_reused(monkeypatch):
    calls = {"count": 0}

    def fake_loader(limit=None):
        calls["count"] += 1
        return ["AAPL", "MSFT", "NVDA"]

    monkeypatch.setattr(symbol_loader, "_load_symbols_uncached", fake_loader)
    monkeypatch.setattr(symbol_loader, "_SYMBOL_CACHE", [])
    monkeypatch.setattr(symbol_loader, "_SYMBOL_CACHE_TS", 0.0)

    first = symbol_loader.get_cached_symbols()
    second = symbol_loader.get_cached_symbols()

    assert first == second
    assert calls["count"] == 1


def test_force_refresh_reloads_symbols(monkeypatch):
    calls = {"count": 0}

    def fake_loader(limit=None):
        calls["count"] += 1
        return ["AAPL", "MSFT", "NVDA", f"X{calls['count']}"]

    monkeypatch.setattr(symbol_loader, "_load_symbols_uncached", fake_loader)
    monkeypatch.setattr(symbol_loader, "_SYMBOL_CACHE", [])
    monkeypatch.setattr(symbol_loader, "_SYMBOL_CACHE_TS", 0.0)

    first = symbol_loader.get_cached_symbols()
    second = symbol_loader.get_cached_symbols(force_refresh=True)

    assert calls["count"] == 2
    assert first != second
