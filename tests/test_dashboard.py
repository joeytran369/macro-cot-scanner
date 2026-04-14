"""
Dashboard Tests — logic + API + data integrity
Run: pytest tests/ -v
"""

import pytest
from httpx import ASGITransport, AsyncClient

from server import (
    MARKETS,
    VALIDATED_EDGE,
    app,
    build_snapshot,
    cache,
    compute_bias,
    fetch_daily_weekly_changes,
    fetch_macro,
    fetch_prices,
    get_current_session,
)


class TestComputeBias:
    def test_bullish_normal_pair(self):
        cache["cot"]["AUD"] = {"sm_index": 90, "sm_net": 80000, "date": "2026-04-01"}
        result = compute_bias()
        assert result["AUD"]["bias"] == "BUY"
        assert result["AUD"]["bias_raw"] == "BULLISH"

    def test_bearish_normal_pair(self):
        cache["cot"]["EUR"] = {"sm_index": 5, "sm_net": 500, "date": "2026-04-01"}
        result = compute_bias()
        assert result["EUR"]["bias"] == "SELL"
        assert result["EUR"]["bias_raw"] == "BEARISH"

    def test_bullish_inverted_pair(self):
        cache["cot"]["CAD"] = {"sm_index": 85, "sm_net": 50000, "date": "2026-04-01"}
        result = compute_bias()
        assert result["CAD"]["bias"] == "SELL"
        assert result["CAD"]["bias_raw"] == "BULLISH"

    def test_bearish_inverted_pair(self):
        cache["cot"]["CHF"] = {"sm_index": 10, "sm_net": -30000, "date": "2026-04-01"}
        result = compute_bias()
        assert result["CHF"]["bias"] == "BUY"
        assert result["CHF"]["bias_raw"] == "BEARISH"

    def test_neutral_range(self):
        cache["cot"]["GBP"] = {"sm_index": 50, "sm_net": 10000, "date": "2026-04-01"}
        result = compute_bias()
        assert result["GBP"]["bias"] == ""
        assert result["GBP"]["bias_raw"] == "NEUTRAL"

    def test_threshold_boundaries(self):
        cache["cot"]["EUR"] = {"sm_index": 25, "sm_net": 5000, "date": "2026-04-01"}
        assert compute_bias()["EUR"]["bias_raw"] == "NEUTRAL"

        cache["cot"]["EUR"] = {"sm_index": 24.9, "sm_net": 5000, "date": "2026-04-01"}
        assert compute_bias()["EUR"]["bias_raw"] == "BEARISH"

        cache["cot"]["EUR"] = {"sm_index": 75.1, "sm_net": 100000, "date": "2026-04-01"}
        assert compute_bias()["EUR"]["bias_raw"] == "BULLISH"

    def test_edge_validation_present(self):
        result = compute_bias()
        assert result["EUR"]["edge"] == "High"
        assert result["CAD"]["edge"] == "High"
        assert result["GBP"]["edge"] == ""
        assert result["Gold"]["edge"] == ""


class TestSession:
    def test_session_returns_valid(self):
        result = get_current_session()
        assert "name" in result
        assert "color" in result


class TestMarketConfig:
    def test_all_markets_have_required_fields(self):
        for key, m in MARKETS.items():
            for field in ("oanda", "cftc", "name", "invert"):
                assert field in m, f"{key} missing {field}"

    def test_validated_edge_covers_all_markets(self):
        for key in MARKETS:
            assert key in VALIDATED_EDGE

    def test_inverted_pairs_correct(self):
        assert MARKETS["CAD"]["invert"] is True
        assert MARKETS["CHF"]["invert"] is True
        assert MARKETS["JPY"]["invert"] is True
        assert MARKETS["EUR"]["invert"] is False
        assert MARKETS["Gold"]["invert"] is False


class TestSnapshot:
    def test_snapshot_structure(self):
        cache["prices"]["EUR"] = {"bid": 1.15, "ask": 1.1502, "mid": 1.1501, "spread": 2.0}
        cache["daily_change"]["EUR"] = 0.3
        cache["weekly_change"]["EUR"] = -1.2
        cache["cot"]["EUR"] = {"sm_index": 10, "sm_net": 500, "date": "2026-04-01"}
        cache["macro"]["DXY"] = 120.5

        snap = build_snapshot()
        for field in ("markets", "macro", "fear_greed", "session", "timestamp"):
            assert field in snap

        assert isinstance(snap["markets"], list)
        assert len(snap["markets"]) == len(MARKETS)

    def test_no_internal_fields_leaked(self):
        cache["macro"]["_fred_ts"] = 12345
        snap = build_snapshot()
        assert "_fred_ts" not in snap["macro"]


@pytest.mark.asyncio
async def test_api_snapshot():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/snapshot")
    assert resp.status_code == 200
    assert "markets" in resp.json()


@pytest.mark.asyncio
async def test_api_index_html():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/")
    assert resp.status_code == 200
    assert "Trading Desk" in resp.text
