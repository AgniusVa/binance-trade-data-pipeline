from __future__ import annotations

from datetime import UTC, datetime

from binance_pipeline.validation import validate_batch


def trade(trade_id: int, price: str = "100.00", qty: str = "0.10") -> dict:
    return {
        "id": trade_id,
        "price": price,
        "qty": qty,
        "quoteQty": "10.00",
        "time": 1_700_000_000_000,
        "isBuyerMaker": False,
        "isBestMatch": True,
    }


def test_valid_contiguous_batch() -> None:
    result = validate_batch(
        "BTCUSDT",
        [trade(10), trade(11), trade(12)],
        expected_from_id=10,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert result.ok is True
    assert [row.trade_id for row in result.trades] == [10, 11, 12]
    assert result.observed_min_trade_id == 10
    assert result.observed_max_trade_id == 12


def test_rejects_batch_that_starts_after_expected_id() -> None:
    result = validate_batch(
        "BTCUSDT",
        [trade(11), trade(12)],
        expected_from_id=10,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert result.ok is False
    assert result.code == "MISSING_EXPECTED_ID"
    assert "expected first trade id 10" in result.message


def test_rejects_non_contiguous_ids() -> None:
    result = validate_batch(
        "BTCUSDT",
        [trade(10), trade(12)],
        expected_from_id=10,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert result.ok is False
    assert result.code == "NON_CONTIGUOUS_BATCH"
    assert "expected trade id 11" in result.message


def test_rejects_non_positive_price() -> None:
    result = validate_batch(
        "BTCUSDT",
        [trade(10, price="0")],
        expected_from_id=10,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert result.ok is False
    assert result.code == "INVALID_TRADE"
    assert "price must be positive" in result.message


def test_rejects_invalid_boolean_shape() -> None:
    raw = trade(10)
    raw["isBestMatch"] = "true"

    result = validate_batch(
        "BTCUSDT",
        [raw],
        expected_from_id=10,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert result.ok is False
    assert result.code == "INVALID_TRADE"
    assert "isBestMatch must be boolean" in result.message
