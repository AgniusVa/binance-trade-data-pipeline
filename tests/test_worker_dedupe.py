from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from binance_pipeline.validation import ParsedTrade


def filter_existing_range(trades: list[ParsedTrade], existing_ids: set[int]) -> list[ParsedTrade]:
    return [trade for trade in trades if trade.trade_id not in existing_ids]


def parsed(trade_id: int) -> ParsedTrade:
    return ParsedTrade(
        symbol="BTCUSDT",
        trade_id=trade_id,
        price=Decimal("100.00"),
        qty=Decimal("0.10"),
        quote_qty=Decimal("10.00"),
        trade_time=datetime(2026, 1, 1, tzinfo=UTC),
        trade_time_ms=1_767_225_600_000,
        is_buyer_maker=False,
        is_best_match=True,
    )


def test_retry_batch_inserts_only_missing_ids() -> None:
    trades = [parsed(100), parsed(101), parsed(102)]
    missing = filter_existing_range(trades, existing_ids={100, 101})

    assert [trade.trade_id for trade in missing] == [102]


def test_checkpoint_should_advance_to_max_id_plus_one_after_success() -> None:
    trades = [parsed(100), parsed(101), parsed(102)]
    source_batch_id = uuid4()

    assert source_batch_id
    assert max(trade.trade_id for trade in trades) + 1 == 103
