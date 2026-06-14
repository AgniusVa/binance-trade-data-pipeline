from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True)
class ParsedTrade:
    symbol: str
    trade_id: int
    price: Decimal
    qty: Decimal
    quote_qty: Decimal
    trade_time: datetime
    trade_time_ms: int
    is_buyer_maker: bool
    is_best_match: bool


@dataclass(frozen=True)
class BatchValidation:
    ok: bool
    trades: tuple[ParsedTrade, ...]
    code: str | None = None
    message: str | None = None
    observed_min_trade_id: int = 0
    observed_max_trade_id: int = 0


def parse_trade(symbol: str, raw: dict[str, Any], now: datetime | None = None) -> ParsedTrade:
    now = now or datetime.now(UTC)
    required = ("id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch")
    missing = [field for field in required if field not in raw]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    trade_id = int(raw["id"])
    price = _positive_decimal(raw["price"], "price")
    qty = _positive_decimal(raw["qty"], "qty")
    quote_qty = _nonnegative_decimal(raw["quoteQty"], "quoteQty")
    trade_time_ms = int(raw["time"])
    trade_time = datetime.fromtimestamp(trade_time_ms / 1000, UTC)
    if trade_time > now + timedelta(minutes=5):
        raise ValueError("trade timestamp is more than 5 minutes in the future")
    if not isinstance(raw["isBuyerMaker"], bool):
        raise ValueError("isBuyerMaker must be boolean")
    if not isinstance(raw["isBestMatch"], bool):
        raise ValueError("isBestMatch must be boolean")

    return ParsedTrade(
        symbol=symbol,
        trade_id=trade_id,
        price=price,
        qty=qty,
        quote_qty=quote_qty,
        trade_time=trade_time,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=raw["isBuyerMaker"],
        is_best_match=raw["isBestMatch"],
    )


def validate_batch(
    symbol: str,
    raw_trades: list[dict[str, Any]],
    expected_from_id: int,
    now: datetime | None = None,
) -> BatchValidation:
    now = now or datetime.now(UTC)
    if not raw_trades:
        return BatchValidation(ok=False, trades=(), code="EMPTY_BATCH", message="Binance returned no trades")

    parsed: list[ParsedTrade] = []
    try:
        for raw in raw_trades:
            parsed.append(parse_trade(symbol, raw, now=now))
    except (InvalidOperation, TypeError, ValueError) as exc:
        ids = [int(raw["id"]) for raw in raw_trades if "id" in raw]
        return BatchValidation(
            ok=False,
            trades=tuple(parsed),
            code="INVALID_TRADE",
            message=str(exc),
            observed_min_trade_id=min(ids) if ids else 0,
            observed_max_trade_id=max(ids) if ids else 0,
        )

    ids = [trade.trade_id for trade in parsed]
    if ids[0] != expected_from_id:
        return BatchValidation(
            ok=False,
            trades=tuple(parsed),
            code="MISSING_EXPECTED_ID",
            message=f"expected first trade id {expected_from_id}, observed {ids[0]}",
            observed_min_trade_id=min(ids),
            observed_max_trade_id=max(ids),
        )

    for previous, current in zip(ids, ids[1:]):
        if current != previous + 1:
            return BatchValidation(
                ok=False,
                trades=tuple(parsed),
                code="NON_CONTIGUOUS_BATCH",
                message=f"expected trade id {previous + 1}, observed {current}",
                observed_min_trade_id=min(ids),
                observed_max_trade_id=max(ids),
            )

    return BatchValidation(
        ok=True,
        trades=tuple(parsed),
        observed_min_trade_id=min(ids),
        observed_max_trade_id=max(ids),
    )


def _positive_decimal(value: Any, field: str) -> Decimal:
    parsed = Decimal(str(value))
    if parsed <= 0:
        raise ValueError(f"{field} must be positive")
    return parsed


def _nonnegative_decimal(value: Any, field: str) -> Decimal:
    parsed = Decimal(str(value))
    if parsed < 0:
        raise ValueError(f"{field} must be nonnegative")
    return parsed
