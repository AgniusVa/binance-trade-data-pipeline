from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    binance_base_url: str
    symbols: tuple[str, ...]
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str
    poll_interval_seconds: float
    gap_grace_seconds: float
    binance_limit: int
    request_timeout_seconds: float


def load_settings() -> Settings:
    symbols = tuple(
        symbol.strip().upper()
        for symbol in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
        if symbol.strip()
    )
    return Settings(
        binance_base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/"),
        symbols=symbols,
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "binance"),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "binance"),
        poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "5")),
        gap_grace_seconds=float(os.getenv("GAP_GRACE_SECONDS", "180")),
        binance_limit=int(os.getenv("BINANCE_LIMIT", "1000")),
        request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10")),
    )
