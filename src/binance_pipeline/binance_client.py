from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class RateLimited(Exception):
    retry_after_seconds: float
    message: str = "Binance rate limit reached"


class BinanceClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def historical_trades(self, symbol: str, from_id: int | None, limit: int) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        return self._get("/api/v3/historicalTrades", params)

    def recent_trades(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        return self._get("/api/v3/trades", {"symbol": symbol, "limit": limit})

    def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            raise RateLimited(float(retry_after) if retry_after else 5.0)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"unexpected Binance response shape: {payload!r}")
        return payload
