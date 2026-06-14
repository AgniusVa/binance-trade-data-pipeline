from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from uuid import uuid4

import requests

from binance_pipeline.binance_client import BinanceClient, RateLimited
from binance_pipeline.config import load_settings
from binance_pipeline.store import ClickHouseStore
from binance_pipeline.validation import validate_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("binance_pipeline.worker")


class Worker:
    def __init__(self) -> None:
        self.settings = load_settings()
        self.binance = BinanceClient(
            self.settings.binance_base_url,
            self.settings.request_timeout_seconds,
        )
        self.store = ClickHouseStore(
            self.settings.clickhouse_host,
            self.settings.clickhouse_port,
            self.settings.clickhouse_database,
            self.settings.clickhouse_user,
            self.settings.clickhouse_password,
        )
        self.missing_since: dict[str, datetime] = {}

    def run_forever(self) -> None:
        logger.info("starting worker for symbols=%s", ",".join(self.settings.symbols))
        while True:
            made_progress = False
            for symbol in self.settings.symbols:
                try:
                    made_progress = self.process_symbol(symbol) or made_progress
                except RateLimited as exc:
                    logger.warning("rate limited by Binance; sleeping %.1fs", exc.retry_after_seconds)
                    time.sleep(exc.retry_after_seconds)
                except requests.RequestException as exc:
                    logger.exception("Binance request failed for %s: %s", symbol, exc)
                except Exception:
                    logger.exception("unexpected worker failure for %s", symbol)
            if not made_progress:
                time.sleep(self.settings.poll_interval_seconds)

    def process_symbol(self, symbol: str) -> bool:
        expected_from_id = self.store.latest_checkpoint(symbol)
        if expected_from_id is None:
            expected_from_id = self.bootstrap_symbol(symbol)

        batch_id = uuid4()
        started_at = datetime.now(UTC)
        raw_trades = self.binance.historical_trades(
            symbol=symbol,
            from_id=expected_from_id,
            limit=self.settings.binance_limit,
        )
        if not raw_trades:
            logger.info("%s no new trades at expected id %s", symbol, expected_from_id)
            return False

        validation = validate_batch(symbol, raw_trades, expected_from_id, now=started_at)
        finished_at = datetime.now(UTC)

        if not validation.ok:
            self.handle_invalid_batch(
                symbol=symbol,
                expected_from_id=expected_from_id,
                raw_trades=raw_trades,
                batch_id=batch_id,
                code=validation.code or "INVALID_BATCH",
                message=validation.message or "batch failed validation",
                observed_min=validation.observed_min_trade_id,
                observed_max=validation.observed_max_trade_id,
                started_at=started_at,
                finished_at=finished_at,
            )
            return False

        min_id = validation.observed_min_trade_id
        max_id = validation.observed_max_trade_id
        existing = self.store.existing_trade_ids(symbol, min_id, max_id)
        to_insert = [trade for trade in validation.trades if trade.trade_id not in existing]
        inserted_count = self.store.insert_final_trades(to_insert, batch_id, finished_at)
        duplicate_count = len(validation.trades) - inserted_count
        next_expected_trade_id = max_id + 1

        self.store.insert_batch(
            batch_id=batch_id,
            symbol=symbol,
            expected_from_id=expected_from_id,
            min_trade_id=min_id,
            max_trade_id=max_id,
            fetched_count=len(raw_trades),
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            status="PROMOTED",
            started_at=started_at,
            finished_at=finished_at,
        )
        self.store.record_checkpoint(
            symbol=symbol,
            next_expected_trade_id=next_expected_trade_id,
            status="PROMOTED",
            source_batch_id=batch_id,
            event_time=finished_at,
        )
        self.missing_since.pop(symbol, None)
        logger.info(
            "%s promoted ids %s-%s inserted=%s duplicates=%s",
            symbol,
            min_id,
            max_id,
            inserted_count,
            duplicate_count,
        )
        return len(raw_trades) == self.settings.binance_limit

    def bootstrap_symbol(self, symbol: str) -> int:
        batch_id = uuid4()
        now = datetime.now(UTC)
        recent = self.binance.recent_trades(symbol=symbol, limit=1)
        if not recent:
            raise RuntimeError(f"cannot bootstrap {symbol}: recent trades endpoint returned no rows")
        next_expected = int(recent[-1]["id"]) + 1
        self.store.record_checkpoint(
            symbol=symbol,
            next_expected_trade_id=next_expected,
            status="BOOTSTRAPPED",
            source_batch_id=batch_id,
            event_time=now,
        )
        logger.info("%s bootstrapped at next_expected_trade_id=%s", symbol, next_expected)
        return next_expected

    def handle_invalid_batch(
        self,
        symbol: str,
        expected_from_id: int,
        raw_trades: list[dict],
        batch_id,
        code: str,
        message: str,
        observed_min: int,
        observed_max: int,
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        self.store.stage_raw_trades(symbol, raw_trades, code, batch_id, finished_at)
        self.store.insert_batch(
            batch_id=batch_id,
            symbol=symbol,
            expected_from_id=expected_from_id,
            min_trade_id=observed_min,
            max_trade_id=observed_max,
            fetched_count=len(raw_trades),
            inserted_count=0,
            duplicate_count=0,
            status=code,
            started_at=started_at,
            finished_at=finished_at,
        )

        severity = "warning"
        if code in {"MISSING_EXPECTED_ID", "NON_CONTIGUOUS_BATCH"}:
            first_seen = self.missing_since.setdefault(symbol, finished_at)
            elapsed = (finished_at - first_seen).total_seconds()
            if elapsed < self.settings.gap_grace_seconds:
                logger.warning(
                    "%s validation blocked by %s within grace %.1fs/%.1fs: %s",
                    symbol,
                    code,
                    elapsed,
                    self.settings.gap_grace_seconds,
                    message,
                )
                return
            severity = "critical"

        self.store.insert_incident(
            symbol=symbol,
            code=code,
            severity=severity,
            expected_trade_id=expected_from_id,
            observed_min_trade_id=observed_min,
            observed_max_trade_id=observed_max,
            message=message,
            source_batch_id=batch_id,
            created_at=finished_at,
        )
        logger.error("%s incident %s: %s", symbol, code, message)


def main() -> None:
    Worker().run_forever()


if __name__ == "__main__":
    main()
