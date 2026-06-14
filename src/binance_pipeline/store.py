from __future__ import annotations

from datetime import UTC, datetime
from typing import Iterable
from uuid import UUID, uuid4

import clickhouse_connect

from binance_pipeline.validation import ParsedTrade


class ClickHouseStore:
    def __init__(self, host: str, port: int, database: str, username: str, password: str) -> None:
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )

    def latest_checkpoint(self, symbol: str) -> int | None:
        result = self.client.query(
            """
            SELECT next_expected_trade_id
            FROM checkpoint_events
            WHERE symbol = {symbol:String}
            ORDER BY event_time DESC
            LIMIT 1
            """,
            parameters={"symbol": symbol},
        )
        if not result.result_rows:
            return None
        return int(result.result_rows[0][0])

    def record_checkpoint(
        self,
        symbol: str,
        next_expected_trade_id: int,
        status: str,
        source_batch_id: UUID,
        event_time: datetime,
    ) -> None:
        self.client.insert(
            "checkpoint_events",
            [[symbol, next_expected_trade_id, status, source_batch_id, event_time]],
            column_names=[
                "symbol",
                "next_expected_trade_id",
                "status",
                "source_batch_id",
                "event_time",
            ],
        )

    def existing_trade_ids(self, symbol: str, min_id: int, max_id: int) -> set[int]:
        result = self.client.query(
            """
            SELECT trade_id
            FROM trades_final
            WHERE symbol = {symbol:String}
              AND trade_id BETWEEN {min_id:UInt64} AND {max_id:UInt64}
            """,
            parameters={"symbol": symbol, "min_id": min_id, "max_id": max_id},
        )
        return {int(row[0]) for row in result.result_rows}

    def insert_final_trades(
        self,
        trades: Iterable[ParsedTrade],
        source_batch_id: UUID,
        ingested_at: datetime,
    ) -> int:
        rows = [
            [
                trade.symbol,
                trade.trade_id,
                trade.price,
                trade.qty,
                trade.quote_qty,
                trade.trade_time,
                trade.is_buyer_maker,
                trade.is_best_match,
                ingested_at,
                source_batch_id,
            ]
            for trade in trades
        ]
        if not rows:
            return 0
        self.client.insert(
            "trades_final",
            rows,
            column_names=[
                "symbol",
                "trade_id",
                "price",
                "qty",
                "quote_qty",
                "trade_time",
                "is_buyer_maker",
                "is_best_match",
                "ingested_at",
                "source_batch_id",
            ],
        )
        return len(rows)

    def insert_batch(
        self,
        batch_id: UUID,
        symbol: str,
        expected_from_id: int,
        min_trade_id: int,
        max_trade_id: int,
        fetched_count: int,
        inserted_count: int,
        duplicate_count: int,
        status: str,
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        self.client.insert(
            "ingestion_batches",
            [[
                batch_id,
                symbol,
                expected_from_id,
                min_trade_id,
                max_trade_id,
                fetched_count,
                inserted_count,
                duplicate_count,
                status,
                started_at,
                finished_at,
            ]],
            column_names=[
                "batch_id",
                "symbol",
                "expected_from_id",
                "min_trade_id",
                "max_trade_id",
                "fetched_count",
                "inserted_count",
                "duplicate_count",
                "status",
                "started_at",
                "finished_at",
            ],
        )

    def insert_incident(
        self,
        symbol: str,
        code: str,
        severity: str,
        expected_trade_id: int,
        observed_min_trade_id: int,
        observed_max_trade_id: int,
        message: str,
        source_batch_id: UUID,
        created_at: datetime | None = None,
    ) -> None:
        self.client.insert(
            "quality_incidents",
            [[
                uuid4(),
                symbol,
                code,
                severity,
                expected_trade_id,
                observed_min_trade_id,
                observed_max_trade_id,
                message,
                source_batch_id,
                created_at or datetime.now(UTC),
            ]],
            column_names=[
                "incident_id",
                "symbol",
                "code",
                "severity",
                "expected_trade_id",
                "observed_min_trade_id",
                "observed_max_trade_id",
                "message",
                "source_batch_id",
                "created_at",
            ],
        )

    def stage_raw_trades(
        self,
        symbol: str,
        raw_trades: list[dict],
        reason: str,
        source_batch_id: UUID,
        staged_at: datetime,
    ) -> None:
        rows = []
        for raw in raw_trades:
            rows.append([
                symbol,
                int(raw.get("id", 0) or 0),
                str(raw.get("price", "")),
                str(raw.get("qty", "")),
                str(raw.get("quoteQty", "")),
                int(raw.get("time", 0) or 0),
                bool(raw.get("isBuyerMaker", False)),
                bool(raw.get("isBestMatch", False)),
                reason,
                source_batch_id,
                staged_at,
            ])
        if rows:
            self.client.insert(
                "staged_trades",
                rows,
                column_names=[
                    "symbol",
                    "trade_id",
                    "price",
                    "qty",
                    "quote_qty",
                    "trade_time_ms",
                    "is_buyer_maker",
                    "is_best_match",
                    "reason",
                    "source_batch_id",
                    "staged_at",
                ],
            )
