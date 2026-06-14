from __future__ import annotations

import os

import clickhouse_connect
import streamlit as st


@st.cache_resource
def client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DATABASE", "binance"),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "binance"),
    )


def query(sql: str):
    return client().query_df(sql)


def symbol_where(symbol: str) -> str:
    if symbol == "All":
        return ""
    return f"WHERE symbol = '{symbol}'"


def stringify_id_columns(df):
    id_columns = [
        "min_trade_id",
        "max_trade_id",
        "next_expected_trade_id",
        "expected_from_id",
        "observed_min_trade_id",
        "observed_max_trade_id",
    ]
    for column in id_columns:
        if column in df.columns:
            df[column] = df[column].astype("string")
    return df


st.set_page_config(page_title="Binance Pipeline", layout="wide")
st.title("Binance Trade Pipeline")

if st.button("Refresh"):
    st.cache_resource.clear()
    st.rerun()

symbols = query(
    """
    SELECT symbol
    FROM
    (
        SELECT DISTINCT symbol FROM trades_final
        UNION DISTINCT
        SELECT DISTINCT symbol FROM checkpoint_events
        UNION DISTINCT
        SELECT DISTINCT symbol FROM staged_trades
        UNION DISTINCT
        SELECT DISTINCT symbol FROM quality_incidents
    )
    ORDER BY symbol
    """
)
symbol_options = ["All"] + symbols["symbol"].tolist()
selected_symbol = st.selectbox("Symbol", symbol_options)
where_symbol = symbol_where(selected_symbol)

summary = query(
    f"""
    SELECT
        symbol,
        count() AS rows,
        min(trade_id) AS min_trade_id,
        max(trade_id) AS max_trade_id,
        max(trade_time) AS newest_trade_time,
        round(dateDiff('second', max(trade_time), now64(3)), 2) AS freshness_lag_seconds,
        round(min(dateDiff('millisecond', trade_time, ingested_at)) / 1000, 3) AS min_latency_seconds,
        round(avg(dateDiff('millisecond', trade_time, ingested_at)) / 1000, 3) AS avg_latency_seconds,
        round(max(dateDiff('millisecond', trade_time, ingested_at)) / 1000, 3) AS max_latency_seconds
    FROM trades_final
    {where_symbol}
    GROUP BY symbol
    ORDER BY symbol
    """
)

checkpoint = query(
    f"""
    SELECT
        symbol,
        argMax(next_expected_trade_id, event_time) AS next_expected_trade_id,
        argMax(status, event_time) AS status,
        max(event_time) AS updated_at
    FROM checkpoint_events
    {where_symbol}
    GROUP BY symbol
    ORDER BY symbol
    """
)

buffered = query(
    f"""
    SELECT symbol, reason, count() AS rows, min(trade_id) AS min_trade_id, max(trade_id) AS max_trade_id
    FROM staged_trades
    {where_symbol}
    GROUP BY symbol, reason
    ORDER BY symbol, reason
    """
)

incidents = query(
    f"""
    SELECT created_at, symbol, severity, code, expected_trade_id, observed_min_trade_id, observed_max_trade_id, message
    FROM quality_incidents
    {where_symbol}
    ORDER BY created_at DESC
    LIMIT 100
    """
)

recent_latency = query(
    f"""
    SELECT round(avg(dateDiff('millisecond', trade_time, ingested_at)) / 1000, 3) AS avg_latency_seconds
    FROM trades_final
    WHERE ingested_at >= now64(3) - INTERVAL 5 MINUTE
    {"AND symbol = '" + selected_symbol + "'" if selected_symbol != "All" else ""}
    """
)

batches = query(
    f"""
    SELECT
        finished_at,
        symbol,
        status,
        expected_from_id,
        min_trade_id,
        max_trade_id,
        fetched_count,
        inserted_count,
        duplicate_count
    FROM ingestion_batches
    {where_symbol}
    ORDER BY finished_at DESC
    LIMIT 100
    """
)

summary = stringify_id_columns(summary)
checkpoint = stringify_id_columns(checkpoint)
buffered = stringify_id_columns(buffered)
incidents = stringify_id_columns(incidents)
batches = stringify_id_columns(batches)

col1, col2, col3 = st.columns(3)
col1.metric("Final rows", int(summary["rows"].sum()) if not summary.empty else 0)
col2.metric("Buffered rows", int(buffered["rows"].sum()) if not buffered.empty else 0)
recent_avg = recent_latency["avg_latency_seconds"].iloc[0] if not recent_latency.empty else None
col3.metric(
    "Avg latency, last 5m",
    "n/a" if recent_avg is None or recent_avg != recent_avg else f"{recent_avg:.3f}s",
)

col4, _ = st.columns([1, 2])
col4.metric("Incidents", len(incidents))

st.subheader("Final table health")
st.dataframe(summary, use_container_width=True, hide_index=True)

st.subheader("Checkpoints")
st.dataframe(checkpoint, use_container_width=True, hide_index=True)

st.subheader("Buffered / quarantined rows")
st.dataframe(buffered, use_container_width=True, hide_index=True)

st.subheader("Quality incidents")
st.dataframe(incidents, use_container_width=True, hide_index=True)

st.subheader("Recent batches")
st.dataframe(batches, use_container_width=True, hide_index=True)
