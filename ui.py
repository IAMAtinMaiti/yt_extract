import duckdb
from pathlib import Path
from datetime import datetime, time

import pandas as pd
import streamlit as st

# Adjust if your DB lives elsewhere
DB_PATH = Path(__file__).resolve().parent / "data" / "yt_trending.duckdb"


@st.cache_data
def load_snapshots(start_ts=None, end_ts=None, limit: int | None = None) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        query = "SELECT * FROM trending_snapshots"
        params: list = []

        if start_ts and end_ts:
            query += " WHERE snapshot_timestamp BETWEEN ? AND ?"
            params.extend([start_ts, end_ts])

        query += " ORDER BY snapshot_timestamp DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        df = con.execute(query, params).df()
    finally:
        con.close()

    return df


def main() -> None:
    st.set_page_config(page_title="YouTube Trending Snapshots", layout="wide")
    st.title("YouTube Trending Snapshots (DuckDB)")

    if not DB_PATH.exists():
        st.error(f"DuckDB database not found at: {DB_PATH}")
        st.stop()

    st.sidebar.header("Filters")

    # Date range filter
    date_range = st.sidebar.date_input(
        "Snapshot date range (optional)",
        [],
        help="Pick a start and end date to filter snapshots.",
    )

    start_ts = end_ts = None
    if isinstance(date_range, list) and len(date_range) == 2:
        start_date, end_date = date_range
        start_ts = datetime.combine(start_date, time.min)
        end_ts = datetime.combine(end_date, time.max)

    # Row limit
    limit = st.sidebar.number_input(
        "Max rows to display",
        min_value=1,
        max_value=1000,
        value=100,
        step=10,
    )

    df = load_snapshots(start_ts=start_ts, end_ts=end_ts, limit=int(limit))

    if df.empty:
        st.info("No snapshot metadata found in DuckDB yet.")
        return

    st.subheader("Snapshot metadata")
    st.dataframe(df, use_container_width=True)

    # Optional: select a row and offer PDF download
    st.subheader("Download a snapshot PDF")
    idx = st.selectbox(
        "Select snapshot",
        options=df.index,
        format_func=lambda i: f"{df.loc[i, 'snapshot_timestamp']} — {df.loc[i, 'file_path']}",
    )

    file_path = Path(df.loc[idx, "file_path"])
    if not file_path.exists():
        st.warning(f"PDF not found on disk: {file_path}")
    else:
        with open(file_path, "rb") as f:
            st.download_button(
                label="Download selected PDF",
                data=f.read(),
                file_name=file_path.name,
                mime="application/pdf",
            )


if __name__ == "__main__":
    main()