from datetime import datetime, time
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from fetch_and_query_duckdb import run_query

DB_PATH = Path(__file__).resolve().parent / "data" / "yt_trending.duckdb"


def main() -> None:
    st.set_page_config(page_title="YouTube Trending Snapshots", layout="wide")
    st.title("YouTube Trending Snapshots (DuckDB)")

    if not DB_PATH.exists():
        st.error(
            f"DuckDB database not found at: {DB_PATH}. Use **Fetch new snapshot** or run the pipeline first."
        )
        st.stop()

    sql = st.text_area(
        "SQL (SELECT only)",
        value=(
            "SELECT table_catalog, table_schema, table_name\n"
            "FROM information_schema.tables\n"
            "ORDER BY table_catalog, table_schema, table_name;"
        ),
        height=120,
        help="Run a read-only query. Only SELECT statements are allowed.",
    )
    if st.button("Run query", key="custom_run"):
        sql_stripped = sql.strip().upper()
        if not sql_stripped.startswith("SELECT"):
            st.error("Only SELECT queries are allowed.")
        else:
            con = duckdb.connect(str(DB_PATH), read_only=True)
            try:
                result = run_query(con, sql.strip())
                df_q = result.df()
                st.dataframe(df_q, use_container_width=True)
            except Exception as e:
                st.error(f"Query error: {e}")
            finally:
                con.close()


if __name__ == "__main__":
    main()
