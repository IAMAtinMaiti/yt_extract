"""
yt_trending_pdf_dag

DAG to capture YouTube Trending as an image snapshot, store metadata in DuckDB,
and purge old snapshots.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Import business logic from tasks module
import sys
from pathlib import Path

# Add parent directory to path for imports
dag_dir = Path(__file__).parent
sys.path.insert(0, str(dag_dir.parent))

from project.tasks import (  # type: ignore # noqa: F401
    create_trending_snapshot,  # noqa: F401
)


def create_snapshot_wrapper(**kwargs: dict) -> str:
    """Wrapper to call create_trending_snapshot from tasks"""
    return create_trending_snapshot()


with DAG(
    dag_id="yt_extract_trending_dag",
    description=(
        "Capture YouTube Trending as an image snapshot, store metadata in DuckDB, "
        "extract data from datalake, and purge old snapshots."
    ),
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(hours=2),
    catchup=False,
    tags={"youtube", "trending"},
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    create_snapshot = PythonOperator(
        task_id="create_snapshot",
        python_callable=create_snapshot_wrapper,
        do_xcom_push=True,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    # Main workflow: capture snapshot -> save metadata -> load data from datalake
    start >> create_snapshot >> stop



