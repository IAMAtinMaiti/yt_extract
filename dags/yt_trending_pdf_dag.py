from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from app import create_trending_pdf


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DUCKDB_PATH = DATA_DIR / "yt_trending.duckdb"
RETENTION_DAYS = 30


def run_create_trending_pdf(**_: dict) -> str:
    """
    Task callable that runs the existing create_trending_pdf function.
    Returns the path to the generated PDF so it can be used via XCom.
    """
    pdf_path = create_trending_pdf()
    return pdf_path


def store_snapshot_metadata(ti, **_: dict) -> None:
    """
    Persist metadata about the generated PDF into DuckDB.

    Data model (table: trending_snapshots):
      - snapshot_timestamp (TIMESTAMPTZ): when this pipeline run stored the record
      - file_mtime        (TIMESTAMPTZ): filesystem modified time of the PDF
      - file_path         (TEXT): absolute path to the PDF file
      - file_size_bytes   (BIGINT): size of the PDF in bytes
    """
    import duckdb

    pdf_path = ti.xcom_pull(task_ids="create_trending_pdf")
    if not pdf_path:
        raise ValueError("No PDF path returned from create_trending_pdf")

    pdf_path_obj = Path(pdf_path)
    if not pdf_path_obj.is_absolute():
        pdf_path_obj = (BASE_DIR / pdf_path_obj).resolve()

    if not pdf_path_obj.exists():
        raise FileNotFoundError(f"Generated PDF not found at {pdf_path_obj}")

    file_stats = pdf_path_obj.stat()
    snapshot_timestamp = datetime.now(timezone.utc)
    file_mtime = datetime.fromtimestamp(file_stats.st_mtime, tz=timezone.utc)
    file_size_bytes = file_stats.st_size

    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trending_snapshots (
                snapshot_timestamp TIMESTAMPTZ,
                file_mtime TIMESTAMPTZ,
                file_path TEXT,
                file_size_bytes BIGINT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO trending_snapshots (
                snapshot_timestamp,
                file_mtime,
                file_path,
                file_size_bytes
            )
            VALUES (?, ?, ?, ?)
            """,
            [snapshot_timestamp, file_mtime, str(pdf_path_obj), file_size_bytes],
        )
    finally:
        conn.close()


def purge_old_files(**_: dict) -> None:
    """
    Remove PDF files and DuckDB records older than RETENTION_DAYS.
    """
    import duckdb

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

    # Delete old PDF files from the project root.
    for path in BASE_DIR.glob("output_*.pdf"):
        if not path.is_file():
            continue
        file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if file_mtime < cutoff:
            path.unlink()

    # Delete old PDF files from the data directory.
    for path in DATA_DIR.glob("output_*.pdf"):
        if not path.is_file():
            continue
        file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if file_mtime < cutoff:
            path.unlink()

    # Delete old metadata rows from DuckDB.
    if DUCKDB_PATH.exists():
        conn = duckdb.connect(str(DUCKDB_PATH))
        try:
            conn.execute(
                """
                DELETE FROM trending_snapshots
                WHERE snapshot_timestamp < ?
                """,
                [cutoff],
            )
        finally:
            conn.close()


with DAG(
    dag_id="yt_trending_pdf_dag",
    description=(
        "Capture YouTube Trending as a PDF, store metadata in DuckDB, "
        "and purge old snapshots."
    ),
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(hours=2),
    catchup=False,
    tags=["youtube", "trending", "pdf", "duckdb"],
) as dag:
    create_pdf = PythonOperator(
        task_id="create_trending_pdf",
        python_callable=run_create_trending_pdf,
    )

    save_metadata = PythonOperator(
        task_id="save_snapshot_metadata",
        python_callable=store_snapshot_metadata,
    )

    purge_old = PythonOperator(
        task_id="purge_old_snapshots",
        python_callable=purge_old_files,
    )

    create_pdf >> save_metadata >> purge_old

