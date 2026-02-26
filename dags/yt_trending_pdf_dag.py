from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import sys
import uuid
import dotenv
dotenv.load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from airflow import DAG
from airflow.operators.python import PythonOperator

from app import create_trending_pdf
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DUCKDB_PATH = DATA_DIR / "yt_trending.duckdb"
RETENTION_DAYS = 30
PROMPTS_DIR = BASE_DIR / "prompts"
YT_EXTRACT_PROMPT_PATH = PROMPTS_DIR / "yt_extract_prompt.md"


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


def extract_trending_data_with_llm(ti, **_: dict) -> None:
    """
    Use an LLM (via LangChain + OpenAI) to extract structured
    video data from the generated PDF and store a flattened view
    into DuckDB.

    Table: trending_videos
      - uuid              (TEXT)          unique row identifier
      - rec_insert_ts     (TIMESTAMPTZ)   ingestion timestamp
      - extraction_date   (DATE)          extraction_date from the JSON payload
      - raw_variant       (JSON)          full JSON object for the video
      - video_title       (TEXT)
      - video_url         (TEXT)
      - video_id          (TEXT)
      - channel_name      (TEXT)
      - channel_url       (TEXT)
      - views_text        (TEXT)
      - views_count       (BIGINT)
      - upload_time_text  (TEXT)
      - upload_type       (TEXT)
      - duration          (TEXT)
      - category_section  (TEXT)
      - description_full  (TEXT)
      - hashtags          (JSON)
      - external_links    (JSON)
      - mentioned_handles (JSON)
      - sponsor_mentions  (JSON)
      - language          (TEXT)
      - repeated_listing  (BOOLEAN)
      - additional_metadata (TEXT)
    """
    from pypdf import PdfReader
    from langchain_openai import ChatOpenAI
    from langchain_core.prompts import ChatPromptTemplate

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise RuntimeError(
            "OPENAI_API_KEY environment variable must be set for LLM extraction task."
        )

    pdf_path = ti.xcom_pull(task_ids="create_trending_pdf")
    if not pdf_path:
        raise ValueError("No PDF path returned from create_trending_pdf")

    pdf_path_obj = Path(pdf_path)
    if not pdf_path_obj.is_absolute():
        pdf_path_obj = (BASE_DIR / pdf_path_obj).resolve()

    if not pdf_path_obj.exists():
        raise FileNotFoundError(f"Generated PDF not found at {pdf_path_obj}")

    if not YT_EXTRACT_PROMPT_PATH.exists():
        raise FileNotFoundError(
            f"Prompt file not found at {YT_EXTRACT_PROMPT_PATH}"
        )

    with YT_EXTRACT_PROMPT_PATH.open("r", encoding="utf-8") as f:
        prompt_instructions = f.read()

    reader = PdfReader(str(pdf_path_obj))
    pages_text = []
    for page in reader.pages:
        text = page.extract_text() or ""
        pages_text.append(text)
    pdf_text = "\n\n".join(pages_text).strip()

    if not pdf_text:
        raise ValueError(f"No text extracted from PDF at {pdf_path_obj}")

    llm = ChatOpenAI(
        model="gpt-4.1-mini",
        temperature=0,
        openai_api_key=openai_api_key,
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", prompt_instructions),
            (
                "user",
                "Here is the full text content of the PDF:\n\n{pdf_text}",
            ),
        ]
    )

    chain = prompt | llm
    response = chain.invoke({"pdf_text": pdf_text})
    content = response.content if hasattr(response, "content") else str(response)

    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "LLM did not return valid JSON as required by the prompt."
        ) from exc

    extraction_date_str = payload.get("extraction_date")
    try:
        extraction_date = (
            datetime.strptime(extraction_date_str, "%Y-%m-%d").date()
            if extraction_date_str
            else None
        )
    except ValueError:
        extraction_date = None

    videos = payload.get("videos") or []
    rec_insert_ts = datetime.now(timezone.utc)

    import duckdb

    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trending_videos (
                uuid TEXT,
                rec_insert_ts TIMESTAMPTZ,
                extraction_date DATE,
                raw_variant JSON,
                video_title TEXT,
                video_url TEXT,
                video_id TEXT,
                channel_name TEXT,
                channel_url TEXT,
                views_text TEXT,
                views_count BIGINT,
                upload_time_text TEXT,
                upload_type TEXT,
                duration TEXT,
                category_section TEXT,
                description_full TEXT,
                hashtags JSON,
                external_links JSON,
                mentioned_handles JSON,
                sponsor_mentions JSON,
                language TEXT,
                repeated_listing BOOLEAN,
                additional_metadata TEXT
            )
            """
        )

        insert_sql = """
            INSERT INTO trending_videos (
                uuid,
                rec_insert_ts,
                extraction_date,
                raw_variant,
                video_title,
                video_url,
                video_id,
                channel_name,
                channel_url,
                views_text,
                views_count,
                upload_time_text,
                upload_type,
                duration,
                category_section,
                description_full,
                hashtags,
                external_links,
                mentioned_handles,
                sponsor_mentions,
                language,
                repeated_listing,
                additional_metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for video in videos:
            row_uuid = str(uuid.uuid4())

            conn.execute(
                insert_sql,
                [
                    row_uuid,
                    rec_insert_ts,
                    extraction_date,
                    json.dumps(video),
                    video.get("video_title"),
                    video.get("video_url"),
                    video.get("video_id"),
                    video.get("channel_name"),
                    video.get("channel_url"),
                    video.get("views_text"),
                    video.get("views_count"),
                    video.get("upload_time_text"),
                    video.get("upload_type"),
                    video.get("duration"),
                    video.get("category_section"),
                    video.get("description_full"),
                    json.dumps(video.get("hashtags")),
                    json.dumps(video.get("external_links")),
                    json.dumps(video.get("mentioned_handles")),
                    json.dumps(video.get("sponsor_mentions")),
                    video.get("language"),
                    bool(video.get("repeated_listing", False)),
                    video.get("additional_metadata"),
                ],
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

    extract_trending = PythonOperator(
        task_id="extract_trending_data_with_llm",
        python_callable=extract_trending_data_with_llm,
    )

    purge_old = PythonOperator(
        task_id="purge_old_snapshots",
        python_callable=purge_old_files,
    )

    create_pdf >> save_metadata >> extract_trending >> purge_old
