## Youtube Data Extract Pipeline

Pipeline and tooling for extracting YouTube trending data.  
Current functionality focuses on capturing the YouTube Trending search results as PNG screenshots using headless Chrome via Selenium, storing metadata in DuckDB, and providing a Streamlit UI for querying. Plans to evolve into a richer data pipeline with OCR extraction and Content Idea App.

---

### Problem Statement

**Goal**: Build a Content Idea App that quickly tells you what topics and formats are currently hot in the market, starting from YouTube trending data.

High‑level objectives:

- **Ingest**: Regularly collect YouTube trending (and later search) data.
- **Store**: Keep raw and processed data in a form that is easy to query.
- **Transform**: Create analytics‑ready views (metrics, aggregations, topics).
- **Surface insights**: Provide a UI where creators can:
  - Discover trending topics and channels.
  - Compare formats (shorts vs long‑form, etc.).
  - Explore engagement patterns (views, likes, comments).

---

### Architecture Overview

The project is divided into **three main components**:

#### 1. **Airflow + Data Lake (JSON) + DBT + ETL Jobs** (`airflow_jobs/`)
   
   Core data ingestion and transformation pipeline:
   - `dags/dag-yt_trending_pipeline.py` – Airflow DAG orchestrating:
     - Capturing YouTube trending snapshots via headless Chrome (Selenium).
     - Storing raw JSON data in the datalake.
     - Running DBT transformations for data quality and business logic.
     - Purging old data according to retention policies.
   - `project/tasks.py` – Python script that:
     - Opens YouTube Trending search results in headless Chrome.
     - Captures screenshots as PNG.
     - Extracts and stores raw JSON metadata.
   - `configs/` – Configuration files including `airflow.cfg` and environment variables.
   - `start_airflow.sh` – Script to initialize and start Airflow standalone.

#### 2. **DuckDB Serving on Kubernetes** (`duckdb_serving/`)
   
   Scalable data warehouse and query service:
   - `api/duckdb_server.py` – REST API for querying DuckDB.
   - `k8s/` – Kubernetes manifests for deploying DuckDB:
     - `duckdb-deployment.yaml` – DuckDB container deployment.
     - `duckdb-service.yaml` – Kubernetes service for accessing DuckDB.
     - `duckdb-configmap.yaml` – Configuration management.
     - `duckdb-pvc.yaml` – Persistent volume for data storage.
   - `docker/` – Docker setup for containerizing DuckDB.
   - `init/init_db.py` – Database initialization scripts.

#### 3. **Self-Hosted Superset for Dashboarding** (`dashbord/`)
   
   Analytics and visualization layer:
   - `Dockerfile` – Docker image for Apache Superset.
   - Connects to DuckDB for querying transformed data.
   - Provides interactive dashboards and insights.

---

### Getting Started

#### Prerequisites

- Python 3.10+ installed.
- Google Chrome installed (required for Selenium to capture screenshots).
- A Linux environment (this repo is currently developed on Ubuntu).

#### Install Python dependencies

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you prefer `uv`, you can instead run:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

#### Install Airflow

To install Airflow with version constraints for compatibility:

```bash
./install_airflow.sh
```

If you encounter an error like "Error while accessing remote requirements file" due to an unsupported Python version (e.g., Python 3.14), edit `install_airflow.sh` and set `PYTHON_VERSION` manually to a supported version such as "3.13".

Supported Python versions for Airflow 3.1.3 are 3.10, 3.11, 3.12, and 3.13.

#### Running the Pipeline

To run a single snapshot capture:

```bash
python tasks.py
```

This will generate a PNG screenshot in the `data/` directory.

To run the Airflow DAG:

```bash
./start_airflow.sh
```

Then access Airflow UI at http://localhost:8080 to trigger the DAG.

#### Running the Streamlit UI

```bash
streamlit run ui.py
```

Access at http://localhost:8501 to query the DuckDB database.

#### Running Superset

Using Docker:

```bash
docker build -t yt-superset .
docker run -p 8088:8088 yt-superset
```

Access Superset at http://localhost:8088 (admin/admin).

---

### Configuring ChromeDriver

Selenium Manager automatically handles ChromeDriver download and matching versions. No manual configuration needed.

---

### Data Storage

Metadata is stored in `data/yt_trending.duckdb` with the following table:

- `trending_snapshots`:
  - `snapshot_timestamp` (TIMESTAMPTZ): When the record was stored.
  - `file_mtime` (TIMESTAMPTZ): File modification time.
  - `file_path` (TEXT): Path to the PNG file.
  - `file_size_bytes` (BIGINT): File size in bytes.

---

### Kubernetes template (optional)

The `templates/deployment.yaml` file is a self‑contained example Kubernetes manifest:

- Creates a `ConfigMap` with an `index.html` “Hello World” page.
- Deploys an `nginx` container that mounts that HTML file.
- Exposes the app via a `NodePort` `Service`.

Usage:

```bash
kubectl apply -f templates/deployment.yaml
```

You can use this as a starting point for deploying the Streamlit UI or Superset.

---

### Roadmap

Planned enhancements for `yt_extract`:

- Integrate OCR (Tesseract) to extract video metadata from PNG screenshots.
- Store structured video data in DuckDB/Postgres.
- Build transformations and metrics for content insights.
- Expand UI with visualizations and content idea suggestions.
- Add more data sources (YouTube API, etc.).

---

### License

Add your preferred license here (e.g., MIT, Apache 2.0).
