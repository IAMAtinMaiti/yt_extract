## Youtube Data Extract Pipeline

Pipeline and tooling for extracting YouTube trending data.  
Current functionality focuses on capturing the YouTube Trending page as a PDF using headless Chrome via Selenium, with plans to evolve into a richer data pipeline and Content Idea App.

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

### Current Components

- `app.py` – Python script that:
  - Opens the YouTube Trending page in a headless Chrome browser via Selenium.
  - Uses Chrome DevTools to print the page to a PDF file.
  - Saves the PDF locally with a timestamp in the filename.

- `templates/deployment.yaml` – Example Kubernetes manifest for a simple Nginx‑based “Hello World” web app.  
  This is currently a template / example and not yet wired to the `yt_extract` script or a future UI.

- `requirements.txt` – Python dependencies used by the project:
  - `selenium` – Browser automation for headless Chrome.
  - `apache-airflow` – (Planned) orchestration layer for future data pipelines.

---

### Getting Started

#### Prerequisites
v
- Python 3.10+ installed.
- Google Chrome installed.
- Matching `chromedriver` binary installed locally.
- A Linux environment (this repo is currently developed on Ubuntu).

#### Install Python dependnencies

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

Then run the script:

```bash
python app.py
# or, if you used uv:
uv run app.py
```

---

### Configuring ChromeDriver

In `app.py`, the `CHROMEDRIVER_PATH` constant points to the local ChromeDriver binary:

- Ensure the path matches your local installation.
- You can download ChromeDriver that matches your Chrome version from the official ChromeDriver site.

Example:

- If you install `chromedriver` to `/usr/local/bin/chromedriver`, update:
  - `CHROMEDRIVER_PATH = "/usr/local/bin/chromedriver"`

---

### Running the script

From the project root:

```bash
python app.py
```

What happens:

- A headless Chrome browser is started via Selenium.
- The browser navigates to `https://www.youtube.com/feed/trending`.
- After a short delay to allow content to load, Chrome DevTools prints the page to a PDF.
- The PDF is written to the current directory with a name like:
  - `output_2026-02-25T12:34:56.789012.pdf`
- The script prints the path to the generated file.

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

You can use this as a starting point for:

- Deploying a future Streamlit / web UI for this project.
- Experimenting with Kubernetes deployment basics.

---

### Roadmap

Planned enhancements for `yt_extract`:

- Use Airflow DAGs to schedule regular data extraction from YouTube APIs.
- Store structured video metadata in a queryable store (e.g., DuckDB, Postgres).
- Build transformations and metrics to power content‑idea insights.
- Replace the Nginx demo deployment with an actual app (API or Streamlit UI).

---

### License

Add your preferred license here (e.g., MIT, Apache 2.0).

