# Deploying the Vertex Ops Hub

The default **`requirements.txt`** installs **only** what the Streamlit app needs (Streamlit, DuckDB, pandas, plotly, python-dotenv). **No Docker, Kafka, Spark, or Airflow** are required to deploy or view the dashboard. The repo ships **`data/vertex_ops.duckdb`** with demo rows so charts work as soon as the app starts.

## Streamlit Community Cloud (recommended, no Docker)

1. Push this repository to GitHub.
2. Open [share.streamlit.io](https://share.streamlit.io) → **New app**.
3. Repository: this repo, branch: `main`.
4. **Main file path:** `src/dashboards/app.py`
5. **Python version:** Prefer **3.11** (matches `runtime.txt` and `.python-version`). If the builder picks **3.14**, installs should still succeed: **`requirements.txt`** pins **Pillow 12.2** (has `cp314` wheels) and a current **Streamlit** that allows Pillow 11+.
6. Deploy. Cloud installs **`requirements.txt`** and optional **`packages.txt`** (Debian package names only, **one per line**—do not use `#` comments; Cloud passes each whitespace-separated token to `apt-get` and will fail on comment words).

Optional: add secrets in the Cloud UI only if you later wire features that need API keys (not required for the bundled demo DB).

## Slim Docker (optional)

If you prefer a container but not the full Kafka stack:

```bash
docker build -f Dockerfile.streamlit -t vertex-ops-hub:latest .
docker run --rm -p 8501:8501 vertex-ops-hub:latest
```

## Full pipeline (Kafka + Spark + producer + consumer)

For local or VM demos with live telemetry, install **`requirements-pipeline.txt`** and use **`docker compose`** with the main **`Dockerfile`** (see README). That path is separate from Streamlit Cloud and is not used by the hosted dashboard-only deploy.
