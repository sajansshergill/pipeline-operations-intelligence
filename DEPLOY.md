# Deploying the Vertex Ops Hub

The repo includes a **small demo `data/vertex_ops.duckdb`** so charts work immediately after clone.

## Option A — Streamlit Community Cloud (free tier)

1. Push this repo to GitHub (already done if you are reading this on GitHub).
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**.
3. Pick the repository and branch, set **Main file path** to: `src/dashboards/app.py`.
4. Under **Advanced settings** → **Python version**: 3.11.
5. **Important:** the default `requirements.txt` installs Airflow, Spark, and other heavy packages and **will not finish** on the free tier. Use a slim dependency file instead:
   - Either temporarily **rename** in a deploy branch: use `requirements-app.txt` as `requirements.txt`, **or**
   - If your workspace supports a custom requirements path, set it to **`requirements-app.txt`**.
6. Deploy. Open the app URL; pick **Technical** or **Finance** in the sidebar.

Secrets: add `ANTHROPIC_API_KEY` in the Cloud **Secrets** UI only if you enable Claude digests elsewhere (not required for the dashboard).

## Option B — Docker (dashboard only, recommended for production-style hosting)

Build and run the slim image (same demo DB baked in):

```bash
docker build -f Dockerfile.streamlit -t vertex-ops-hub:latest .
docker run --rm -p 8501:8501 vertex-ops-hub:latest
```

Open `http://localhost:8501`.

Deploy the same image to **Google Cloud Run**, **Fly.io**, **Railway**, or **Render**: set the container port to **8501**, allocate ~512MB–1GB RAM.

## Option C — Full stack (Kafka + producer + consumer + dashboard)

Use the main `Dockerfile` and `docker compose up --build` from the repository root (see README). That path uses `requirements.txt` and is intended for local / VM demos, not Streamlit Cloud.
