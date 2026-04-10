FROM python:3.11-slim

# ── System dependencies ────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    gcc \
    g++ \
    git \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# PySpark needs JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Working directory ──────────────────────────────────────────────────────────
WORKDIR /app

# ── Python dependencies ────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Copy source ────────────────────────────────────────────────────────────────
COPY . .

# ── Create runtime directories ─────────────────────────────────────────────────
RUN mkdir -p data outputs/digests /tmp/vertex_ops_checkpoint

# ── Environment defaults (override via docker-compose or --env-file) ───────────
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV ANTHROPIC_API_KEY=""
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# ── Expose Streamlit port ──────────────────────────────────────────────────────
EXPOSE 8501

# ── Default: launch dashboard ──────────────────────────────────────────────────
CMD ["streamlit", "run", "src/dashboards/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]