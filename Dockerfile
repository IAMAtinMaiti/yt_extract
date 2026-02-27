# Superset only (run Airflow separately or add a second service)
# Use full image name to avoid credential-helper issues with docker.io
FROM docker.io/library/python:3.11-slim

# Superset system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libsasl2-dev \
    libldap2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Project deps (DuckDB for your data)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Superset
RUN pip install --no-cache-dir "apache-superset>=4.0,<5"

# Copy app (for any custom config)
COPY . .

# Run at container start, not at build time
ENV FLASK_APP=superset
ENV SUPERSET_PORT=8088
# Override in production; required for sessions
ENV SUPERSET_SECRET_KEY=change-me-in-production-use-openssl-rand-base64-42
ENV SUPERSET_ADMIN_USERNAME=admin \
    SUPERSET_ADMIN_PASSWORD=admin \
    SUPERSET_ADMIN_FIRST_NAME=admin \
    SUPERSET_ADMIN_LAST_NAME=admin \
    SUPERSET_ADMIN_EMAIL=admin@admin.com
ENV PYTHONPATH=/app
EXPOSE 8088

# Init DB, create admin user, and run (idempotent after first run)
CMD superset db upgrade && \
    (superset fab create-admin \
        --username "${SUPERSET_ADMIN_USERNAME}" \
        --firstname "${SUPERSET_ADMIN_FIRST_NAME}" \
        --lastname "${SUPERSET_ADMIN_LAST_NAME}" \
        --email "${SUPERSET_ADMIN_EMAIL}" \
        --password "${SUPERSET_ADMIN_PASSWORD}" || true) && \
    superset init && \
    superset run -p 8088 --with-threads --host 0.0.0.0