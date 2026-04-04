FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements (from parent directory context)
COPY ./requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./api/duckdb_server.py .
COPY ./init/init_db.py ./init_db.py

# Create data directory
RUN mkdir -p /data/duckdb

# Set environment variables
ENV DUCKDB_DATA_PATH=/data/duckdb/data.duckdb
ENV LOG_LEVEL=INFO
ENV PYTHONUNBUFFERED=1

# Initialize database
RUN python init_db.py || true

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the server
CMD ["python", "-m", "uvicorn", "duckdb_server:app", "--host", "0.0.0.0", "--port", "8000"]

