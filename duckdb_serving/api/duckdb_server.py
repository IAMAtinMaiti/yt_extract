"""
DuckDB Server - RESTful API for DuckDB
Provides endpoints to query and manage DuckDB database
"""

import os
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import duckdb
from typing import Optional, List, Dict, Any

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="DuckDB Server",
    description="RESTful API for DuckDB database operations",
    version="1.0.0"
)

# Database path from environment variable
DUCKDB_DATA_PATH = os.getenv("DUCKDB_DATA_PATH", "/data/duckdb/data.duckdb")

# Ensure data directory exists
os.makedirs(os.path.dirname(DUCKDB_DATA_PATH), exist_ok=True)

# DuckDB connection
db_connection = None


def get_db_connection():
    """Get or create DuckDB connection"""
    global db_connection
    if db_connection is None:
        try:
            db_connection = duckdb.connect(DUCKDB_DATA_PATH)
            logger.info(f"Connected to DuckDB at {DUCKDB_DATA_PATH}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {str(e)}")
            raise
    return db_connection


# Request/Response models
class QueryRequest(BaseModel):
    sql: str


class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    database: str
    connected: bool


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        conn = get_db_connection()
        logger.info("DuckDB Server started successfully")
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection on shutdown"""
    global db_connection
    if db_connection is not None:
        try:
            db_connection.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint
    """
    try:
        conn = get_db_connection()
        conn.query("SELECT 1")
        return HealthResponse(
            status="healthy",
            database=DUCKDB_DATA_PATH,
            connected=True
        )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return HealthResponse(
            status="unhealthy",
            database=DUCKDB_DATA_PATH,
            connected=False
        )


@app.get("/ready")
async def readiness_check():
    """
    Readiness check endpoint
    """
    try:
        conn = get_db_connection()
        conn.query("SELECT 1")
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Database not ready")


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """
    Execute a SQL query against DuckDB

    Example:
    {
        "sql": "SELECT * FROM table_name LIMIT 10"
    }
    """
    try:
        conn = get_db_connection()
        result = conn.query(request.sql)

        # Get column names
        columns = [desc[0] for desc in result.description] if result.description else []

        # Get data as list of dictionaries
        data = []
        for row in result.fetchall():
            data.append(dict(zip(columns, row)))

        logger.info(f"Query executed successfully. Rows: {len(data)}")
        return QueryResponse(
            success=True,
            data=data,
            columns=columns
        )
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Query execution failed: {error_msg}")
        return QueryResponse(
            success=False,
            error=error_msg
        )


@app.get("/tables")
async def list_tables():
    """
    List all tables in the database
    """
    try:
        conn = get_db_connection()
        result = conn.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
        tables = [row[0] for row in result.fetchall()]
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Failed to list tables: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schema/{table_name}")
async def get_table_schema(table_name: str):
    """
    Get schema information for a specific table
    """
    try:
        conn = get_db_connection()
        result = conn.query(f"DESCRIBE {table_name}")

        columns = [desc[0] for desc in result.description]
        schema = []
        for row in result.fetchall():
            schema.append(dict(zip(columns, row)))

        return {"table_name": table_name, "schema": schema}
    except Exception as e:
        logger.error(f"Failed to get schema: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/execute")
async def execute_statement(request: QueryRequest):
    """
    Execute a SQL statement (INSERT, UPDATE, DELETE, CREATE, etc.)
    """
    try:
        conn = get_db_connection()
        conn.execute(request.sql)
        logger.info("Statement executed successfully")
        return {"success": True, "message": "Statement executed"}
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Statement execution failed: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)


@app.get("/stats")
async def get_database_stats():
    """
    Get database statistics
    """
    try:
        conn = get_db_connection()

        # Get table count
        tables_result = conn.query(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'"
        )
        table_count = tables_result.fetchone()[0]

        # Get file size
        if os.path.exists(DUCKDB_DATA_PATH):
            file_size = os.path.getsize(DUCKDB_DATA_PATH)
        else:
            file_size = 0

        return {
            "database_path": DUCKDB_DATA_PATH,
            "file_size_bytes": file_size,
            "table_count": table_count,
            "version": duckdb.__version__
        }
    except Exception as e:
        logger.error(f"Failed to get stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

