"""
Enhanced PostgreSQL FastAPI Server for Azure AI Agent Graph API
Fixed Azure Database for PostgreSQL authentication
"""
import os
import psycopg2
import json
import sys
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import logging

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'tessapocserver.postgres.database.azure.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'TessaDBAdmin'),
    'password': os.getenv('DB_PASSWORD'),
    'sslmode': 'require'  # Required for Azure Database for PostgreSQL
}

# Build connection string
CONNECTION_STRING = f"host={DB_CONFIG['host']} port={DB_CONFIG['port']} dbname={DB_CONFIG['database']} user={DB_CONFIG['user']} password={DB_CONFIG['password']} sslmode={DB_CONFIG['sslmode']}"

# Initialize FastAPI app
app = FastAPI(
    title="PostgreSQL Database API",
    description="PostgreSQL database query API for Azure AI Agent",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class QueryRequest(BaseModel):
    query: str
    fetch_results: bool = True

class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    row_count: int
    message: str

def test_connection():
    """Test database connection on startup"""
    try:
        conn = psycopg2.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        logger.info(f"Database connection successful. PostgreSQL version: {version[0]}")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False

def execute_db_query(query: str, fetch_results: bool = True) -> Dict[str, Any]:
    """Execute SQL query against PostgreSQL database with enhanced error handling"""
    conn = None
    cursor = None
    
    try:
        logger.info(f"Executing query: {query[:100]}...")
        
        # Connect to database
        conn = psycopg2.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute(query)
        
        if fetch_results and cursor.description:
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            data = []
            for row in results:
                row_dict = {}
                for i, value in enumerate(row):
                    if value is None:
                        row_dict[columns[i]] = None
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        row_dict[columns[i]] = value.isoformat()
                    else:
                        row_dict[columns[i]] = value
                data.append(row_dict)
            
            logger.info(f"Query successful. Returned {len(results)} rows.")
            return {
                "success": True,
                "data": data,
                "columns": columns,
                "row_count": len(results),
                "message": f"Query executed successfully. Found {len(results)} rows."
            }
        else:
            # For INSERT, UPDATE, DELETE operations
            conn.commit()
            row_count = cursor.rowcount if cursor.rowcount != -1 else 0
            
            logger.info(f"Query successful. {row_count} rows affected.")
            return {
                "success": True,
                "data": None,
                "columns": None,
                "row_count": row_count,
                "message": f"Query executed successfully. {row_count} rows affected."
            }
            
    except psycopg2.OperationalError as e:
        error_msg = f"Connection error: {str(e)}"
        logger.error(error_msg)
        if conn:
            conn.rollback()
        return {
            "success": False,
            "data": None,
            "columns": None,
            "row_count": 0,
            "message": error_msg
        }
    except psycopg2.Error as e:
        error_msg = f"Database error: {str(e)}"
        logger.error(error_msg)
        if conn:
            conn.rollback()
        return {
            "success": False,
            "data": None,
            "columns": None,
            "row_count": 0,
            "message": error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        if conn:
            conn.rollback()
        return {
            "success": False,
            "data": None,
            "columns": None,
            "row_count": 0,
            "message": error_msg
        }
    finally:
        # Clean up connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Startup event
@app.on_event("startup")
async def startup_event():
    """Test database connection on startup"""
    logger.info("Starting up FastAPI server...")
    logger.info(f"Database host: {DB_CONFIG['host']}")
    logger.info(f"Database user: {DB_CONFIG['user']}")
    
    if not test_connection():
        logger.error("Failed to connect to database on startup!")
    else:
        logger.info("Database connection verified on startup")

# Health check endpoint with database test
@app.get("/health")
async def health_check():
    """Enhanced health check with database connectivity"""
    try:
        # Test database connection
        conn = psycopg2.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy", 
            "service": "PostgreSQL Database API",
            "database": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy", 
            "service": "PostgreSQL Database API",
            "database": "disconnected",
            "error": str(e)
        }

# Debug endpoint to check configuration
@app.get("/debug/config")
async def debug_config():
    """Debug endpoint to check configuration (remove in production)"""
    return {
        "host": DB_CONFIG['host'],
        "port": DB_CONFIG['port'],
        "database": DB_CONFIG['database'],
        "user": DB_CONFIG['user'],
        "password_set": bool(DB_CONFIG['password']),
        "sslmode": DB_CONFIG['sslmode']
    }

# Main query endpoint
@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute SQL query against PostgreSQL database"""
    try:
        result = execute_db_query(request.query, request.fetch_results)
        
        if not result["success"]:
            # Return the error as a proper response instead of raising exception
            return QueryResponse(**result)
        
        return QueryResponse(**result)
    except Exception as e:
        logger.error(f"Unexpected error in execute_query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def main():
    """Main entry point for the FastAPI server"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()