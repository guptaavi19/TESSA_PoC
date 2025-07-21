#!/usr/bin/env python3
"""
PostgreSQL FastAPI Server for Azure AI Agent Graph API
Provides database query capabilities through REST endpoints
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

from dotenv import load_dotenv
load_dotenv()

# DEBUGGING: Let's see what environment variables are actually loaded
print("=== ENVIRONMENT VARIABLES DEBUG ===")
print(f"DB_HOST from env: {os.getenv('DB_HOST')}")
print(f"DB_PORT from env: {os.getenv('DB_PORT')}")
print(f"DB_USER from env: {os.getenv('DB_USER')}")
print(f"DB_PASSWORD from env: {'***' if os.getenv('DB_PASSWORD') else 'NOT SET'}")
print(f"DB_NAME from env: {os.getenv('DB_NAME')}")
print(f"OLD connection_string env: {os.getenv('connection_string')}")
print("=====================================")

# FIXED: Proper PostgreSQL connection string format
# Check if we should use the old connection_string or build a new one
old_connection_string = os.getenv("connection_string")

if old_connection_string and not old_connection_string.startswith("postgresql://"):
    print("DETECTED MALFORMED connection_string - Building new connection string from individual components")
    # Use individual environment variables (RECOMMENDED)
    DB_HOST = os.getenv("DB_HOST", "tessapocserver.postgres.database.azure.com")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "TessaDBAdmin@tessapocserver")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    
    if not DB_PASSWORD or not DB_NAME:
        print("ERROR: DB_PASSWORD or DB_NAME not set in environment variables!")
        # Try to extract from the malformed connection string
        if old_connection_string:
            parts = old_connection_string.split("@")
            if len(parts) >= 3:
                DB_USER = parts[0] if parts[0] else "TessaDBAdmin@tessapocserver"
                DB_PASSWORD = parts[1] if parts[1] else "1234"
                host_port_db = parts[2]
                if ":" in host_port_db:
                    host_part = host_port_db.split(":")[0]
                    DB_HOST = host_part if host_part else "tessapocserver.postgres.database.azure.com"
                print(f"EXTRACTED from malformed string - User: {DB_USER}, Host: {DB_HOST}")
    
    # Build proper connection string
    CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
elif old_connection_string and old_connection_string.startswith("postgresql://"):
    print("Using existing properly formatted connection string")
    CONNECTION_STRING = old_connection_string
    
else:
    print("Building connection string from individual environment variables")
    DB_HOST = os.getenv("DB_HOST", "tessapocserver.postgres.database.azure.com")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "TessaDBAdmin@tessapocserver")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "Admin@1234")
    DB_NAME = os.getenv("DB_NAME", "tessa")
    CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"FINAL connection string: postgresql://{DB_USER if 'DB_USER' in locals() else 'unknown'}:****@{DB_HOST if 'DB_HOST' in locals() else 'unknown'}:{DB_PORT if 'DB_PORT' in locals() else 'unknown'}/{DB_NAME if 'DB_NAME' in locals() else 'unknown'}")

# Initialize FastAPI app
app = FastAPI(
    title="PostgreSQL Database API",
    description="PostgreSQL database query API for Azure AI Agent",
    version="1.0.0"
)

# Add CORS middleware for Azure AI Agent access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for your Azure environment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class QueryRequest(BaseModel):
    query: str
    fetch_results: bool = True

class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    row_count: int
    message: str

def execute_db_query(query: str, fetch_results: bool = True) -> Dict[str, Any]:
    """
    Execute SQL query against PostgreSQL database.
    """
    conn = None
    cursor = None
    
    try:
        # FIXED: Proper connection handling for Azure PostgreSQL
        print(f"Attempting to connect to database...")
        
        # Azure PostgreSQL often requires SSL and specific authentication format
        if "tessapocserver.postgres.database.azure.com" in CONNECTION_STRING:
            print("Detected Azure PostgreSQL - adding SSL requirements")
            if "sslmode" not in CONNECTION_STRING:
                ssl_connection = CONNECTION_STRING + "?sslmode=require"
            else:
                ssl_connection = CONNECTION_STRING
        else:
            ssl_connection = CONNECTION_STRING
            
        print(f"Final connection attempt with: {ssl_connection.replace(DB_PASSWORD if 'DB_PASSWORD' in locals() else 'password', '****')}")
        
        conn = psycopg2.connect(ssl_connection)
        cursor = conn.cursor()
        
        # Execute query
        print(f"Executing query: {query[:100]}...")
        cursor.execute(query)
        
        if fetch_results and cursor.description:
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert results to list of dictionaries
            data = []
            for row in results:
                # Handle None values and convert to JSON-serializable types
                row_dict = {}
                for i, value in enumerate(row):
                    if value is None:
                        row_dict[columns[i]] = None
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        row_dict[columns[i]] = value.isoformat()
                    else:
                        row_dict[columns[i]] = value
                data.append(row_dict)
            
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
            
            return {
                "success": True,
                "data": None,
                "columns": None,
                "row_count": row_count,
                "message": f"Query executed successfully. {row_count} rows affected."
            }
            
    except psycopg2.Error as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        return {
            "success": False,
            "data": None,
            "columns": None,
            "row_count": 0,
            "message": f"Database error: {str(e)}"
        }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
        return {
            "success": False,
            "data": None,
            "columns": None,
            "row_count": 0,
            "message": f"Unexpected error: {str(e)}"
        }
    finally:
        # Clean up connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "PostgreSQL Database API"}

# ADDED: Database connection test endpoint
@app.get("/test-connection")
async def test_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "Database connection successful",
            "postgres_version": version[0] if version else "Unknown",
            "connection_info": f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Database connection failed: {str(e)}",
            "connection_info": f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
        }

# Main query endpoint
@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """
    Execute SQL query against PostgreSQL database.
    
    - **query**: SQL query to execute
    - **fetch_results**: Whether to fetch and return results (True for SELECT) or just execute (False for INSERT/UPDATE/DELETE)
    """
    try:
        result = execute_db_query(request.query, request.fetch_results)
        return QueryResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def main():
    """Main entry point for the FastAPI server"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
