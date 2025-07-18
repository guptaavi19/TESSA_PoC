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

# Hard-coded database connection string
# Replace with your actual PostgreSQL connection details
CONNECTION_STRING = os.getenv("connection_string")

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
    conn = psycopg2.connect(dsn=CONNECTION_STRING)
    cursor = None
    
    try:
        # Connect to database using connection string
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

# # OpenAPI schema endpoint for Azure AI Agent
# @app.get("/openapi-schema")
# async def get_openapi_schema():
#     """
#     Get OpenAPI schema for Azure AI Agent integration
#     """
    # return {
    #     "openapi": "3.0.0",
    #     "info": {
    #         "title": "PostgreSQL Database API",
    #         "description": "PostgreSQL database query API for Azure AI Agent",
    #         "version": "1.0.0"
    #     },
    #     "servers": [
    #         {
    #             "url": "https://your-domain.com",
    #             "description": "Production server"
    #         }
    #     ],
    #     "paths": {
    #         "/query": {
    #             "post": {
    #                 "summary": "Execute SQL Query",
    #                 "description": "Execute SQL query against PostgreSQL database",
    #                 "requestBody": {
    #                     "required": True,
    #                     "content": {
    #                         "application/json": {
    #                             "schema": {
    #                                 "type": "object",
    #                                 "properties": {
    #                                     "query": {
    #                                         "type": "string",
    #                                         "description": "SQL query to execute"
    #                                     },
    #                                     "fetch_results": {
    #                                         "type": "boolean",
    #                                         "description": "Whether to return results",
    #                                         "default": True
    #                                     }
    #                                 },
    #                                 "required": ["query"]
    #                             }
    #                         }
    #                     }
    #                 },
    #                 "responses": {
    #                     "200": {
    #                         "description": "Query executed successfully",
    #                         "content": {
    #                             "application/json": {
    #                                 "schema": {
    #                                     "$ref": "#/components/schemas/QueryResponse"
    #                                 }
    #                             }
    #                         }
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }

def main():
    """Main entry point for the FastAPI server"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
