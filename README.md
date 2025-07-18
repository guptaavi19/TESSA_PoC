# PostgreSQL FastAPI Server for Azure AI Agent

This FastAPI-based REST API server allows you to query a PostgreSQL database hosted on **Azure PostgreSQL** using HTTP POST requests. It is specifically designed for integration with **Azure AI Agents** and supports CORS, structured responses, and secure connection handling via environment variables.

---

## 🚀 Features

- RESTful API for executing SQL queries (SELECT/INSERT/UPDATE/DELETE)
- Support for `fetch_results` toggle for read/write operations
- CORS enabled for integration with Azure AI Agent or any frontend
- Detailed response structure with rows, columns, and messages
- Health check endpoint for availability monitoring
- Ready for deployment with `.env` configuration

---

## 🧾 Prerequisites

- Python 3.8+
- PostgreSQL database (Azure-hosted or local)
- Virtual environment (recommended)

---

## 📦 Dependencies

Install using `pip`:

```bash
pip install fastapi uvicorn psycopg2-binary python-dotenv
