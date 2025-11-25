"""
Shared database connection utility.
"""
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, Engine

# Use relative import if possible, or rely on sys.path setup in calling scripts
# In this codebase, sys.path usually includes the project root.
from utils.env_loader import load_environment

def get_warehouse_engine() -> Engine:
    """
    Get SQLAlchemy engine for warehouse with optimized connection pooling.
    
    Returns:
        sqlalchemy.engine.Engine: Configured database engine
    """
    # Ensure environment is loaded (idempotent)
    load_environment()

    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER", "localhost")
    database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
    user = os.getenv("TARGET_USERNAME", "sa")
    password = quote_plus(os.getenv("TARGET_PASSWORD", ""))
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
        "&timeout=60"
        "&login_timeout=60"
    )
    
    # Optimized connection pooling configuration
    return create_engine(
        connection_uri, 
        pool_size=5,           # Maintain 5 connections in pool
        max_overflow=10,       # Allow up to 10 additional connections
        pool_pre_ping=True,    # Verify connections before using
        echo=False,
        connect_args={
            "timeout": 60,
            "login_timeout": 60
        }
    )

