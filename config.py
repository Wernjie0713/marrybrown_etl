import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Azure SQL Database connection settings (from environment variables)
AZURE_SQL_CONFIG = {
    "server": os.getenv("XILNEX_SERVER", "your-server.database.windows.net"),
    "database": os.getenv("XILNEX_DATABASE", "XilnexDB158"),
    "username": os.getenv("XILNEX_USERNAME", "your_username"),
    "password": os.getenv("XILNEX_PASSWORD", "your_password"),
    "driver": "{" + os.getenv("XILNEX_DRIVER", "ODBC Driver 18 for SQL Server") + "}",
}

TARGET_SQL_CONFIG = {
    "server": os.getenv("TARGET_SERVER", "localhost"),
    "database": os.getenv("TARGET_DATABASE", "MarryBrown_DW"),
    "username": os.getenv("TARGET_USERNAME", "etl_user"),
    "password": os.getenv("TARGET_PASSWORD", "yourStrong(!)Password"),
    "driver": "{" + os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server") + "}",
}


def build_connection_string(config_dict: dict, timeout: int = 30) -> str:
    """
    Helper to build a SQL Server ODBC connection string.
    """
    return (
        f"DRIVER={config_dict['driver']};"
        f"SERVER={config_dict['server']};"
        f"DATABASE={config_dict['database']};"
        f"UID={config_dict['username']};"
        f"PWD={config_dict['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        f"Connection Timeout={timeout};"
    )

# Export settings
EXPORT_DIR = os.getenv("EXPORT_DIR", "exports")
MONTH_TO_EXPORT = os.getenv("MONTH_TO_EXPORT", "2025-09")  # September 2025

