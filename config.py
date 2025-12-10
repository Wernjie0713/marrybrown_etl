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
    # Set to "ReadOnly" to connect to Xilnex replica (avoids impacting primary POS database)
    "application_intent": os.getenv("XILNEX_APPLICATION_INTENT", "ReadOnly"),
}

TARGET_SQL_CONFIG = {
    "server": os.getenv("TARGET_SERVER", "10.0.1.194,1433"),
    "database": os.getenv("TARGET_DATABASE", "MarryBrown_DW"),
    "username": os.getenv("TARGET_USERNAME", "etl_user"),
    "password": os.getenv("TARGET_PASSWORD", "ETL@MarryBrown2025!"),
    "driver": "{" + os.getenv("TARGET_DRIVER", "ODBC Driver 17 for SQL Server") + "}",
}


def build_connection_string(config_dict: dict, timeout: int = 30, trust_server_cert: bool = None) -> str:
    """
    Helper to build a SQL Server ODBC connection string.
    
    Args:
        config_dict: Database configuration dictionary
        timeout: Connection timeout in seconds
        trust_server_cert: Whether to trust server certificate. 
                          If None, defaults to True for localhost, False for Azure
    """
    # Auto-detect if localhost (for SSL certificate trust)
    if trust_server_cert is None:
        server = config_dict.get('server', '').lower()
        trust_server_cert = 'localhost' in server or '127.0.0.1' in server or '.' not in server
    
    trust_cert = "yes" if trust_server_cert else "no"
    
    conn_str = (
        f"DRIVER={config_dict['driver']};"
        f"SERVER={config_dict['server']};"
        f"DATABASE={config_dict['database']};"
        f"UID={config_dict['username']};"
        f"PWD={config_dict['password']};"
        "Encrypt=yes;"
        f"TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
    )
    
    # Add ApplicationIntent if specified (e.g., "ReadOnly" for replica)
    app_intent = config_dict.get("application_intent")
    if app_intent:
        conn_str += f"ApplicationIntent={app_intent};"
    
    return conn_str

# Export settings
EXPORT_DIR = os.getenv("EXPORT_DIR", "exports")
MONTH_TO_EXPORT = os.getenv("MONTH_TO_EXPORT", "2025-09")  # September 2025

