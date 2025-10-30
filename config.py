import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Azure SQL Database connection settings (from environment variables)
AZURE_SQL_CONFIG = {
    'server': os.getenv('XILNEX_SERVER', 'your-server.database.windows.net'),
    'database': os.getenv('XILNEX_DATABASE', 'XilnexDB158'),
    'username': os.getenv('XILNEX_USERNAME', 'your_username'),
    'password': os.getenv('XILNEX_PASSWORD', 'your_password'),
    'driver': '{' + os.getenv('XILNEX_DRIVER', 'ODBC Driver 18 for SQL Server') + '}'
}

# Export settings
EXPORT_DIR = os.getenv('EXPORT_DIR', 'C:/exports')
MONTH_TO_EXPORT = os.getenv('MONTH_TO_EXPORT', '2025-09')  # September 2025

