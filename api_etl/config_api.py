"""
API ETL Configuration
Xilnex Sync API credentials and target parameters
"""

# Xilnex Sync API Credentials
API_HOST = "api.xilnex.com"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"

# Target Data Range (October 2018 - first month API returns)
TARGET_YEAR = 2018
TARGET_MONTH = 10
TARGET_START_DATE = "2018-10-01"
TARGET_END_DATE = "2018-10-31"

# API Response Limits
MAX_API_CALLS = 50  # Safety limit to prevent infinite loops
BATCH_SIZE = 1000   # API returns 1000 records per call

