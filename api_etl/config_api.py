"""
API ETL Configuration
Xilnex Sync API credentials and target parameters

UPDATED: November 7, 2025
Added optimized ETL configurations for production use
"""

# Xilnex Sync API Credentials
API_HOST = "api.xilnex.com"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"


# Production configuration
BATCH_SIZE = 1000      # Batch size per API call (API max limit is 1000)
MAX_API_CALLS = 250    # Maximum API calls limit

# Smart Early Exit Configuration
# ================================
# Used by extract_sales_for_period_smart() to stop API calls intelligently
ENABLE_SMART_EXIT = True  # Set to False to fetch ALL data from API
BUFFER_DAYS = 7          # Continue fetching 7 days past target end_date

