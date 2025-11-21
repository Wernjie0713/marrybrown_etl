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

# Target Data Range (October 2018 - first month API returns)
TARGET_YEAR = 2018
TARGET_MONTH = 10
TARGET_START_DATE = "2018-10-01"
TARGET_END_DATE = "2018-10-31"

# API Response Limits
# ====================
# For PRODUCTION (full dataset):
#   Set MAX_API_CALLS = None for unlimited calls
#   The smart early exit will stop automatically when sufficient data is collected
#
# For DEVELOPMENT/TESTING:
#   Set a reasonable limit like 50-100 to prevent long waits
#   Note: ~10 calls ≈ 10,000 sales (covers ~27 days of Oct 2018)
#         ~50 calls ≈ 50,000 sales (covers full Oct 2018 + buffer)

# MAX_API_CALLS = None  # None = unlimited (for full production runs)
MAX_API_CALLS = 500   # Testing limit (500 calls ≈ 500K records, good for development)

# Use smaller batches locally to avoid 90s timeout on slower networks.
# Cloud runs can override via env var FAST_SAMPLE_BATCH_SIZE or direct edit.
BATCH_SIZE = 100       # API max is 1000; 200 keeps each call under timeout

# Smart Early Exit Configuration
# ================================
# Used by extract_sales_for_period_smart() to stop API calls intelligently
ENABLE_SMART_EXIT = True  # Set to False to fetch ALL data from API
BUFFER_DAYS = 7          # Continue fetching 7 days past target end_date

