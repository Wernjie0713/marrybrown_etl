"""
Quick Xilnex API Explorer
Simple interactive script to test different endpoints

Usage:
    python explore_xilnex_api.py sales
    python explore_xilnex_api.py salesitems
    python explore_xilnex_api.py payments --timestamp 0x00000000A333D6F1
"""

import requests
import json
import sys
from datetime import datetime

# Credentials
API_BASE_URL = "https://api.xilnex.com/apps/v2/sync"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"


def call_api(endpoint, start_timestamp=None):
    """Call Xilnex Sync API"""
    url = f"{API_BASE_URL}/{endpoint}"
    
    headers = {
        "Content-Type": "application/json",
        "appid": APP_ID,
        "token": TOKEN,
        "auth": AUTH_LEVEL,
    }
    
    params = {}
    if start_timestamp:
        params["starttimestamp"] = start_timestamp
    
    print(f"Calling: {url}")
    if params:
        print(f"Params: {params}")
    print()
    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        
        # Show summary
        if isinstance(data, dict):
            if "items" in data:
                print(f"[OK] Items: {len(data['items'])}")
            if "lastTimestamp" in data:
                print(f"[TIMESTAMP] {data['lastTimestamp']}")
                print(f"   (Save this for next sync!)")
            
            # Show first item fields
            if "items" in data and data["items"]:
                first_item = data["items"][0]
                print(f"\n[FIELDS] Available ({len(first_item)}):")
                for key in first_item.keys():
                    value = first_item[key]
                    # Truncate long values
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:50] + "..."
                    print(f"  - {key}: {value}")
        
        # Save to file
        filename = f"xilnex_{endpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"\n[SAVED] {filename}")
        
        return data
    else:
        print(f"[ERROR] {response.text}")
        return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python explore_xilnex_api.py <endpoint> [--timestamp <value>]")
        print("\nExamples:")
        print("  python explore_xilnex_api.py sales")
        print("  python explore_xilnex_api.py salesitems")
        print("  python explore_xilnex_api.py payments --timestamp 0x00000000A333D6F1")
        print("\nAvailable endpoints:")
        print("  - sales")
        print("  - salesitems (or salesitem, sales-items)")
        print("  - payments")
        print("  - items")
        print("  - customers")
        print("  - locations")
        sys.exit(1)
    
    endpoint = sys.argv[1]
    timestamp = None
    
    if "--timestamp" in sys.argv:
        idx = sys.argv.index("--timestamp")
        if idx + 1 < len(sys.argv):
            timestamp = sys.argv[idx + 1]
    
    print("="*60)
    print("XILNEX API EXPLORER")
    print("="*60)
    
    call_api(endpoint, timestamp)


if __name__ == "__main__":
    main()

