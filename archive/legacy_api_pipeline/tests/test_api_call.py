"""
Simple Xilnex Sync Sales API Test Script
Just calls the API and displays sample response data
For demonstration and documentation purposes only

Based on Xilnex API Documentation:
https://developers.xilnex.com/docs/xilnex-developers/beb99101b3573-sync-sales

Author: YONG WERN JIE
Date: November 5, 2025
"""

import requests
import json
from datetime import datetime

# Xilnex Sync API Configuration
API_HOST = "api.xilnex.com"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"

# API Endpoint (v2)
API_ENDPOINT = "/apps/v2/sync/sales"


def call_sync_sales_api(start_timestamp=None):
    """
    Call Xilnex sync sales API and return the response
    
    The API uses timestamp-based pagination:
    - First call: no parameters (gets initial batch)
    - Subsequent calls: use starttimestamp from previous response's lastTimestamp
    - Continue until items array is empty
    
    Args:
        start_timestamp (str, optional): Timestamp from previous call's lastTimestamp
                                        Format: hex string like "0x00000000A333D6F1"
    
    Returns:
        dict: API response as dictionary
    """
    url = f"https://{API_HOST}{API_ENDPOINT}"
    
    headers = {
        'appid': APP_ID,
        'token': TOKEN,
        'auth': AUTH_LEVEL,
        'Content-Type': 'application/json'
    }
    
    params = {}
    if start_timestamp:
        params['starttimestamp'] = start_timestamp
    
    print(f"Calling Xilnex Sync Sales API...")
    print(f"URL: {url}")
    if start_timestamp:
        print(f"Start Timestamp: {start_timestamp}")
    else:
        print(f"Mode: Initial sync (no timestamp)")
    print("-" * 80)
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Time: {response.elapsed.total_seconds():.2f} seconds")
        print("-" * 80)
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {str(e)}")
        return None


def display_response_summary(response_data, batch_num=1):
    """Display a summary of the API response"""
    if not response_data:
        print("No data to display")
        return None, 0
    
    print("\n" + "=" * 80)
    print(f"API RESPONSE SUMMARY - BATCH {batch_num}")
    print("=" * 80)
    
    # Display full response keys to understand structure
    print(f"Response Keys: {list(response_data.keys())}")
    print("-" * 80)
    
    # Display lastTimestamp (critical for pagination)
    if 'lastTimestamp' in response_data:
        print(f"Last Timestamp: {response_data['lastTimestamp']}")
        print(f"  (Use this value as 'starttimestamp' parameter for next call)")
        print("-" * 80)
    
    # Try to find the records - could be dict or list
    records = None
    record_count = 0
    
    if 'items' in response_data:
        items_data = response_data['items']
        if isinstance(items_data, list):
            records = items_data
        elif isinstance(items_data, dict):
            # If items is a dict, display it as such
            print(f"Items structure: Dictionary with {len(items_data)} keys")
            print(f"Items keys: {list(items_data.keys())}")
            records = items_data
        record_count = len(items_data) if isinstance(items_data, (list, dict)) else 0
    elif 'data' in response_data:
        records = response_data['data']
        record_count = len(records) if isinstance(records, (list, dict)) else 0
    
    print(f"Records/Items Count: {record_count}")
    
    if record_count == 0:
        print("  -> Empty response = All data synced (no more records)")
    else:
        print(f"  -> {record_count} item(s) in this batch")
    
    print("-" * 80)
    
    # Display sample records based on type
    if records is not None:
        if isinstance(records, list) and len(records) > 0:
            print(f"\nSample Record (First Transaction):")
            print(json.dumps(records[0], indent=2))
            
            if len(records) > 1:
                print(f"\n\nSample Record (Second Transaction):")
                print(json.dumps(records[1], indent=2))
        elif isinstance(records, dict):
            # If it's a dict, show the full structure
            print(f"\nItems Data Structure (Dictionary):")
            print(json.dumps(records, indent=2))
    
    print("\n" + "=" * 80)
    
    return response_data.get('lastTimestamp'), record_count


def main():
    """
    Main function to test API call
    
    Demonstrates the timestamp-based pagination approach:
    1. First call with no timestamp gets initial batch
    2. Extract lastTimestamp from response
    3. Call again with starttimestamp parameter
    4. Repeat until items array is empty
    """
    print("=" * 80)
    print("XILNEX SYNC SALES API TEST")
    print("=" * 80)
    print(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Endpoint: https://{API_HOST}{API_ENDPOINT}")
    print("=" * 80)
    print("\nThis test demonstrates the timestamp-based pagination:")
    print("  1. Initial call (no timestamp)")
    print("  2. Get 'lastTimestamp' from response")
    print("  3. Next call uses that as 'starttimestamp' parameter")
    print("  4. Repeat until 'items' array is empty")
    print("=" * 80)
    print()
    
    # Track all batches for summary
    all_responses = []
    total_records = 0
    batch_num = 1
    max_batches = 3  # Limit to 3 batches for demonstration
    
    # First call - no timestamp
    print(f"\n{'=' * 80}")
    print(f"BATCH {batch_num}: Initial Sync (no timestamp)")
    print(f"{'=' * 80}\n")
    
    response_data = call_sync_sales_api()
    
    if response_data:
        all_responses.append(response_data)
        last_timestamp, record_count = display_response_summary(response_data, batch_num)
        total_records += record_count
        
        # Save first batch
        output_file = f"api_response_batch{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, indent=2, ensure_ascii=False)
        print(f"\nBatch {batch_num} saved to: {output_file}")
        
        # If there are more records, demonstrate pagination with next batch
        if last_timestamp and record_count > 0 and batch_num < max_batches:
            batch_num += 1
            
            print(f"\n\n{'=' * 80}")
            print(f"BATCH {batch_num}: Continue Sync (using lastTimestamp)")
            print(f"{'=' * 80}\n")
            
            response_data = call_sync_sales_api(last_timestamp)
            
            if response_data:
                all_responses.append(response_data)
                last_timestamp, record_count = display_response_summary(response_data, batch_num)
                total_records += record_count
                
                # Save second batch
                output_file = f"api_response_batch{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(response_data, f, indent=2, ensure_ascii=False)
                print(f"\nBatch {batch_num} saved to: {output_file}")
        
        # Summary
        print(f"\n\n{'=' * 80}")
        print("SYNC SUMMARY")
        print(f"{'=' * 80}")
        print(f"Total Batches Retrieved: {len(all_responses)}")
        print(f"Total Records: {total_records}")
        print(f"Last Timestamp: {last_timestamp if last_timestamp else 'N/A'}")
        print(f"{'=' * 80}")
        print("\nNote: In production, continue calling with lastTimestamp")
        print("      until the API returns an empty 'items' array.")
        print(f"{'=' * 80}\n")
        
    else:
        print("\nAPI call failed. No data returned.")
        print("\nTroubleshooting:")
        print("  - Verify appid, token, and auth level are correct")
        print("  - Check network connectivity")
        print("  - Confirm API endpoint: https://api.xilnex.com/apps/v2/sync/sales")


if __name__ == "__main__":
    main()

