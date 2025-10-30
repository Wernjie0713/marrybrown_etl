"""
Xilnex Sync API Test Script (Based on Official Documentation)
Tests the Xilnex Sync Sales API using the correct format from docs

Author: YONG WERN JIE
Date: October 27, 2025
Reference: https://developers.xilnex.com/docs/xilnex-developers/beb99101b3573-sync-sales
"""

import http.client
import json
from datetime import datetime

# Xilnex API Credentials (COM_5013 - Marrybrown)
API_HOST = "api.xilnex.com"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"


def test_sync_sales_api(start_timestamp=None, outlet_list=None, end_timestamp=None):
    """
    Test Xilnex Sync Sales API using format from official documentation
    
    Args:
        start_timestamp: Optional - Continue from last sync (e.g., "0x00000000A333D6F1")
        outlet_list: Optional - Filter by outlet (comma-separated if multiple)
        end_timestamp: Optional - End timestamp for sync range
    """
    print("="*80)
    print("XILNEX SYNC SALES API TEST")
    print("="*80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Host: {API_HOST}")
    print()
    
    # Build URL path with query parameters
    url_path = "/apps/v2/sync/sales"
    query_params = []
    
    if start_timestamp:
        query_params.append(f"starttimestamp={start_timestamp}")
    if outlet_list:
        query_params.append(f"outletList={outlet_list}")
    if end_timestamp:
        query_params.append(f"endtimestamp={end_timestamp}")
    
    if query_params:
        url_path += "?" + "&".join(query_params)
    
    print(f"URL Path: {url_path}")
    print()
    
    # Build headers based on documentation
    headers = {
        'Accept': 'application/json, text/json, text/html, application/*+json',
        'Content-Type': 'application/json',
        'appid': APP_ID,
        'token': TOKEN,
        'auth': AUTH_LEVEL,
    }
    
    print("Headers:")
    for key, value in headers.items():
        if key in ['appid', 'token']:
            print(f"  {key}: {value[:20]}...")
        else:
            print(f"  {key}: {value}")
    print()
    
    try:
        # Create HTTPS connection (as shown in documentation)
        print("Connecting to API...")
        conn = http.client.HTTPSConnection(API_HOST, timeout=30)
        
        # Make GET request
        print("Sending GET request...")
        conn.request("GET", url_path, headers=headers)
        
        # Get response
        res = conn.getresponse()
        status = res.status
        reason = res.reason
        
        print(f"Status: {status} {reason}")
        print()
        
        # Read response data
        data = res.read()
        response_text = data.decode("utf-8")
        
        if status == 200:
            print("[SUCCESS] API Response received!")
            print("-" * 80)
            
            try:
                # Parse JSON
                json_data = json.loads(response_text)
                
                # Pretty print first 2000 characters
                print("Response Preview:")
                print(json.dumps(json_data, indent=2, default=str)[:2000])
                print()
                
                # Analyze structure
                print("-" * 80)
                print("RESPONSE ANALYSIS:")
                print("-" * 80)
                
                if isinstance(json_data, dict):
                    print(f"Type: Dictionary")
                    print(f"Keys: {list(json_data.keys())}")
                    
                    # Check for data structure
                    if "data" in json_data:
                        data_obj = json_data["data"]
                        if isinstance(data_obj, dict):
                            print(f"\nData keys: {list(data_obj.keys())}")
                            
                            # Check for sales items
                            if "sales" in data_obj:
                                sales = data_obj["sales"]
                                print(f"\n[SALES] Count: {len(sales)}")
                                
                                if sales and len(sales) > 0:
                                    first_sale = sales[0]
                                    print(f"\nFirst Sale Structure:")
                                    print(f"  Total Fields: {len(first_sale)}")
                                    print(f"  Fields: {list(first_sale.keys())}")
                                    
                                    print(f"\n  Sample Values (first 10 fields):")
                                    for key, value in list(first_sale.items())[:10]:
                                        print(f"    {key}: {value}")
                            
                            # Check for timestamp
                            if "lastTimestamp" in data_obj:
                                timestamp = data_obj["lastTimestamp"]
                                print(f"\n[TIMESTAMP] {timestamp}")
                                print("  ^ Save this for next sync!")
                    
                    # Check for ok/status fields
                    if "ok" in json_data:
                        print(f"\nAPI Status OK: {json_data['ok']}")
                    if "status" in json_data:
                        print(f"Status: {json_data['status']}")
                    if "warning" in json_data:
                        print(f"Warning: {json_data['warning']}")
                
                # Save full response
                output_file = f"xilnex_sales_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, default=str)
                print(f"\n[SAVED] Full response: {output_file}")
                
                return json_data
                
            except json.JSONDecodeError:
                print("[ERROR] Response is not valid JSON:")
                print(response_text[:1000])
                return None
                
        else:
            print(f"[ERROR] API returned error {status}")
            print("Response:")
            print(response_text[:1000])
            return None
            
    except Exception as e:
        print(f"[ERROR] Exception occurred: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    """Main test function"""
    print("\n" + "="*80)
    print("TEST 1: Initial Sync (Get First Batch)")
    print("="*80)
    
    # Test 1: Get first batch without any parameters
    result = test_sync_sales_api()
    
    if result:
        # If successful, try to get the timestamp for next test
        timestamp = None
        if isinstance(result, dict):
            if "data" in result and "lastTimestamp" in result["data"]:
                timestamp = result["data"]["lastTimestamp"]
            elif "lastTimestamp" in result:
                timestamp = result["lastTimestamp"]
        
        if timestamp:
            print("\n" + "="*80)
            print("TEST 2: Incremental Sync (Using Timestamp)")
            print("="*80)
            print(f"Using timestamp from previous response: {timestamp}")
            print()
            
            # Test 2: Get next batch using timestamp
            test_sync_sales_api(start_timestamp=timestamp)
    
    print("\n" + "="*80)
    print("NEXT STEPS:")
    print("="*80)
    print("1. Check the saved JSON file for full response structure")
    print("2. Compare API fields with your warehouse schema (staging_sales)")
    print("3. Test other endpoints: /salesitems, /payments, /items")
    print("4. If successful, design new ETL using API instead of direct DB queries")
    print("="*80)


if __name__ == "__main__":
    main()
