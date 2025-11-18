#!/usr/bin/env python3
"""
Simple API Test - Get 1 Call (1000 Records)
Makes exactly 1 API call to Xilnex and saves the response to JSON
For analyzing the actual data structure and date formats

Author: YONG WERN JIE
Date: November 12, 2025
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


def call_single_api_call():
    """
    Make exactly 1 API call and save the response
    Returns: dict with API response data
    """
    url = f"https://{API_HOST}{API_ENDPOINT}"
    
    headers = {
        'appid': APP_ID,
        'token': TOKEN,
        'auth': AUTH_LEVEL,
        'Content-Type': 'application/json'
    }
    
    # Parameters for 1 call with 1000 records
    params = {
        'limit': '1000',
        'mode': 'ByDateTime'
    }
    
    print("="*80)
    print("SINGLE API CALL TEST - 1000 Records")
    print("="*80)
    print(f"URL: {url}")
    print(f"Parameters: {params}")
    print("-"*80)
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Time: {response.elapsed.total_seconds():.2f} seconds")
        print(f"Response Size: {len(response.content):,} bytes")
        print("-"*80)
        
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


def analyze_response_structure(response_data):
    """
    Analyze and display the response structure
    """
    if not response_data:
        print("No data to analyze")
        return
    
    print("\n" + "="*80)
    print("RESPONSE STRUCTURE ANALYSIS")
    print("="*80)
    
    # Top-level keys
    print(f"Top-level Keys: {list(response_data.keys())}")
    print("-"*80)
    
    # Check for data section
    if 'data' in response_data:
        data_section = response_data['data']
        print(f"Data Keys: {list(data_section.keys())}")
        print("-"*80)
        
        # Check for sales array
        if 'sales' in data_section:
            sales_array = data_section['sales']
            print(f"Sales Records Count: {len(sales_array):,}")
            
            if len(sales_array) > 0:
                # Show first record structure
                first_record = sales_array[0]
                print(f"First Record Keys: {list(first_record.keys())}")
                print("-"*80)
                
                # Look for date-related fields
                date_fields = []
                for key in first_record.keys():
                    if any(word in key.lower() for word in ['date', 'time', 'created', 'updated']):
                        date_fields.append(key)
                
                print(f"Date-Related Fields Found: {date_fields}")
                
                # Show sample values for date fields
                for field in date_fields[:5]:  # First 5 date fields
                    sample_value = first_record.get(field, 'N/A')
                    print(f"  {field}: {sample_value}")
                
                print("-"*80)
                
                # Show first few records completely
                print("SAMPLE RECORDS (first 3):")
                for i, record in enumerate(sales_array[:3]):
                    print(f"\nRecord {i+1}:")
                    for key, value in record.items():
                        if isinstance(value, str) and len(value) > 100:
                            print(f"  {key}: {value[:100]}...")
                        else:
                            print(f"  {key}: {value}")
        
        # Check for lastTimestamp
        if 'lastTimestamp' in data_section:
            print(f"Last Timestamp: {data_section['lastTimestamp']}")
    
    print("="*80)


def save_response_to_file(response_data):
    """
    Save the API response to a JSON file
    """
    if not response_data:
        print("No data to save")
        return
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"single_api_call_1000_records_{timestamp}.json"
    
    # Save to api_data directory
    import os
    api_data_dir = "api_data"
    os.makedirs(api_data_dir, exist_ok=True)
    
    filepath = os.path.join(api_data_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(response_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Response saved to: {filepath}")
    print(f"   File size: {os.path.getsize(filepath):,} bytes")
    return filepath


def main():
    """
    Main execution function
    """
    print("Making 1 API call to get 1000 records for analysis...")
    
    # Make the API call
    response_data = call_single_api_call()
    
    if response_data:
        # Analyze the structure
        analyze_response_structure(response_data)
        
        # Save to file
        saved_file = save_response_to_file(response_data)
        
        print(f"\n🎯 SUCCESS!")
        print(f"   - API call completed successfully")
        print(f"   - Response structure analyzed above")
        print(f"   - Data saved to: {saved_file}")
        print(f"   - Use this file to fix the date parsing logic")
        
    else:
        print(f"\n❌ FAILED!")
        print(f"   - API call failed")
        print(f"   - Check network connectivity and API credentials")


if __name__ == "__main__":
    main()
