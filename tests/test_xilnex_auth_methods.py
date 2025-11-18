"""
Test Different Authentication Methods for Xilnex API
Tries multiple authentication formats to find the correct one

Author: YONG WERN JIE
Date: October 27, 2025
"""

import requests
import json
import base64

# Xilnex API Credentials
API_BASE_URL = "https://api.xilnex.com/apps/v2/sync/sales"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"


def test_method_1():
    """Method 1: Custom headers (what we tried)"""
    print("\n" + "="*70)
    print("METHOD 1: Custom Headers (appid, token, auth)")
    print("="*70)
    
    headers = {
        "Content-Type": "application/json",
        "appid": APP_ID,
        "token": TOKEN,
        "auth": AUTH_LEVEL,
    }
    
    response = requests.get(API_BASE_URL, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def test_method_2():
    """Method 2: Bearer Token"""
    print("\n" + "="*70)
    print("METHOD 2: Bearer Token Authorization")
    print("="*70)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    
    response = requests.get(API_BASE_URL, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def test_method_3():
    """Method 3: Basic Authentication (APP_ID:TOKEN)"""
    print("\n" + "="*70)
    print("METHOD 3: Basic Auth (APP_ID:TOKEN)")
    print("="*70)
    
    # Create Basic Auth string
    credentials = f"{APP_ID}:{TOKEN}"
    encoded = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded}",
    }
    
    response = requests.get(API_BASE_URL, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def test_method_4():
    """Method 4: requests.auth.HTTPBasicAuth"""
    print("\n" + "="*70)
    print("METHOD 4: HTTPBasicAuth (APP_ID, TOKEN)")
    print("="*70)
    
    from requests.auth import HTTPBasicAuth
    
    response = requests.get(
        API_BASE_URL, 
        auth=HTTPBasicAuth(APP_ID, TOKEN),
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def test_method_5():
    """Method 5: Query Parameters"""
    print("\n" + "="*70)
    print("METHOD 5: Query Parameters")
    print("="*70)
    
    params = {
        "appid": APP_ID,
        "token": TOKEN,
        "auth": AUTH_LEVEL,
    }
    
    response = requests.get(API_BASE_URL, params=params, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def test_method_6():
    """Method 6: Mixed - Headers + Query Params"""
    print("\n" + "="*70)
    print("METHOD 6: Headers + Query Parameters Combined")
    print("="*70)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    
    params = {
        "appid": APP_ID,
        "auth": AUTH_LEVEL,
    }
    
    response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def test_method_7():
    """Method 7: X-API-Key header"""
    print("\n" + "="*70)
    print("METHOD 7: X-API-Key Header")
    print("="*70)
    
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": TOKEN,
        "X-APP-ID": APP_ID,
    }
    
    response = requests.get(API_BASE_URL, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
    else:
        print("[SUCCESS]")
    return response.status_code == 200


def main():
    print("="*70)
    print("XILNEX API AUTHENTICATION METHOD TESTING")
    print("="*70)
    print("Testing different authentication formats to find the correct one...")
    print(f"API Endpoint: {API_BASE_URL}")
    
    methods = [
        ("Custom Headers", test_method_1),
        ("Bearer Token", test_method_2),
        ("Basic Auth (encoded)", test_method_3),
        ("HTTPBasicAuth", test_method_4),
        ("Query Parameters", test_method_5),
        ("Mixed (Headers + Params)", test_method_6),
        ("X-API-Key", test_method_7),
    ]
    
    successful_methods = []
    
    for name, test_func in methods:
        try:
            if test_func():
                successful_methods.append(name)
                print(f"[SUCCESS] {name} worked!")
        except Exception as e:
            print(f"[ERROR] {name} failed with exception: {e}")
    
    print("\n" + "="*70)
    print("RESULTS SUMMARY")
    print("="*70)
    
    if successful_methods:
        print(f"\n[SUCCESS] Working authentication methods:")
        for method in successful_methods:
            print(f"  - {method}")
        print("\nUse the successful method in your ETL scripts!")
    else:
        print("\n[FAILED] None of the methods worked.")
        print("\nNext steps:")
        print("1. Check Xilnex documentation for correct authentication format")
        print("2. Contact Xilnex support with your credentials")
        print("3. Verify the API endpoint URL is correct")
        print("4. Check if there's an API documentation page")
        
        print("\nAPI Response Headers from last attempt:")
        print("(Look for 'WWW-Authenticate' header for hints)")


if __name__ == "__main__":
    main()

