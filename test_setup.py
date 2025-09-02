#!/usr/bin/env python
# coding: utf-8
"""
Test script to verify the Shopify sync setup
Run this before the full sync to ensure everything is configured correctly
"""

import sys
import os
from pathlib import Path
import json

def test_imports():
    """Test if all required packages are installed"""
    print("Testing required imports...")
    required_packages = [
        ('gspread', 'gspread'),
        ('requests', 'requests'),
        ('pandas', 'pandas'),
        ('numpy', 'numpy'),
        ('google.oauth2', 'google-auth'),
        ('gspread_pandas', 'gspread-pandas'),
        ('dotenv', 'python-dotenv')
    ]
    
    all_good = True
    for module_name, package_name in required_packages:
        try:
            __import__(module_name)
            print(f"✓ {module_name} imported successfully")
        except ImportError:
            print(f"✗ Failed to import {module_name}. Install with: pip install {package_name}")
            all_good = False
    
    return all_good

def test_config():
    """Test configuration setup"""
    print("\nTesting configuration...")
    
    # Check for config file
    config_exists = Path('config.json').exists()
    env_exists = Path('.env').exists()
    
    if not config_exists and not env_exists:
        print("✗ No configuration found. Create either config.json or .env file")
        return False
    
    if config_exists:
        print("✓ Found config.json")
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                
            # Check required fields
            required_fields = ['shopify_access_token', 'google_service_account_file']
            for field in required_fields:
                if field in config and config[field] and config[field] != f"YOUR_{field.upper()}_HERE":
                    print(f"✓ {field} is configured")
                else:
                    print(f"✗ {field} is not configured properly")
                    return False
                    
            # Check if Google service account file exists
            if Path(config['google_service_account_file']).exists():
                print("✓ Google service account file exists")
            else:
                print(f"✗ Google service account file not found: {config['google_service_account_file']}")
                return False
                
        except Exception as e:
            print(f"✗ Error reading config.json: {e}")
            return False
    
    if env_exists:
        print("✓ Found .env file")
        # Load and check env vars
        from dotenv import load_dotenv
        load_dotenv()
        
        if os.environ.get('SHOPIFY_ACCESS_TOKEN'):
            print("✓ SHOPIFY_ACCESS_TOKEN found in environment")
        else:
            print("✗ SHOPIFY_ACCESS_TOKEN not found in environment")
            
        if os.environ.get('GOOGLE_SERVICE_ACCOUNT_FILE'):
            print("✓ GOOGLE_SERVICE_ACCOUNT_FILE found in environment")
        else:
            print("✗ GOOGLE_SERVICE_ACCOUNT_FILE not found in environment")
    
    return True

def test_shopify_connection():
    """Test basic Shopify API connection"""
    print("\nTesting Shopify API connection...")
    
    try:
        from shopify_sheets_sync import ShopifyOrderSync
        sync = ShopifyOrderSync()
        
        # Try to fetch store info
        import requests
        url = f"https://{sync.config['shopify_store_name']}.myshopify.com/admin/api/2023-04/shop.json"
        headers = {"X-Shopify-Access-Token": sync.config['shopify_access_token']}
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            shop_data = response.json()
            print(f"✓ Connected to Shopify store: {shop_data['shop']['name']}")
            return True
        else:
            print(f"✗ Failed to connect to Shopify API: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"✗ Error testing Shopify connection: {e}")
        return False

def test_google_connection():
    """Test Google Sheets connection"""
    print("\nTesting Google Sheets connection...")
    
    try:
        from shopify_sheets_sync import ShopifyOrderSync
        sync = ShopifyOrderSync()
        sync.setup_google_auth()
        
        # Try to list spreadsheets
        spreadsheets = sync.google_client.openall()
        print(f"✓ Connected to Google Sheets. Found {len(spreadsheets)} spreadsheets")
        
        # Check if target spreadsheet exists
        target_name = sync.config['target_spreadsheet']
        found = False
        for sheet in spreadsheets:
            if sheet.title == target_name:
                found = True
                break
                
        if found:
            print(f"✓ Target spreadsheet '{target_name}' found")
        else:
            print(f"✗ Target spreadsheet '{target_name}' not found")
            print("  Available spreadsheets:")
            for sheet in spreadsheets[:5]:
                print(f"    - {sheet.title}")
                
        return found
        
    except Exception as e:
        print(f"✗ Error testing Google Sheets connection: {e}")
        return False

def main():
    """Run all tests"""
    print("=== Shopify Sync Setup Test ===\n")
    
    tests = [
        ("Package imports", test_imports),
        ("Configuration", test_config),
        ("Shopify API", test_shopify_connection),
        ("Google Sheets", test_google_connection)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ Error running {test_name}: {e}")
            results.append((test_name, False))
    
    print("\n=== Test Summary ===")
    all_passed = True
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        print("\nAll tests passed! You can now run the sync script.")
    else:
        print("\nSome tests failed. Please fix the issues before running the sync.")
        sys.exit(1)

if __name__ == "__main__":
    main()