#!/usr/bin/env python3
"""
Test script to fix Google Sheets authentication issues
"""

import gspread
from google.oauth2 import service_account
import json
import time
from datetime import datetime, timezone

def test_auth_with_fixes():
    """Test authentication with various fixes"""
    
    print("=== Google Sheets Authentication Fix Test ===")
    
    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    service_account_file = config['google_service_account_file']
    target_spreadsheet = config['target_spreadsheet']
    
    print(f"Service account file: {service_account_file}")
    print(f"Target spreadsheet: {target_spreadsheet}")
    
    try:
        # Method 1: Standard approach with explicit scopes
        print("\n--- Method 1: Standard Authentication ---")
        
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file, 
            scopes=SCOPES
        )
        
        # Check if credentials are expired and refresh if needed
        if credentials.expired:
            print("Credentials expired, refreshing...")
            credentials.refresh()
        
        client = gspread.authorize(credentials)
        print("✓ Authentication successful")
        
        # Test spreadsheet access
        try:
            spreadsheet = client.open(target_spreadsheet)
            print(f"✓ Successfully opened spreadsheet: {spreadsheet.title}")
            
            worksheets = spreadsheet.worksheets()
            print(f"✓ Found {len(worksheets)} worksheets:")
            for ws in worksheets:
                print(f"  - {ws.title}")
            
            # Try to read from Customer Orders worksheet
            try:
                customer_orders = spreadsheet.worksheet("Customer Orders")
                sample_data = customer_orders.get('A1:C3')
                print(f"✓ Successfully read {len(sample_data)} rows from Customer Orders")
                return True
                
            except Exception as ws_error:
                print(f"⚠ Warning: Could not access Customer Orders worksheet: {ws_error}")
                print("  This might be expected if the worksheet doesn't exist yet")
                return True
                
        except Exception as sheet_error:
            print(f"✗ Error accessing spreadsheet: {sheet_error}")
            
            if "not found" in str(sheet_error).lower():
                print("  Solution: Make sure the spreadsheet name is correct")
                print("  Or create the spreadsheet manually")
            elif "permission" in str(sheet_error).lower():
                print("  Solution: Share the spreadsheet with the service account email:")
                print(f"  pythonsheets@long-canto-360620.iam.gserviceaccount.com")
            
            return False
            
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        
        error_str = str(e).lower()
        if "invalid_grant" in error_str and "jwt" in error_str:
            print("\n--- JWT Signature Error Detected ---")
            print("Common solutions:")
            print("1. System time sync issue - run: sudo sntp -sS time.apple.com")
            print("2. Service account key might be expired")
            print("3. Check Google Cloud Console for the service account status")
            
        return False

def check_service_account_info():
    """Display service account information"""
    print("\n=== Service Account Information ===")
    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        with open(config['google_service_account_file'], 'r') as f:
            sa_data = json.load(f)
        
        print(f"Project ID: {sa_data.get('project_id')}")
        print(f"Client Email: {sa_data.get('client_email')}")
        print(f"Private Key ID: {sa_data.get('private_key_id')}")
        print(f"Client ID: {sa_data.get('client_id')}")
        
        print(f"\nTo fix access issues:")
        print(f"1. Share your Google Sheet with: {sa_data.get('client_email')}")
        print(f"2. Give it Editor permissions")
        print(f"3. Enable Google Sheets API in Google Cloud Console")
        
    except Exception as e:
        print(f"Error reading service account info: {e}")

if __name__ == "__main__":
    success = test_auth_with_fixes()
    check_service_account_info()
    
    if success:
        print(f"\n✅ Authentication is working! You can now run the sync in production mode.")
    else:
        print(f"\n❌ Authentication still failing. Please check the solutions above.")