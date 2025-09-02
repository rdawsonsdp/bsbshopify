#!/usr/bin/env python
# coding: utf-8
"""
Simple test script to diagnose Google Sheets authentication issues
"""

import gspread
from google.oauth2 import service_account
import json
from datetime import datetime
import time

def test_google_auth():
    print("=== Google Sheets Authentication Test ===")
    print(f"System time: {datetime.now()}")
    print(f"UTC time: {datetime.utcnow()}")
    
    try:
        # Load config
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        print(f"Service account file: {config['google_service_account_file']}")
        print(f"Target spreadsheet: {config['target_spreadsheet']}")
        
        # Setup credentials
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        print("Creating credentials...")
        credentials = service_account.Credentials.from_service_account_file(
            config['google_service_account_file'], 
            scopes=SCOPES
        )
        print("✓ Credentials created successfully")
        
        print("Authorizing gspread client...")
        client = gspread.authorize(credentials)
        print("✓ Client authorized successfully")
        
        print("Testing spreadsheet access...")
        try:
            # Try to open the spreadsheet
            spreadsheet = client.open(config['target_spreadsheet'])
            print(f"✓ Successfully opened spreadsheet: {spreadsheet.title}")
            
            # Try to list worksheets
            worksheets = spreadsheet.worksheets()
            print(f"✓ Found {len(worksheets)} worksheets:")
            for ws in worksheets:
                print(f"  - {ws.title}")
            
            # Try to access the Customer Orders worksheet
            try:
                customer_orders_ws = spreadsheet.worksheet("Customer Orders")
                print("✓ Successfully accessed 'Customer Orders' worksheet")
                
                # Try to read a small sample
                sample_data = customer_orders_ws.get('A1:C3')
                print(f"✓ Successfully read sample data: {len(sample_data)} rows")
                
            except Exception as ws_error:
                print(f"✗ Error accessing Customer Orders worksheet: {ws_error}")
            
        except Exception as sheet_error:
            print(f"✗ Error opening spreadsheet: {sheet_error}")
            
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        
        # Additional debugging
        if "invalid_grant" in str(e).lower():
            print("\nDEBUG: Invalid grant error detected")
            print("This is usually caused by:")
            print("1. System clock being out of sync")
            print("2. Service account key being expired or revoked")
            print("3. Incorrect service account permissions")
            
            # Check if the service account email is in the right format
            try:
                with open(config['google_service_account_file'], 'r') as f:
                    sa_data = json.load(f)
                    email = sa_data.get('client_email', '')
                    if email:
                        print(f"4. Service account email: {email}")
                        print("   Make sure this email has been granted access to the spreadsheet")
            except:
                pass

if __name__ == "__main__":
    test_google_auth()