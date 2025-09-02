#!/usr/bin/env python
# coding: utf-8
"""
Script to create TEST worksheets in Google Sheets with the latest sync data
This is a workaround for the JWT signature issue
"""

import pandas as pd
import gspread
from google.oauth2 import service_account
import json
from pathlib import Path

def create_test_worksheets():
    """Create TEST worksheets and populate with latest test data"""
    
    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    try:
        # Setup Google authentication
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = service_account.Credentials.from_service_account_file(
            config['google_service_account_file'], 
            scopes=SCOPES
        )
        
        client = gspread.authorize(credentials)
        print("✓ Successfully authenticated with Google")
        
        # Open target spreadsheet
        target_sheet = client.open(config['target_spreadsheet'])
        print(f"✓ Opened spreadsheet: {config['target_spreadsheet']}")
        
        # Find the latest test output files
        test_output_dir = Path('test_output')
        if not test_output_dir.exists():
            print("✗ No test_output directory found. Run the sync in test mode first.")
            return
        
        # Find the latest files
        order_files = list(test_output_dir.glob('orders_to_append_*.csv'))
        line_files = list(test_output_dir.glob('order_lines_to_append_*.csv'))
        
        if not order_files:
            print("✗ No test order files found. Run the sync in test mode first.")
            return
            
        # Get the latest files
        latest_order_file = max(order_files, key=lambda p: p.stat().st_mtime)
        latest_line_file = max(line_files, key=lambda p: p.stat().st_mtime)
        
        print(f"✓ Found latest order file: {latest_order_file}")
        print(f"✓ Found latest line file: {latest_line_file}")
        
        # Read the CSV files
        orders_df = pd.read_csv(latest_order_file)
        lines_df = pd.read_csv(latest_line_file)
        
        print(f"✓ Loaded {len(orders_df)} orders and {len(lines_df)} order lines")
        
        # Create or update TEST Customer Orders worksheet
        try:
            test_orders_worksheet = target_sheet.worksheet("TEST Customer Orders")
            print("✓ Found existing 'TEST Customer Orders' worksheet")
        except:
            test_orders_worksheet = target_sheet.add_worksheet(title="TEST Customer Orders", rows=1000, cols=30)
            print("✓ Created new 'TEST Customer Orders' worksheet")
        
        # Create or update TEST Bakery Products Ordered worksheet  
        try:
            test_lines_worksheet = target_sheet.worksheet("TEST Bakery Products Ordered")
            print("✓ Found existing 'TEST Bakery Products Ordered' worksheet")
        except:
            test_lines_worksheet = target_sheet.add_worksheet(title="TEST Bakery Products Ordered", rows=1000, cols=30)
            print("✓ Created new 'TEST Bakery Products Ordered' worksheet")
        
        # Clear and update orders worksheet
        test_orders_worksheet.clear()
        orders_df_filled = orders_df.fillna('')
        headers = [orders_df_filled.columns.tolist()]
        data_rows = orders_df_filled.values.tolist()
        all_data = headers + data_rows
        
        test_orders_worksheet.update('A1', all_data, value_input_option='USER_ENTERED')
        print(f"✓ Wrote {len(orders_df)} orders to 'TEST Customer Orders' worksheet")
        
        # Clear and update order lines worksheet
        test_lines_worksheet.clear()
        lines_df_filled = lines_df.fillna('')
        headers = [lines_df_filled.columns.tolist()]
        data_rows = lines_df_filled.values.tolist()
        all_data = headers + data_rows
        
        test_lines_worksheet.update('A1', all_data, value_input_option='USER_ENTERED')
        print(f"✓ Wrote {len(lines_df)} order lines to 'TEST Bakery Products Ordered' worksheet")
        
        # Show Order Type distribution
        order_type_counts = orders_df['Order Type'].value_counts()
        print("\n📊 Order Type Distribution:")
        for order_type, count in order_type_counts.items():
            print(f"  {order_type}: {count}")
        
        print(f"\n✅ Success! TEST worksheets have been created/updated in Google Sheets")
        print(f"Total value: ${orders_df['Total'].astype(float).sum():.2f}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        print("The JWT signature error might be due to:")
        print("1. System time synchronization issue")
        print("2. Service account key needs to be refreshed")
        print("3. Permissions issue with the service account")

if __name__ == "__main__":
    create_test_worksheets()