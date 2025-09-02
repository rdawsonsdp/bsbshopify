#!/usr/bin/env python
# coding: utf-8
"""
Test runner for Shopify sync
This script runs the sync in test mode and creates detailed logs
"""

import os
import sys
from datetime import datetime
from pathlib import Path

def main():
    print("="*60)
    print("Shopify to Google Sheets Sync - TEST MODE")
    print("="*60)
    print()
    print("This will:")
    print("1. Connect to Shopify and fetch orders")
    print("2. Connect to Google Sheets and read existing data")
    print("3. Process orders but NOT write to Google Sheets")
    print("4. Save results to test_output/ directory for review")
    print()
    
    response = input("Continue with test sync? (y/N): ")
    if response.lower() != 'y':
        print("Test sync cancelled.")
        return
    
    print("\nRunning test sync...")
    print("-"*60)
    
    # Create test output directory
    Path('test_output').mkdir(exist_ok=True)
    
    # Run the sync in test mode
    os.system('python shopify_sheets_sync.py --test')
    
    print("-"*60)
    print("\nTest sync complete!")
    print("\nPlease check the following files:")
    print("1. shopify_sync.log - Detailed log of all operations")
    print("2. test_output/ - Directory containing:")
    print("   - orders_to_append_*.csv - Orders that would be added")
    print("   - order_lines_to_append_*.csv - Order line items")
    print("   - sync_summary_*.txt - Summary of the sync")
    print()
    print("Review these files before running the production sync.")
    print("\nTo run production sync, use:")
    print("  python shopify_sheets_sync.py")
    print("  OR")
    print("  python sync_manager.py sync")

if __name__ == "__main__":
    main()