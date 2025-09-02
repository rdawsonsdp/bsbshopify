#!/usr/bin/env python
# coding: utf-8
"""
Shopify Sync Manager - Utility script for managing the sync process
Provides commands for status checking, manual sync, and troubleshooting
"""

import argparse
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

from shopify_sheets_sync import ShopifyOrderSync


class SyncManager:
    """Manager class for sync operations and diagnostics"""
    
    def __init__(self, config_file='config.json'):
        self.sync = ShopifyOrderSync(config_file)
        self.db_path = self.sync.db_path
    
    def show_status(self):
        """Display current sync status"""
        status = self.sync.get_sync_status()
        
        print("=== Shopify Sync Status ===")
        
        if status['last_sync']:
            timestamp, processed, new, sync_status, error = status['last_sync']
            print(f"Last sync: {timestamp}")
            print(f"Status: {sync_status}")
            print(f"Orders processed: {processed}")
            print(f"New orders: {new}")
            if error:
                print(f"Error: {error}")
        else:
            print("No sync history found")
        
        print(f"\nTotal orders tracked: {status['total_tracked']}")
        print(f"Pending errors: {status['pending_errors']}")
    
    def show_recent_orders(self, limit=10):
        """Show recently synced orders"""
        conn = sqlite3.connect(self.db_path)
        query = '''
            SELECT order_number, created_at, sync_timestamp
            FROM order_tracking
            ORDER BY sync_timestamp DESC
            LIMIT ?
        '''
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        
        print(f"\n=== Recent {limit} Orders ===")
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No orders found")
    
    def show_errors(self, unresolved_only=True):
        """Display sync errors"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT error_timestamp, order_id, error_type, error_message, retry_count
            FROM sync_errors
        '''
        
        if unresolved_only:
            query += ' WHERE resolved = FALSE'
        
        query += ' ORDER BY error_timestamp DESC'
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print("\n=== Sync Errors ===")
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No errors found")
    
    def check_missing_orders(self):
        """Check for gaps in order numbers"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT order_number 
            FROM order_tracking 
            WHERE sync_status = "synced" AND order_number IS NOT NULL
            ORDER BY order_number
        ''')
        
        order_numbers = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not order_numbers:
            print("No orders in database")
            return
        
        min_num = min(order_numbers)
        max_num = max(order_numbers)
        expected = set(range(min_num, max_num + 1))
        actual = set(order_numbers)
        missing = sorted(expected - actual)
        
        print(f"\n=== Order Number Analysis ===")
        print(f"Range: {min_num} - {max_num}")
        print(f"Total expected: {len(expected)}")
        print(f"Total synced: {len(actual)}")
        
        if missing:
            print(f"Missing {len(missing)} orders:")
            # Show first 20 missing orders
            for num in missing[:20]:
                print(f"  - Order #{num}")
            if len(missing) > 20:
                print(f"  ... and {len(missing) - 20} more")
        else:
            print("No gaps found in order numbers")
    
    def reset_order_status(self, order_number):
        """Reset sync status for a specific order"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM order_tracking 
            WHERE order_number = ?
        ''', (order_number,))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected:
            print(f"Reset status for order #{order_number}")
        else:
            print(f"Order #{order_number} not found in tracking database")
    
    def export_tracking_data(self, output_file='tracking_export.csv'):
        """Export tracking database to CSV"""
        conn = sqlite3.connect(self.db_path)
        query = 'SELECT * FROM order_tracking ORDER BY order_number'
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        df.to_csv(output_file, index=False)
        print(f"Exported {len(df)} orders to {output_file}")
    
    def validate_sheets_connection(self):
        """Test connection to Google Sheets"""
        try:
            self.sync.setup_google_auth()
            max_id, col = self.sync.get_max_web_orderid_from_sheets()
            print("✓ Google Sheets connection successful")
            print(f"  Maximum WebOrderID in sheets: {max_id}")
            print(f"  Order Type column: {col}")
        except Exception as e:
            print("✗ Google Sheets connection failed:")
            print(f"  {e}")
    
    def validate_shopify_connection(self):
        """Test connection to Shopify API"""
        try:
            # Try to fetch just one order
            test_date = datetime.now() - timedelta(days=1)
            orders = self.sync.fetch_shopify_orders(test_date)
            print("✓ Shopify API connection successful")
            print(f"  Found {len(orders)} orders in last 24 hours")
        except Exception as e:
            print("✗ Shopify API connection failed:")
            print(f"  {e}")
    
    def run_sync(self, force=False, test_mode=False):
        """Run the sync process"""
        if not force:
            # Check if sync was run recently
            status = self.sync.get_sync_status()
            if status['last_sync']:
                last_sync_time = datetime.fromisoformat(status['last_sync'][0])
                time_since = datetime.now() - last_sync_time
                if time_since.total_seconds() < 300:  # 5 minutes
                    print("Sync was run recently. Use --force to override.")
                    return
        
        mode_str = "TEST MODE" if test_mode else "PRODUCTION MODE"
        print(f"Starting sync process in {mode_str}...")
        self.sync.run_sync(test_mode=test_mode)
    
    def generate_report(self, start_date=None, end_date=None):
        """Generate a sync report for a date range"""
        if not start_date:
            start_date = datetime.now() - timedelta(days=7)
        if not end_date:
            end_date = datetime.now()
        
        conn = sqlite3.connect(self.db_path)
        
        # Get sync history
        sync_query = '''
            SELECT * FROM sync_history
            WHERE sync_timestamp BETWEEN ? AND ?
            ORDER BY sync_timestamp
        '''
        sync_df = pd.read_sql_query(sync_query, conn, params=[
            start_date.isoformat(), end_date.isoformat()
        ])
        
        # Get order statistics
        order_query = '''
            SELECT DATE(sync_timestamp) as sync_date, COUNT(*) as orders_synced
            FROM order_tracking
            WHERE sync_timestamp BETWEEN ? AND ?
            GROUP BY DATE(sync_timestamp)
        '''
        order_df = pd.read_sql_query(order_query, conn, params=[
            start_date.isoformat(), end_date.isoformat()
        ])
        
        conn.close()
        
        print(f"\n=== Sync Report: {start_date.date()} to {end_date.date()} ===")
        
        if not sync_df.empty:
            total_syncs = len(sync_df)
            successful_syncs = len(sync_df[sync_df['status'] == 'success'])
            total_orders = sync_df['orders_processed'].sum()
            
            print(f"Total sync runs: {total_syncs}")
            print(f"Successful syncs: {successful_syncs}")
            print(f"Success rate: {successful_syncs/total_syncs*100:.1f}%")
            print(f"Total orders processed: {total_orders}")
            
            print("\nDaily order sync summary:")
            if not order_df.empty:
                print(order_df.to_string(index=False))
        else:
            print("No sync activity in this period")


def main():
    """Main entry point for the sync manager"""
    parser = argparse.ArgumentParser(description='Shopify Sync Manager')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show sync status')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Run sync process')
    sync_parser.add_argument('--force', action='store_true', help='Force sync even if recently run')
    sync_parser.add_argument('--test', action='store_true', help='Run in test mode (no writes to Google Sheets)')
    
    # Orders command
    orders_parser = subparsers.add_parser('orders', help='Show recent orders')
    orders_parser.add_argument('--limit', type=int, default=10, help='Number of orders to show')
    
    # Errors command
    errors_parser = subparsers.add_parser('errors', help='Show sync errors')
    errors_parser.add_argument('--all', action='store_true', help='Show all errors including resolved')
    
    # Missing command
    missing_parser = subparsers.add_parser('missing', help='Check for missing orders')
    
    # Reset command
    reset_parser = subparsers.add_parser('reset', help='Reset order sync status')
    reset_parser.add_argument('order_number', type=int, help='Order number to reset')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export tracking data')
    export_parser.add_argument('--output', default='tracking_export.csv', help='Output file name')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate connections')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate sync report')
    report_parser.add_argument('--days', type=int, default=7, help='Number of days to report')
    
    args = parser.parse_args()
    
    # Create manager instance
    manager = SyncManager()
    
    # Execute command
    if args.command == 'status':
        manager.show_status()
    elif args.command == 'sync':
        manager.run_sync(force=args.force, test_mode=args.test)
    elif args.command == 'orders':
        manager.show_recent_orders(limit=args.limit)
    elif args.command == 'errors':
        manager.show_errors(unresolved_only=not args.all)
    elif args.command == 'missing':
        manager.check_missing_orders()
    elif args.command == 'reset':
        manager.reset_order_status(args.order_number)
    elif args.command == 'export':
        manager.export_tracking_data(args.output)
    elif args.command == 'validate':
        print("Validating connections...")
        manager.validate_shopify_connection()
        manager.validate_sheets_connection()
    elif args.command == 'report':
        start_date = datetime.now() - timedelta(days=args.days)
        manager.generate_report(start_date=start_date)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()