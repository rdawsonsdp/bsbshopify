#!/usr/bin/env python3
"""
Health check script for GitHub Actions
Provides quick status overview and validates configuration
"""

import sys
import os
from pathlib import Path
import json
from datetime import datetime

def check_health():
    """Perform health checks and display status"""
    
    print("=== Shopify Sync Health Check ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # Check configuration
    if Path('config.json').exists():
        print("✅ Configuration file found")
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
            
            required_keys = ['shopify_store_name', 'shopify_access_token', 'target_spreadsheet']
            missing = [key for key in required_keys if not config.get(key)]
            
            if missing:
                print(f"❌ Missing config keys: {missing}")
                return False
            else:
                print("✅ All required configuration keys present")
                
        except Exception as e:
            print(f"❌ Error reading config: {e}")
            return False
    else:
        print("❌ Configuration file missing")
        return False
    
    # Check service account file
    service_account_file = config.get('google_service_account_file', 'service_account.json')
    if Path(service_account_file).exists():
        print("✅ Google service account file found")
    else:
        print(f"❌ Google service account file missing: {service_account_file}")
        return False
    
    # Check database
    db_file = config.get('db_path', 'shopify_sync.db')
    if Path(db_file).exists():
        print("✅ Database file exists")
        
        # Quick database check
        try:
            import sqlite3
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM order_tracking')
            count = cursor.fetchone()[0]
            print(f"✅ Database contains {count} tracked orders")
            
            # Check recent sync
            cursor.execute('SELECT MAX(sync_timestamp) FROM sync_history WHERE status = "success"')
            last_sync = cursor.fetchone()[0]
            if last_sync:
                print(f"✅ Last successful sync: {last_sync}")
            else:
                print("⚠️  No successful sync history found")
            
            conn.close()
            
        except Exception as e:
            print(f"⚠️  Database check failed: {e}")
    else:
        print("⚠️  Database file not found (will be created on first run)")
    
    print()
    print("✅ Health check completed")
    return True

if __name__ == "__main__":
    success = check_health()
    sys.exit(0 if success else 1)