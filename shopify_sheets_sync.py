#!/usr/bin/env python
# coding: utf-8
"""
Robust Shopify to Google Sheets Order Sync
This script syncs orders from Shopify to Google Sheets with enhanced reliability features:
- Persistent order tracking to prevent missed orders
- Comprehensive duplicate detection
- Error recovery and retry mechanisms
- Detailed logging and audit trails
- Order validation and reconciliation
"""

import gspread
import requests
import json
import pandas as pd
import os
import logging
import time
import sqlite3
import hashlib
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
# from gspread_pandas import Spread  # Not using this due to auth issues
from google.oauth2 import service_account
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shopify_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ShopifyOrderSync:
    """Main class for syncing Shopify orders to Google Sheets"""
    
    def __init__(self, config_file: str = 'config.json'):
        """Initialize with configuration from file or environment"""
        self.config = self._load_config(config_file)
        self.db_path = self.config.get('db_path', 'shopify_sync.db')
        self.session = self._create_session()
        self.google_client = None
        self._init_database()
        
    def _load_config(self, config_file: str) -> dict:
        """Load configuration from file or environment variables"""
        config = {
            'shopify_store_name': os.environ.get('SHOPIFY_STORE_NAME', 'brown-sugar-pre-orders'),
            'shopify_access_token': os.environ.get('SHOPIFY_ACCESS_TOKEN', ''),
            'google_service_account_file': os.environ.get('GOOGLE_SERVICE_ACCOUNT_FILE', ''),
            'template_spreadsheet': os.environ.get('TEMPLATE_SPREADSHEET', 'Customer Orders'),
            'target_spreadsheet': os.environ.get('TARGET_SPREADSHEET', 'Customer Orders-3-1'),
            'lookback_days': int(os.environ.get('LOOKBACK_DAYS', '30')),
            'batch_size': int(os.environ.get('BATCH_SIZE', '250')),
            'max_retries': int(os.environ.get('MAX_RETRIES', '3')),
            'retry_delay': int(os.environ.get('RETRY_DELAY', '5'))
        }
        
        # Try to load from config file if exists
        if Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
                    config.update(file_config)
                    logger.info(f"Loaded configuration from {config_file}")
            except Exception as e:
                logger.warning(f"Could not load config file: {e}")
        
        # Validate required fields
        required_fields = ['shopify_access_token', 'google_service_account_file']
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {missing_fields}")
            
        return config
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config['max_retries'],
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Changed from method_whitelist
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
        
    def _init_database(self):
        """Initialize SQLite database for tracking synced orders"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables for tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                orders_processed INTEGER,
                orders_new INTEGER,
                orders_updated INTEGER,
                status TEXT,
                error_message TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_tracking (
                shopify_order_id TEXT PRIMARY KEY,
                order_number INTEGER UNIQUE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                order_hash TEXT,
                line_items_hash TEXT,
                sheets_row_number INTEGER,
                sync_status TEXT DEFAULT 'synced'
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                order_id TEXT,
                error_type TEXT,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                resolved BOOLEAN DEFAULT FALSE
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def setup_google_auth(self) -> gspread.Client:
        """Set up Google Sheets API credentials"""
        try:
            SCOPES = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            credentials = service_account.Credentials.from_service_account_file(
                self.config['google_service_account_file'], 
                scopes=SCOPES
            )
            
            client = gspread.authorize(credentials)
            logger.info("Successfully authenticated with Google")
            
            self.google_client = client
            
            return client
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise
    
    def _calculate_order_hash(self, order: dict) -> str:
        """Calculate hash of order data to detect changes"""
        # Select key fields that indicate order changes
        key_fields = [
            'total_price', 'subtotal_price', 'total_tax',
            'customer', 'billing_address', 'shipping_address',
            'fulfillment_status', 'financial_status'
        ]
        
        hash_data = {field: order.get(field, '') for field in key_fields}
        hash_string = json.dumps(hash_data, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
    
    def _calculate_line_items_hash(self, line_items: List[dict]) -> str:
        """Calculate hash of line items to detect changes"""
        items_data = []
        for item in line_items:
            item_data = {
                'product_id': item.get('product_id'),
                'variant_id': item.get('variant_id'),
                'quantity': item.get('quantity'),
                'price': item.get('price'),
                'properties': item.get('properties', [])
            }
            items_data.append(item_data)
        
        hash_string = json.dumps(items_data, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
    
    def fetch_shopify_orders(self, since_date: Optional[datetime] = None) -> List[dict]:
        """Fetch orders from Shopify with comprehensive error handling"""
        orders = []
        
        if not since_date:
            # Get the last successful sync date from database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT MAX(created_at) FROM order_tracking 
                WHERE sync_status = 'synced'
            ''')
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                since_date = datetime.fromisoformat(result[0])
                # Add a small buffer to ensure we don't miss orders
                since_date -= timedelta(hours=1)
            else:
                since_date = datetime.now() - timedelta(days=self.config['lookback_days'])
        
        formatted_date = since_date.strftime('%Y-%m-%dT%H:%M:%S-00:00')
        
        base_url = f"https://{self.config['shopify_store_name']}.myshopify.com"
        url = f"{base_url}/admin/api/2023-04/orders.json"
        
        headers = {
            "X-Shopify-Access-Token": self.config['shopify_access_token'],
        }
        
        params = {
            'limit': self.config['batch_size'],
            'status': 'any',
            'created_at_min': formatted_date
        }
        
        logger.info(f"Fetching orders from Shopify API starting at {formatted_date}")
        
        page_count = 0
        while url:
            try:
                response = self.session.get(url, headers=headers, params=params if page_count == 0 else None, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                orders_batch = data.get('orders', [])
                orders.extend(orders_batch)
                
                page_count += 1
                logger.info(f"Fetched page {page_count}: {len(orders_batch)} orders (total: {len(orders)})")
                
                # Extract next page URL from Link header
                link_header = response.headers.get('Link', None)
                next_link = None
                if link_header:
                    links = link_header.split(',')
                    for link in links:
                        if 'rel="next"' in link:
                            next_link = link.split(';')[0].strip('<> ')
                            break
                url = next_link
                
                # Clear params after first request (they're in the URL now)
                params = None
                
                # Rate limiting protection
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching orders from Shopify: {e}")
                self._log_sync_error(None, 'api_fetch', str(e))
                
                # If we've fetched some orders, return what we have
                if orders:
                    logger.warning(f"Returning partial results: {len(orders)} orders")
                    break
                else:
                    raise
                    
        logger.info(f"Total orders fetched: {len(orders)}")
        return orders
    
    def _log_sync_error(self, order_id: Optional[str], error_type: str, error_message: str):
        """Log sync errors to database for tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sync_errors (order_id, error_type, error_message)
            VALUES (?, ?, ?)
        ''', (order_id, error_type, error_message))
        conn.commit()
        conn.close()
    
    def get_existing_order_data(self) -> Dict[str, dict]:
        """Get existing order data from database for comparison"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT shopify_order_id, order_number, order_hash, line_items_hash, sync_timestamp
            FROM order_tracking
            WHERE sync_status = 'synced'
        ''')
        
        existing_orders = {}
        for row in cursor.fetchall():
            existing_orders[row[0]] = {
                'order_number': row[1],
                'order_hash': row[2],
                'line_items_hash': row[3],
                'sync_timestamp': row[4]
            }
        
        conn.close()
        return existing_orders
    
    def identify_new_and_updated_orders(self, shopify_orders: List[dict]) -> Tuple[List[dict], List[dict]]:
        """Identify which orders are new and which have been updated"""
        existing_orders = self.get_existing_order_data()
        new_orders = []
        updated_orders = []
        
        for order in shopify_orders:
            order_id = str(order['id'])
            order_hash = self._calculate_order_hash(order)
            line_items_hash = self._calculate_line_items_hash(order.get('line_items', []))
            
            if order_id not in existing_orders:
                new_orders.append(order)
            else:
                existing = existing_orders[order_id]
                if (existing['order_hash'] != order_hash or 
                    existing['line_items_hash'] != line_items_hash):
                    updated_orders.append(order)
        
        logger.info(f"Identified {len(new_orders)} new orders and {len(updated_orders)} updated orders")
        return new_orders, updated_orders
    
    def get_max_web_orderid_from_sheets(self) -> Tuple[int, str]:
        """Get the maximum WebOrderID from the target spreadsheet"""
        try:
            target_sheet = self.google_client.open(self.config['target_spreadsheet'])
            worksheet = target_sheet.worksheet("Customer Orders")
            
            all_data = worksheet.get_all_values()
            if not all_data or len(all_data) <= 1:
                logger.warning("Target spreadsheet has no order data")
                return 0, "Order Type"
            
            headers = all_data[0]
            data = all_data[1:]
            current_orders_df = pd.DataFrame(data, columns=headers)
            
            # Find columns
            web_order_id_col = None
            order_type_col = None
            
            for col in current_orders_df.columns:
                if 'WebOrderID' in col:
                    web_order_id_col = col
                if 'Order Type' in col:
                    order_type_col = col
            
            if not order_type_col:
                order_type_col = "Order Type"
            
            if web_order_id_col and order_type_col:
                web_orders = current_orders_df[current_orders_df[order_type_col].str.contains('Web', na=False)]
                
                if not web_orders.empty:
                    numeric_ids = web_orders[web_order_id_col].str.replace('[^0-9]', '', regex=True)
                    numeric_ids = pd.to_numeric(numeric_ids, errors='coerce')
                    
                    if not numeric_ids.isna().all():
                        max_id = int(numeric_ids.max())
                        logger.info(f"Found maximum WebOrderID in sheets: {max_id}")
                        return max_id, order_type_col
            
            return 0, order_type_col
            
        except Exception as e:
            logger.error(f"Error getting max WebOrderID from sheets: {e}")
            return 0, "Order Type"
    
    def validate_order_completeness(self, orders: List[dict]) -> List[str]:
        """Validate that we have all expected orders"""
        issues = []
        
        if not orders:
            return issues
        
        # Sort orders by order_number
        sorted_orders = sorted(orders, key=lambda x: x.get('order_number', 0))
        
        # Check for gaps in order numbers
        order_numbers = [o.get('order_number') for o in sorted_orders if o.get('order_number')]
        
        if order_numbers:
            min_num = min(order_numbers)
            max_num = max(order_numbers)
            expected_count = max_num - min_num + 1
            
            if len(order_numbers) < expected_count:
                missing = set(range(min_num, max_num + 1)) - set(order_numbers)
                if missing:
                    issues.append(f"Missing order numbers: {sorted(missing)}")
        
        # Check for duplicate order numbers
        duplicates = [num for num in order_numbers if order_numbers.count(num) > 1]
        if duplicates:
            issues.append(f"Duplicate order numbers found: {set(duplicates)}")
        
        return issues
    
    def transform_orders_for_sheets(self, orders: List[dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Transform Shopify orders into format for Google Sheets"""
        if not orders:
            return pd.DataFrame(), pd.DataFrame()
        
        rows = []
        
        for order in orders:
            order_number = order.get("order_number", "")
            weborderid = order.get("order_number", "")
            order_date = ""
            
            if "created_at" in order:
                order_date = order["created_at"].replace("T", " ").replace("Z", "").split(".")[0]
                
            customer = order.get("customer", {})
            customer_name = customer.get("first_name", "")
            customer_last_name = customer.get("last_name", "")
            customer_email = order.get("contact_email", "")
            customer_phone = order.get("phone", "")
            line_items = order.get("line_items", [])
            note_attributes = self._get_note_attributes(order.get("note_attributes", []))
            fulfillment_status = order.get("fulfillment_status", "")
            # Extract order type from tags
            order_tags = order.get("tags", "")
            order_type = self._get_order_type_from_tags(order_tags)
            
            if line_items:
                index = 0
                for line_item in line_items:
                    index += 1
                    title = line_item.get("title", "")
                    variant_title = line_item.get("variant_title", "")
                    cake_writing = ""
                    writing_color = ""
                    special_pickup_date = ""
                    special_pickup_time = ""
                    quantity = line_item.get("quantity", 0)
                    price = line_item.get("price", 0)
                    subtotal = quantity * float(price)
                    taxes = order.get("total_tax", 0)
                    total = order.get("total_price", 0)
                    line_number = index
                    
                    # Extract properties
                    if "properties" in line_item:
                        for property in line_item["properties"]:
                            if property.get("name") == "Cake Writing":
                                cake_writing = property.get("value", "")
                            elif property.get("name") == "Writing-Color":
                                writing_color = property.get("value", "")
                            elif property.get("name") == "Special-Pickup-Date":
                                special_pickup_date = property.get("value", "")
                            elif property.get("name") == "Special-Pickup-Time":
                                special_pickup_time = property.get("value", "")
                    
                    # Determine the pickup/shipping date
                    pickup_date = note_attributes.get("pickupDate", "")
                    if not pickup_date and note_attributes.get("shippingDate"):
                        pickup_date = note_attributes.get("shippingDate", "")
                    
                    row_data = [
                        order_number, order_date, weborderid, customer_name, customer_last_name,
                        customer_email, customer_phone, line_number, title, variant_title,
                        cake_writing, writing_color, quantity, price, subtotal, taxes, total,
                        fulfillment_status, pickup_date,
                        note_attributes.get("pickupTime", ""), note_attributes.get("checkoutMethod", ""),
                        special_pickup_date, special_pickup_time, order_type
                    ]
                    rows.append(row_data)
        
        # Create DataFrame
        columns = [
            "Order Number", "Order Date", "WebOrderID", "First Name", "Last Name",
            "Customer Email", "Customer Phone", "Line Number", "Line Item", "Variant Title",
            "Cake Writing", "Writing Color", "Line Item Qty", "Line Item Price", "Order Subtotal",
            "Order Taxes", "Order Total", "Fulfillment Status", "Pickup Date", "Pickup Time",
            "Pickup Method", "Special Pickup Date", "Special Pickup Time", "Order Type"
        ]
        
        order_data_df = pd.DataFrame(rows, columns=columns)
        
        # Process orders and order lines
        return self._process_order_dataframes(order_data_df)
    
    def _get_note_attributes(self, attributes: List[dict]) -> dict:
        """Extract note attributes from order"""
        note_attributes = {}
        for attribute in attributes:
            if attribute.get("name") == "Pickup-Date":
                note_attributes["pickupDate"] = attribute.get("value", "")
            elif attribute.get("name") == "Pickup-Time":
                note_attributes["pickupTime"] = attribute.get("value", "")
            elif attribute.get("name") == "Checkout-Method":
                note_attributes["checkoutMethod"] = attribute.get("value", "")
            elif attribute.get("name") == "Shipping Date":
                note_attributes["shippingDate"] = attribute.get("value", "")
            elif attribute.get("name") == "Shipping-Date":
                note_attributes["shippingDate"] = attribute.get("value", "")
        return note_attributes
    
    def _get_order_type_from_tags(self, tags: str) -> str:
        """Extract order type from Shopify tags"""
        if not tags:
            return "Web"  # Default value
        
        # Convert tags to lowercase for case-insensitive matching
        tags_lower = tags.lower()
        
        # Priority order: check for specific tags
        if "pickup order" in tags_lower:
            return "Pickup Order"
        elif "nationwide shipping" in tags_lower:
            return "Nationwide Shipping"
        elif "local delivery order" in tags_lower:
            return "Local Delivery Order"
        else:
            # If none of the expected tags found, return "Web" as default
            return "Web"
    
    def _process_order_dataframes(self, order_data_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process raw order data into final format for sheets"""
        # Rename columns
        order_data_df = order_data_df.rename(columns={
            'Order Number': 'OrderID',
            'First Name': 'Customer First Name',
            'Last Name': 'Customer Last Name',
            'Pickup Date': 'Due Pickup Date',
            'Pickup Time': 'Due Pickup Time',
            'Customer Email': 'Email',
            'Customer Phone': 'PhoneNumber',
            'Order Total': 'Total',
            'Line Item': 'Type',
            'Variant Title': 'Size',
            'Line Item Price': 'Unit Price',
            'Line Item Qty': 'CakeQty',
            'Line Number': 'LineItem',
            'Cake Writing': 'Writing Notes',
            'Writing Color': 'Color'
        })
        
        # Create base order DataFrame with required columns
        base_order_df = order_data_df[[
            'Order Date', 'OrderID', 'WebOrderID', 'Due Pickup Time', 'Due Pickup Date',
            'Customer First Name', 'Customer Last Name', 'Email', 'PhoneNumber', 'Total', 'Order Type'
        ]].drop_duplicates(subset=['OrderID'])
        
        # Format dates
        base_order_df['Order Date'] = pd.to_datetime(base_order_df['Order Date'], errors='coerce', utc=True)
        base_order_df['Order Date'] = base_order_df['Order Date'].dt.strftime('%m-%d-%Y')
        
        # Format Due Pickup Date to MM-DD-YYYY
        if 'Due Pickup Date' in base_order_df.columns:
            pickup_dates = base_order_df['Due Pickup Date'].copy()
            pickup_dates_dt = pd.to_datetime(pickup_dates, errors='coerce')
            valid_dates_mask = pickup_dates_dt.notna()
            
            if any(valid_dates_mask):
                base_order_df.loc[valid_dates_mask, 'Due Pickup Date'] = pickup_dates_dt.loc[valid_dates_mask].dt.strftime('%m-%d-%Y')
        
        # Create order_df with all columns in the correct order to match Google Sheet
        order_df = pd.DataFrame()
        order_df['Status'] = 'New'
        order_df['Order Date'] = base_order_df['Order Date']
        order_df['OrderID'] = base_order_df['OrderID']
        order_df['WebOrderID'] = base_order_df['WebOrderID']
        order_df['Special'] = ''
        order_df['TextNumber'] = ''
        order_df['Pickup Timestamp'] = ''
        order_df['Due Date'] = ''
        order_df['Customer Name'] = ''
        order_df['Due Pickup Date'] = base_order_df['Due Pickup Date']
        order_df['Due Pickup Time'] = base_order_df['Due Pickup Time']
        order_df['Customer First Name'] = base_order_df['Customer First Name']
        order_df['Customer Last Name'] = base_order_df['Customer Last Name']
        order_df['Address'] = ''
        order_df['Email'] = base_order_df['Email']
        order_df['City'] = ''
        order_df['Country'] = ''
        order_df['PhoneNumber'] = base_order_df['PhoneNumber']
        order_df['Taxes'] = ''
        order_df['TextOk'] = ''
        order_df['EmailOk'] = ''
        order_df['Total'] = base_order_df['Total']
        order_df['Order Type'] = base_order_df['Order Type']
        order_df['Updated'] = ''
        order_df['LineItems'] = ''
        order_df['Order Count'] = ''
        order_df['Order Notes'] = ''
        order_df['Location'] = ''
        order_df['Order Image'] = ''
        order_df['OrderLineItemHeader'] = ''
        order_df['TopofFormHeader'] = ''
        order_df['FormDescriptionHeader'] = ''
        order_df['DueDateRulesHeader'] = ''
        order_df['Printed'] = ''
        order_df['ChangeTimeStamp'] = ''
        order_df['Order Change Notes'] = ''
        order_df['Order Taker'] = 'Web'
        order_df['Customer Ready Text Sent'] = ''
        order_df['Late Pickup Reminder Sent'] = ''
        order_df['PickupReminderSent'] = ''
        
        # Create order lines DataFrame
        order_lines_df = order_data_df[[
            'OrderID', 'LineItem', 'Type', 'Size', 'Unit Price', 'CakeQty', 'Color', 'Writing Notes'
        ]]
        
        # Add additional columns
        order_lines_df['Category'] = 'Cake'
        order_lines_df['Product Description'] = None
        order_lines_df['Line Item Notes'] = None
        order_lines_df['Flavor'] = None
        order_lines_df['Addons'] = None
        order_lines_df['Item Tax (Calculated)'] = None
        
        # Data cleanup
        replacements = {
            '2 Layer': '2L', '2 Layers': '2L',
            '4 Layer': '4L', '4 Layers': '4L',
            'OBAMA': 'Obama'
        }
        for old, new in replacements.items():
            order_lines_df['Size'] = order_lines_df['Size'].replace(old, new)
        
        # Add WEB prefix to OrderID if needed
        if not order_df['OrderID'].astype(str).str.startswith('WEB').all():
            order_df['OrderID'] = 'WEB' + order_df['OrderID'].astype(str)
        if not order_lines_df['OrderID'].astype(str).str.startswith('WEB').all():
            order_lines_df['OrderID'] = 'WEB' + order_lines_df['OrderID'].astype(str)
        
        # Convert line item numbers to letters
        line_item_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G'}
        order_lines_df['LineItem'] = order_lines_df['LineItem'].apply(
            lambda x: line_item_map.get(x, '*') if isinstance(x, (int, float)) else x
        )
        
        # Update Product Description
        order_lines_df['Product Description'] = (
            order_lines_df['Type'].astype(str) + ' ' + order_lines_df['Size'].astype(str)
        ).str.title()
        
        return order_df, order_lines_df
    
    def append_to_sheets(self, order_df: pd.DataFrame, order_lines_df: pd.DataFrame, test_mode: bool = False):
        """Append orders to Google Sheets with error handling"""
        
        # If in test mode, write to TEST worksheets and CSV files
        if test_mode:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Create CSV files for backup/reference
            if not order_df.empty:
                orders_file = f'test_output/orders_to_append_{timestamp}.csv'
                Path('test_output').mkdir(exist_ok=True)
                order_df.to_csv(orders_file, index=False)
                logger.info(f"TEST MODE: Saved orders to CSV: {orders_file}")
            
            if not order_lines_df.empty:
                lines_file = f'test_output/order_lines_to_append_{timestamp}.csv'
                Path('test_output').mkdir(exist_ok=True)
                order_lines_df.to_csv(lines_file, index=False)
                logger.info(f"TEST MODE: Saved order lines to CSV: {lines_file}")
            
            # Write to TEST worksheets in Google Sheets
            try:
                target_sheet = self.google_client.open(self.config['target_spreadsheet'])
                
                # Create or get TEST Customer Orders worksheet
                try:
                    test_orders_worksheet = target_sheet.worksheet("TEST Customer Orders")
                    logger.info("Found existing 'TEST Customer Orders' worksheet")
                except:
                    test_orders_worksheet = target_sheet.add_worksheet(title="TEST Customer Orders", rows=1000, cols=30)
                    logger.info("Created new 'TEST Customer Orders' worksheet")
                
                # Create or get TEST - Bakery Products Ordered worksheet  
                try:
                    test_lines_worksheet = target_sheet.worksheet("TEST - Bakery Products Ordered")
                    logger.info("Found existing 'TEST - Bakery Products Ordered' worksheet")
                except:
                    test_lines_worksheet = target_sheet.add_worksheet(title="TEST - Bakery Products Ordered", rows=1000, cols=30)
                    logger.info("Created new 'TEST - Bakery Products Ordered' worksheet")
                
                if not order_df.empty:
                    # Clear existing test data and write headers + new data
                    test_orders_worksheet.clear()
                    
                    # Prepare data with headers
                    order_df_filled = order_df.fillna('')
                    headers = [order_df_filled.columns.tolist()]
                    data_rows = order_df_filled.values.tolist()
                    all_data = headers + data_rows
                    
                    # Write to sheet
                    test_orders_worksheet.update('A1', all_data, value_input_option='USER_ENTERED')
                    logger.info(f"TEST MODE: Wrote {len(order_df)} orders to 'TEST Customer Orders' worksheet")
                
                if not order_lines_df.empty:
                    # Clear existing test data and write headers + new data
                    test_lines_worksheet.clear()
                    
                    # Prepare data with headers
                    lines_df_filled = order_lines_df.fillna('')
                    headers = [lines_df_filled.columns.tolist()]
                    data_rows = lines_df_filled.values.tolist()
                    all_data = headers + data_rows
                    
                    # Write to sheet
                    test_lines_worksheet.update('A1', all_data, value_input_option='USER_ENTERED')
                    logger.info(f"TEST MODE: Wrote {len(order_lines_df)} order lines to 'TEST Bakery Products Ordered' worksheet")
                
            except Exception as e:
                logger.error(f"TEST MODE: Error writing to Google Sheets: {e}")
                logger.info("TEST MODE: Data is still available in CSV files")
            
            # Also create a summary log
            summary_file = f'test_output/sync_summary_{timestamp}.txt'
            with open(summary_file, 'w') as f:
                f.write(f"Sync Test Summary - {datetime.now()}\n")
                f.write(f"{'='*50}\n\n")
                f.write(f"Orders written to TEST worksheets: {len(order_df)}\n")
                f.write(f"Order lines written to TEST worksheets: {len(order_lines_df)}\n\n")
                f.write("TEST worksheets created:\n")
                f.write("  - TEST Customer Orders\n")
                f.write("  - TEST - Bakery Products Ordered\n\n")
                
                if not order_df.empty:
                    f.write("Order Numbers:\n")
                    for order_id in order_df['OrderID'].tolist()[:20]:  # Show first 20
                        f.write(f"  - {order_id}\n")
                    if len(order_df) > 20:
                        f.write(f"  ... and {len(order_df) - 20} more orders\n")
                    f.write(f"\nTotal value: ${order_df['Total'].astype(float).sum():.2f}\n")
                
            logger.info(f"TEST MODE: Summary written to {summary_file}")
            return
        
        # Normal mode - actually append to sheets
        try:
            target_sheet = self.google_client.open(self.config['target_spreadsheet'])
            order_worksheet = target_sheet.worksheet("Customer Orders")
            order_lines_worksheet = target_sheet.worksheet("Bakery Products Ordered ")
            
            if not order_df.empty:
                # Prepare data for appending
                order_df = order_df.fillna('')
                order_values = order_df.values.tolist()
                
                # Append orders
                order_worksheet.append_rows(order_values, table_range='A1', value_input_option='USER_ENTERED')
                logger.info(f"Appended {len(order_values)} new orders to sheets")
            
            if not order_lines_df.empty:
                # Prepare order lines
                order_lines_df = order_lines_df.fillna('')
                order_lines_values = order_lines_df.values.tolist()
                
                # Append order lines
                order_lines_worksheet.append_rows(order_lines_values, table_range='A1', value_input_option='USER_ENTERED')
                logger.info(f"Appended {len(order_lines_values)} order lines to sheets")
                
        except Exception as e:
            logger.error(f"Error appending to sheets: {e}")
            raise
    
    def update_tracking_database(self, orders: List[dict], status: str = 'synced'):
        """Update tracking database with synced orders"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for order in orders:
            order_id = str(order['id'])
            order_number = order.get('order_number')
            created_at = order.get('created_at')
            updated_at = order.get('updated_at')
            order_hash = self._calculate_order_hash(order)
            line_items_hash = self._calculate_line_items_hash(order.get('line_items', []))
            
            cursor.execute('''
                INSERT OR REPLACE INTO order_tracking 
                (shopify_order_id, order_number, created_at, updated_at, order_hash, line_items_hash, sync_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (order_id, order_number, created_at, updated_at, order_hash, line_items_hash, status))
        
        conn.commit()
        conn.close()
        logger.info(f"Updated tracking database for {len(orders)} orders")
    
    def log_sync_result(self, orders_processed: int, orders_new: int, orders_updated: int, status: str, error_message: str = None):
        """Log sync results to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sync_history (orders_processed, orders_new, orders_updated, status, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (orders_processed, orders_new, orders_updated, status, error_message))
        conn.commit()
        conn.close()
    
    def reconcile_orders(self):
        """Reconcile orders between Shopify and tracking database"""
        # Get all order numbers from tracking database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT order_number FROM order_tracking WHERE sync_status = "synced" ORDER BY order_number')
        tracked_numbers = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        
        if not tracked_numbers:
            logger.info("No orders in tracking database to reconcile")
            return
        
        # Check for gaps
        min_num = min(tracked_numbers)
        max_num = max(tracked_numbers)
        expected_numbers = set(range(min_num, max_num + 1))
        tracked_set = set(tracked_numbers)
        
        missing_numbers = expected_numbers - tracked_set
        
        if missing_numbers:
            logger.warning(f"Found {len(missing_numbers)} missing order numbers: {sorted(missing_numbers)[:10]}...")
            
            # Attempt to fetch missing orders
            for order_num in missing_numbers:
                try:
                    # Fetch specific order from Shopify
                    url = f"https://{self.config['shopify_store_name']}.myshopify.com/admin/api/2023-04/orders.json"
                    params = {'name': f"#{order_num}"}
                    headers = {"X-Shopify-Access-Token": self.config['shopify_access_token']}
                    
                    response = self.session.get(url, headers=headers, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('orders'):
                            logger.info(f"Found missing order #{order_num}")
                            # Process this order
                            self._process_single_order(data['orders'][0])
                except Exception as e:
                    logger.error(f"Error fetching missing order #{order_num}: {e}")
        else:
            logger.info("No missing orders found during reconciliation")
    
    def _process_single_order(self, order: dict):
        """Process a single order"""
        try:
            order_df, order_lines_df = self.transform_orders_for_sheets([order])
            if not order_df.empty:
                self.append_to_sheets(order_df, order_lines_df)
                self.update_tracking_database([order])
                logger.info(f"Successfully processed order #{order.get('order_number')}")
        except Exception as e:
            logger.error(f"Error processing order #{order.get('order_number')}: {e}")
            self._log_sync_error(str(order.get('id')), 'processing', str(e))
    
    def run_sync(self, test_mode: bool = False):
        """Main sync method"""
        sync_start = datetime.now()
        mode_str = "TEST MODE" if test_mode else "PRODUCTION"
        logger.info(f"=== Starting Shopify to Google Sheets sync ({mode_str}) ===")
        
        try:
            # Setup Google authentication
            self.setup_google_auth()
            
            # Get max order ID from sheets
            max_web_orderid, order_type_col = self.get_max_web_orderid_from_sheets()
            
            # Fetch orders from Shopify
            all_orders = self.fetch_shopify_orders()
            
            if not all_orders:
                logger.info("No orders to process")
                self.log_sync_result(0, 0, 0, 'success', 'No orders found')
                return
            
            # Validate order completeness
            validation_issues = self.validate_order_completeness(all_orders)
            if validation_issues:
                logger.warning("Validation issues found:")
                for issue in validation_issues:
                    logger.warning(f"  - {issue}")
            
            # Identify new and updated orders
            new_orders, updated_orders = self.identify_new_and_updated_orders(all_orders)
            
            # Filter orders by max WebOrderID from sheets
            if max_web_orderid > 0:
                new_orders = [o for o in new_orders if o.get('order_number', 0) > max_web_orderid]
                logger.info(f"Filtered to {len(new_orders)} orders newer than WebOrderID {max_web_orderid}")
            
            # Process new orders
            if new_orders:
                order_df, order_lines_df = self.transform_orders_for_sheets(new_orders)
                
                # Append to sheets (or test output)
                self.append_to_sheets(order_df, order_lines_df, test_mode=test_mode)
                
                # Update tracking database only if not in test mode
                if not test_mode:
                    self.update_tracking_database(new_orders)
                else:
                    logger.info(f"TEST MODE: Would update tracking database with {len(new_orders)} orders")
            
            # Log results
            total_processed = len(new_orders) + len(updated_orders)
            status = 'test_success' if test_mode else 'success'
            self.log_sync_result(total_processed, len(new_orders), len(updated_orders), status)
            
            # Perform reconciliation
            if not test_mode:
                self.reconcile_orders()
            
            sync_duration = (datetime.now() - sync_start).total_seconds()
            logger.info(f"=== Sync completed successfully in {sync_duration:.1f} seconds ===")
            
            if test_mode:
                logger.info("TEST MODE: Check the test_output directory for results")
                logger.info("TEST MODE: Review the CSV files and summary before running in production mode")
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            self.log_sync_result(0, 0, 0, 'failed', str(e))
            raise
    
    def get_sync_status(self) -> dict:
        """Get current sync status and statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get last sync info
        cursor.execute('''
            SELECT sync_timestamp, orders_processed, orders_new, status, error_message
            FROM sync_history
            ORDER BY sync_timestamp DESC
            LIMIT 1
        ''')
        last_sync = cursor.fetchone()
        
        # Get total orders tracked
        cursor.execute('SELECT COUNT(*) FROM order_tracking WHERE sync_status = "synced"')
        total_tracked = cursor.fetchone()[0]
        
        # Get pending errors
        cursor.execute('SELECT COUNT(*) FROM sync_errors WHERE resolved = FALSE')
        pending_errors = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'last_sync': last_sync,
            'total_tracked': total_tracked,
            'pending_errors': pending_errors
        }


def main():
    """Main entry point for the sync script"""
    import sys
    
    # Check for test mode flag
    test_mode = '--test' in sys.argv or '-t' in sys.argv
    
    # Create sync instance
    sync = ShopifyOrderSync()
    
    # Run sync
    sync.run_sync(test_mode=test_mode)
    
    # Display status
    status = sync.get_sync_status()
    logger.info(f"Sync Status: {status}")


if __name__ == "__main__":
    main()