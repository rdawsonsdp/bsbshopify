# Shopify to Google Sheets Order Sync - Robust Version

This improved script provides reliable synchronization of orders from Shopify to Google Sheets with comprehensive tracking, error handling, and duplicate prevention.

## Key Improvements Over Original Script

### 1. **Persistent Order Tracking**
- SQLite database tracks all synced orders
- Maintains hash of order data to detect changes
- Prevents missed orders even if script fails mid-sync

### 2. **Duplicate Prevention**
- Database-level tracking ensures no duplicate orders
- Order hash comparison detects updated orders
- Line item level duplicate checking

### 3. **Error Recovery**
- Comprehensive error logging and retry mechanisms
- HTTP session with automatic retry for API failures
- Failed orders tracked for manual review

### 4. **Order Validation**
- Checks for gaps in order numbers
- Reconciliation process to find missing orders
- Automatic fetching of missing orders

### 5. **Security Improvements**
- Credentials stored in environment variables or config file
- No hardcoded API tokens in code

### 6. **Monitoring & Management**
- Detailed logging to file and console
- Sync manager utility for status checks
- Export capabilities for audit trails

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Set up configuration:
   - Copy `config_template.json` to `config.json` and fill in your values
   - OR copy `.env.template` to `.env` and set environment variables

3. Ensure you have:
   - Shopify API access token with order read permissions
   - Google Service Account JSON key file
   - Access to target Google Sheets

## Configuration

### Using config.json:
```json
{
  "shopify_store_name": "your-store",
  "shopify_access_token": "shpat_xxxxx",
  "google_service_account_file": "/path/to/key.json",
  "target_spreadsheet": "Customer Orders-3-1",
  "lookback_days": 30
}
```

### Using Environment Variables:
```bash
export SHOPIFY_STORE_NAME="your-store"
export SHOPIFY_ACCESS_TOKEN="shpat_xxxxx"
export GOOGLE_SERVICE_ACCOUNT_FILE="/path/to/key.json"
export TARGET_SPREADSHEET="Customer Orders-3-1"
```

## Usage

### Test Mode (Recommended First Run)
Run in test mode to review what would be synced without writing to Google Sheets:
```bash
python run_test_sync.py
```
Or directly:
```bash
python shopify_sheets_sync.py --test
```

This will:
- Fetch orders from Shopify
- Read existing data from Google Sheets
- Process orders but NOT write anything
- Save results to `test_output/` directory for review

### Basic Sync (Production)
After reviewing test results:
```bash
python shopify_sheets_sync.py
```

### Using Sync Manager

Check sync status:
```bash
python sync_manager.py status
```

Run manual sync:
```bash
python sync_manager.py sync
```

Check for missing orders:
```bash
python sync_manager.py missing
```

View recent orders:
```bash
python sync_manager.py orders --limit 20
```

View sync errors:
```bash
python sync_manager.py errors
```

Validate connections:
```bash
python sync_manager.py validate
```

Generate weekly report:
```bash
python sync_manager.py report --days 7
```

Export tracking data:
```bash
python sync_manager.py export --output orders_backup.csv
```

## Scheduling

### Using cron (Linux/Mac)
Add to crontab to run every hour:
```bash
0 * * * * cd /path/to/shopify && /usr/bin/python shopify_sheets_sync.py >> sync.log 2>&1
```

### Using Task Scheduler (Windows)
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.board., hourly)
4. Set action to run: `python C:\path\to\shopify_sheets_sync.py`

## Database Schema

The script creates an SQLite database (`shopify_sync.db`) with:

- **order_tracking**: Tracks all synced orders with hashes
- **sync_history**: Log of all sync operations
- **sync_errors**: Detailed error tracking for troubleshooting

## Monitoring

### Log Files
- `shopify_sync.log`: Detailed sync operations log
- Check for ERROR or WARNING messages

### Health Checks
```bash
# Check if sync is running properly
python sync_manager.py status

# Check for any missing orders
python sync_manager.py missing

# View any errors
python sync_manager.py errors
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify Google Service Account file path
   - Check Shopify API token is valid
   - Ensure spreadsheet is shared with service account email

2. **Missing Orders**
   - Run reconciliation: `python sync_manager.py missing`
   - Check Shopify API rate limits
   - Verify date range in lookback_days

3. **Duplicate Orders**
   - Database prevents duplicates automatically
   - Check order_tracking table for issues
   - Reset specific order: `python sync_manager.py reset ORDER_NUMBER`

### Recovery Procedures

If sync fails consistently:
1. Check connections: `python sync_manager.py validate`
2. Review errors: `python sync_manager.py errors`
3. Export current data: `python sync_manager.py export`
4. Fix issues and retry: `python sync_manager.py sync --force`

## Security Notes

- Never commit credentials to version control
- Use environment variables or secure config files
- Regularly rotate API tokens
- Restrict Google Service Account permissions
- Keep audit logs of all sync operations

## Maintenance

### Regular Tasks
- Monitor log files for errors
- Check for missing orders weekly
- Export tracking data monthly for backups
- Review and resolve sync errors

### Database Maintenance
The SQLite database is self-contained and requires minimal maintenance. To backup:
```bash
cp shopify_sync.db shopify_sync_backup_$(date +%Y%m%d).db
```

## Support

For issues:
1. Check the log file for detailed error messages
2. Run validation: `python sync_manager.py validate`
3. Generate a report: `python sync_manager.py report`
4. Export tracking data for analysis