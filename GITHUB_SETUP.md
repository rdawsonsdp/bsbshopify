# GitHub Actions Setup Guide

This document explains how to set up GitHub Secrets for the automated Shopify sync workflow.

## Required GitHub Secrets

Navigate to your repository: **Settings > Secrets and variables > Actions > New repository secret**

### 1. `SHOPIFY_STORE_NAME`
- **Value:** `brown-sugar-bakery-chicago` (without .myshopify.com)

### 2. `SHOPIFY_ACCESS_TOKEN`
- **Value:** Your Shopify API access token (starts with `shpat_`)
- **Location:** Copy from your local `config.json` file

### 3. `TARGET_SPREADSHEET`
- **Value:** `Customer Orders-3-1`

### 4. `GOOGLE_SERVICE_ACCOUNT_JSON`
- **Value:** Complete contents of your Google service account JSON file
- **Current file:** `long-canto-360620-dcaf3c6f9ef6.json`

To get the JSON content:
```bash
cat long-canto-360620-dcaf3c6f9ef6.json
```

Copy the entire JSON content (including the outer braces `{ }`) and paste as the secret value.

## Workflow Schedule

The workflow is configured to run:
- **Every 2 hours** automatically
- **Manual trigger** available via "Actions" tab

## Monitoring

After setup, you can monitor the sync:
1. Go to **Actions** tab in your GitHub repository
2. Click on "Shopify to Google Sheets Sync" workflow
3. View logs and status of each run

## Testing the Setup

1. After adding all secrets, go to **Actions** tab
2. Click "Shopify to Google Sheets Sync"
3. Click "Run workflow" button to test manually
4. Monitor the run to ensure it completes successfully

## Troubleshooting

If the workflow fails:
1. Check the **Actions** tab for error logs
2. Verify all secrets are correctly set
3. Check that Google service account has access to the spreadsheet
4. Ensure Shopify API token has the correct permissions

## Security Notes

- Never commit actual secrets to the repository
- Regularly rotate API tokens and service account keys
- Monitor GitHub Actions usage (free tier has limits)
- Review workflow logs for any security issues