# Setting Up Shopify Access Token

## Important Note
The API ID and Secret Key you provided are used to create a private app and generate an access token. The access token is what the script needs to access the Shopify API.

## Steps to Generate Access Token

1. **Log in to your Shopify Admin Panel**
   - Go to https://brown-sugar-pre-orders.myshopify.com/admin
   - Or use your custom domain admin panel

2. **Navigate to Apps**
   - In the Shopify admin, go to "Apps"
   - Click on "App and sales channel settings" (or similar)

3. **Create/Manage Private App**
   - Look for "Manage private apps" or "Custom apps"
   - Create a new private app if you haven't already

4. **Configure API Permissions**
   Required permissions for this sync:
   - Read access to Orders
   - Read access to Customers (if needed)
   - Read access to Products (if needed)

5. **Generate Access Token**
   - After setting permissions, Shopify will generate an access token
   - It will look like: `shpat_xxxxxxxxxxxxxxxxxxxxx`
   - Copy this token

6. **Update Configuration**
   Replace the placeholder token in `.env` file:
   ```
   SHOPIFY_ACCESS_TOKEN=shpat_your_actual_token_here
   ```

## Security Notes
- Keep your access token secure and never commit it to version control
- The token in the current `.env` file is from your original script - verify it's still valid
- If you regenerate the token, update all scripts using it

## Verifying Your Token
Run the test script to verify your token works:
```bash
python test_setup.py
```

This will test the connection to your Shopify store.