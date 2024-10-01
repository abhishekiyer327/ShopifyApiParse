# ShopifyApiParse

Project to Get Shopify Orders and related data to Google Sheets incrementally

[![Get Data from API](https://github.com/abhinavsreesan/ShopifyApiParse/actions/workflows/run-python.yml/badge.svg)](https://github.com/abhinavsreesan/ShopifyApiParse/actions/workflows/run-python.yml)

## Summary

Repo to use a python script to read data from the Shopify REST Api to get daily Orders & Customers data and append incrementally to a Google Sheet.

## Requirements

### Shopify API Details

Get the below details from the shopify store admin page:

    1. Shopify Store Name
    2. Shopify API Access Token

### Google Sheets API Details

Create a service account and provide access to it from the GCP console and get the below details:

    1. Google Service Account Email
    2. Google Service Account Key File

Install the Google sheet library using the below command:

```bash
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

Referenced from: https://denisluiz.medium.com/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e

## Github Actions

The script is scheduled to run daily at 12:00 AM UTC using GitHub Actions. The script will read the data from the Shopify API and append it to the Google Sheet. 
It fetches the above-mentioned values from the below secrets:
    
        1. SHOPIFY_STORE_NAME
        2. SHOPIFY_API_ACCESS_TOKEN
        3. GOOGLE_SERVICE_ACCOUNT_EMAIL
        4. GOOGLE_SERVICE_ACCOUNT_KEY_FILE

