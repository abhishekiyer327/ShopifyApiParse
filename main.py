import time
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import json

# Google Sheets API
from google.oauth2 import service_account
import gspread
from gspread_dataframe import set_with_dataframe

# Get Config from Env Variables

# Shopify API Config Details
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
SHOP_NAME = os.environ.get('SHOP_NAME')

# Google Sheets Config Details
GOOGLE_SHEET_SECRET = os.environ.get('GOOGLE_SHEET_SECRET')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# Shopify API Base URL to Call
shopify_base_url = f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/'

# Shopify API Relative URL Dictionary with Params

shopify_dict = {
    'orders_count': {
        'relative_url': 'orders/count.json',
        'params': {
            'status': 'any',
        },
        'schema': ['count']
    },
    'orders': {
        'relative_url': 'orders.json',
        'params': {
            'status': 'any',
            'limit': '250',
            'created_at_max': f'{datetime.now().date()}',
            'created_at_min': f'{datetime.now().date() - timedelta(days=1)}',
            # 'ids': '5917569057042',
            'fields': """id,customer.id,customer,current_subtotal_price,current_total_additional_fees_set,
                         current_total_discounts,current_total_duties_set,current_total_price,current_total_tax,
                         shipping_address,financial_status,fulfillment_status,payment_gateway_names,line_items,
                         created_at,updated_at"""
        },
        'schema': ['id', 'created_at', 'updated_at', 'product_id', 'customer.id', 'customer.created_at',
                   'current_subtotal_price', 'current_total_additional_fees_set',
                   'current_total_discounts', 'current_total_duties_set', 'current_total_price', 'current_total_tax',
                   'customer.default_address.province', 'customer.default_address.zip', 'shipping_address.province',
                   'shipping_address.zip', 'financial_status', 'fulfillment_status', 'payment_gateway_names',
                   'line_items'],
        'primary_key': 'id',
        'primary_key_pos': 1
    },
    'products': {
        'relative_url': 'products.json',
        'params': None,
        'schema': ['id', 'title', 'vendor', 'product_type', 'created_at', 'updated_at']
    },
    'customers_count': {
        'relative_url': 'customers/count.json',
        'params': {
            'status': 'any',
        },
        'schema': ['count']
    },
    'customers': {
        'relative_url': 'customers.json',
        'params': {
            'limit': '250',
            'created_at_max': f'{datetime.now().date()}',
            'created_at_min': f'{datetime.now().date() - timedelta(days=1)}',
            'fields': 'id,email,created_at,updated_at,last_order_id',
        },
        'schema': ['id', 'created_at', 'updated_at', 'last_order_id'],
        'primary_key': 'id',
        'primary_key_pos': 1
    },
}

# Google Sheets API Scopes
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


def get_data_from_shopify_api(shopify_base_url: str, shopify_relative_url: str, params: dict, token: str) -> requests:
    """
    Function to get data from the Shopify API
    :param params: Parameters to pass to the API
    :param shopify_relative_url: Relative API specific URL
    :param shopify_base_url: Base Shopify URL with Shop Name
    :param token: Shopify Private App Token
    :return: Return the request output if status is OK else -1
    """
    headers = {'User-Agent': 'PostmanRuntime/7.37.0', 'X-Shopify-Access-Token': f"{token}"}
    res = requests.get(shopify_base_url + shopify_relative_url, headers=headers, params=params)

    if res.status_code == 200:
        print(f'Log: Data Received from Shopify {api} API')
        return res
    else:
        print(res.text)
        return -1


def get_paginated_data_from_shopify_api(api: str, historical=False) -> requests:
    """
    Function to get data from the Shopify API with pagination
    :param historical:
    :param api: API name to call
    """
    last_id = 0
    all_api_df = pd.DataFrame()
    params = shopify_dict[f'{api}_count']['params']
    if historical:
        params['created_at_max'] = '2024-05-30'
        params['created_at_min'] = '2023-01-01'
    else:
        params['created_at_max'] = shopify_dict[api]['params']['created_at_max']
        params['created_at_min'] = shopify_dict[api]['params']['created_at_min']
    count_api = get_data_from_shopify_api(shopify_base_url, shopify_dict[f'{api}_count']['relative_url'],
                                          params, SHOPIFY_ACCESS_TOKEN)
    count = count_api.json()['count']
    print(f'Total Count of {api}: {count}')
    total_records = 0
    api_params = shopify_dict[api]['params']
    if historical:
        api_params['created_at_max'] = '2024-05-30'
        api_params['created_at_min'] = '2023-01-01'
    while total_records < count:
        api_params['since_id'] = int(last_id)
        result = get_data_from_shopify_api(shopify_base_url, shopify_dict[api]['relative_url'],
                                           api_params, SHOPIFY_ACCESS_TOKEN)
        data = parse_post_data(result, api, shopify_dict[api]['schema'])
        last_id = data['id'].max()
        all_api_df = pd.concat([all_api_df, data])
        total_records += data['id'].nunique()
        print(f'Records Completed: {total_records}  Records Pending: {count - total_records}')
        if len(data) < int(shopify_dict[api]['params']['limit']):
            break
    print(f'Total Orders: {total_records}')
    return all_api_df


def parse_post_data(json_data: json, api_name: str, schema: str) -> pd.DataFrame | None:
    """
    Function to parse the Reddit API output into a pandas datafrome
    :param schema: Expected schema of the API output
    :param api_name: API name to parse
    :param json_data: API output
    :return: A pandas dataframe if the API output has data else -1
    """

    # Pandas code to read json and output to dataframe after flattening json dynamically
    if api_name == 'products':
        df = pd.json_normalize(json_data.json()[api_name])
        df = df[schema]
    elif api_name in ['orders']:
        df = pd.json_normalize(json_data.json()[api_name])
        df = df.explode('line_items')
        df['product_id'] = df['line_items'].str['product_id']
        df = df[schema]
    elif api_name in ['customers']:
        df = pd.json_normalize(json_data.json()[api_name])
        df = df[schema]
    else:
        df = pd.json_normalize(json_data.json()[api_name])
        df = df[schema]
    if df.empty:
        return -1
    else:
        print(f'Log: Data Parsed for {api} Successfully')
        return df


def get_google_sheets_credentials():
    """
    Function to get the Google Sheets API credentials
    :return: Service object if the credentials are correct else -1 if the credentials are incorrect
    """
    credentials = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_SHEET_SECRET, strict=False),
                                                                        scopes=scopes)
    if credentials:
        print('Log: Google Credentials Created Successfully')
        return credentials
    else:
        return -1


def sheet_incremental_load(df: pd.DataFrame, api: str, credentials):
    sheet_exists = False
    gc = gspread.authorize(credentials)
    # open a google sheet
    gs = gc.open_by_key(SPREADSHEET_ID)
    # select a work sheet from its name
    try:
        worksheet1 = gs.worksheet(api)
        sheet_exists = True
    except:
        worksheet1 = gs.add_worksheet(title=api, rows=100, cols=20)

    # update the sheet with the dataframe
    if sheet_exists:
        # Append the data to the existing sheet
        primary_column = shopify_dict[api]['primary_key']
        primary_column_list = df[primary_column].tolist()
        for value in primary_column_list:
            if worksheet1.find(str(value), in_column=int(shopify_dict[api]['primary_key_pos'])):
                worksheet1.delete_rows(
                    worksheet1.find(str(value), in_column=int(shopify_dict[api]['primary_key_pos'])).row)
            time.sleep(1)

        set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False, include_column_header=False,
                           row=worksheet1.row_count + 1, resize=False)

    else:
        set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
                           include_column_header=True, resize=True)


if __name__ == '__main__':

    api_to_run = ['orders', 'customers']

    creds = get_google_sheets_credentials()

    for api in api_to_run:
        if api in ['orders', 'customers']:
            data = get_paginated_data_from_shopify_api(api)
            data.to_csv('data/{api}_{datetime.now().date()}.csv')
            sheet_incremental_load(data, api, creds)
        else:
            result = get_data_from_shopify_api(shopify_base_url, shopify_dict[api]['relative_url'],
                                               shopify_dict[api]['params'], SHOPIFY_ACCESS_TOKEN)
            data = parse_post_data(result, api, shopify_dict[api]['schema'])
            data.to_csv('data/{api}_{datetime.now().date()}.csv')
