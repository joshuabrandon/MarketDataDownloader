from Connections import *
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Docs for this API: https://www.alphavantage.co/documentation/
alphavantage_api = open("AlphaVantageAPI.txt", "r").readline()

"""
Below are the functions used to assist with the data requests.
"""

def request_string_builder(function, ticker=None, outputsize=None, apikey=alphavantage_api):
    # Building API request string
    base_url = "https://www.alphavantage.co/query?function="
    function_url = function
    symbol_url = ticker
    outputsize_url = outputsize # can be "compact" or "full"
    apikey_url = apikey

    if function == "TIME_SERIES_DAILY":
        url = base_url + function_url + "&symbol=" + symbol_url + "&outputsize=" + outputsize_url + "&apikey=" + apikey_url
    elif function == "TOP_GAINERS_LOSERS":
        url = base_url + function + "&apikey=" + apikey_url
    
    return url

def valid_request_check(data):
    if next(iter(data)) == "Error Message":
        print("Invalid API call, check requested ticker.")
        return
    else:
        print("Valid request.")

def format_top_glt_df(data_dict, classification, update_datetime):
    df = pd.DataFrame.from_dict(data_dict)
    df['classification'] = classification
    df['update_datetime'] = update_datetime
    df['change_percentage'] = df['change_percentage'].str.rstrip('%').astype(float) / 100
    
    return df

"""
Below are the functions for each type of data request.
"""

def daily_historial_data_request(ticker, outputsize='compact'):
    # API Documentation: https://www.alphavantage.co/documentation/#daily

    # Function variable(s)
    request_type = "TIME_SERIES_DAILY"
    table_name = "daily_prices"

    # Pass arugments for API request string
    url = request_string_builder(request_type, ticker, outputsize)

    # Requesting data from API, store in dict, narrow in on returned data
    r = requests.get(url)
    data = r.json()

    # Checks if the API call returns invalid data
    valid_request_check(data)

    # Continue if there are no issues
    daily_data = data['Time Series (Daily)']

    # Convert dict to dataframe
    df = pd.DataFrame.from_dict(daily_data, orient='index')
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = df.reset_index().rename(columns={'index': 'Date'})
    df['Ticker'] = ticker

    # Reoder and remap dataframe to match underlying table
    df = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
    column_mapping = {
        'Date': 'date',
        'Ticker': 'ticker',
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'Volume': 'volume'
    }
    df = df.rename(columns=column_mapping)
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Build connection string to database and create query engine
    connection_string = build_connection_string()
    engine = create_engine(connection_string)

    # Filter to only insert new records
    filter_query = f"SELECT MAX(date) FROM {table_name} WHERE Ticker = '{ticker}'"
    df_query_results = pd.DataFrame(engine.connect().execute(text(filter_query)))
    max_date = df_query_results['max'].item()

    if max_date is not None:
        filtered_df = df.loc[df['date'] > max_date]
    else:
        filtered_df = df

    filtered_df

    # Append dataframe data to database table
    filtered_df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Load complete for ticker: {ticker}")
    
    # Close engine
    engine.dispose()
    print("Engine closed.")

def top_gainers_losers_traded_request():
    # API Documentation: https://www.alphavantage.co/documentation/#gainer-loser

    # Function variable(s)
    request_type = "TOP_GAINERS_LOSERS"
    table_name = "top_gainers_losers_traded"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Pass arugments for API request string
    url = request_string_builder(request_type)

    # Requesting data from API, store in dict, narrow in on returned price data
    r = requests.get(url)
    data = r.json()

    valid_request_check(data)

    # Get date request was last updated
    request_date = datetime.strptime(data['last_updated'][:19], date_format)

    # Put the data into a dataframe
    gainers_df = format_top_glt_df(data['top_gainers'], 'top_gainer', request_date)
    losers_df = format_top_glt_df(data['top_losers'], 'top_loser', request_date)
    actives_df = format_top_glt_df(data['most_actively_traded'], 'most_active', request_date)

    # Build connection string to database and create query engine
    connection_string = build_connection_string()
    engine = create_engine(connection_string)

    # Filter to only insert new records
    filter_query = f"SELECT MAX(update_datetime) FROM {table_name}"
    df_query_results = pd.DataFrame(engine.connect().execute(text(filter_query)))
    max_date = df_query_results['max'].item()

    if request_date <= max_date and max_date is not None:
        print(f"No new data returned: database updated {max_date} and requested data updated {request_date}.")
        return
    
    # Join the dataframes containing the requested data
    combined_df = pd.concat([gainers_df, losers_df, actives_df], ignore_index=True)

    # Append dataframe data to database table
    combined_df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Load complete for gainers, losers, and most active data.")
    
    # Close engine
    engine.dispose()
    print("Engine closed.")
