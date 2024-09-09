from Connections import *
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# Docs for this API: https://www.alphavantage.co/documentation/
alphavantage_api = open("AlphaVantageAPI.txt", "r").readline()

def daily_historial_data_request(ticker, size='compact'):
    # Building API request string
    base_url = "https://www.alphavantage.co/query?function="
    function_url = "TIME_SERIES_DAILY"
    symbol_url = ticker
    outputsize_url = size
    apikey_url = alphavantage_api
    url = base_url + function_url + "&symbol=" + symbol_url + "&outputsize=" + outputsize_url + "&apikey=" + apikey_url

    # Requesting data from API, store in dict, narrow in on returned price data
    r = requests.get(url)
    data = r.json()
    daily_data = (data["Time Series (Daily)"])

    # Convert dict to dataframe
    df = pd.DataFrame.from_dict(daily_data, orient='index')
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = df.reset_index().rename(columns={'index': 'Date'})
    df['Ticker'] = ticker

    # Build connection string to database and create query engine
    user = conn_params["user"]
    password = conn_params["password"]
    host = conn_params["host"]
    port = conn_params["port"]
    database = conn_params["dbname"]
    table_name = "daily_prices"
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)

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
    
    # Close engine
    engine.dispose()