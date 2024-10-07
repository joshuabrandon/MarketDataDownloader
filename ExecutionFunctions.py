from DataRequestFunctions import *
from Connections import *
from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("select * from top_gainers_losers_traded limit 10"))
    for row in result:
        print(row.ticker)

def GetRequestTickers(query):
    with engine.connect() as conn:
        df_query_results = pd.DataFrame(conn.execute(text(query)))

    return df_query_results

def RequestMultipleTickers(tickers, request_type, outputsize='compact'):
    for i in tickers:
        print(f"Requesting ticker: {i}")
        request_type(i,outputsize)
        print(f"Request for ticker: {i} complete")