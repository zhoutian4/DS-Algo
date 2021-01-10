import requests
import pandas as pd
import pyodbc
import psycopg2
from time import sleep
import config_parser as conf
import finnhub
import numpy as np
import datetime
import time
# import the sqlalchemy module for DataFrame to_sql
from sqlalchemy import create_engine
import urllib


def convert_date_datetime(date = datetime.datetime.today()):

    return int(time.mktime(date.timetuple()))


# Start Program
# setup api_key
api_key = conf.finnhub()["api_key"]
# api_key = "sandbox_bv838gn48v6vtpa0emu0"
# Setup client
finnhub_client = finnhub.Client(api_key=api_key)
candle_freq = conf.settings()["candle_freq"]
# print(candle_freq)

sqlserver_engine = conf.settings()["db"]
if sqlserver_engine == "postgresql":
    # setup postgreSQL server
    server = conf.postgresql()["host"]
    database = conf.postgresql()["db"]
    username = conf.postgresql()["user"]
    password = conf.postgresql()["passwd"]
    cnxn = psycopg2.connect(host=server, dbname=database, user=username, password=password)
    # create sqlalchemy engine

    sqlalchemy_engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}"
                                      .format(user=username,
                                              pw=password,
                                              host=server,
                                              db=database))
elif sqlserver_engine == "mssql":
    # setup SQL server
    server = conf.mssql()["host"]
    database = conf.mssql()["db"]
    username = conf.mssql()["user"]
    password = conf.mssql()["passwd"]
    conn_string = 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=' + server + ';DATABASE=' + database + \
            ';UID=' + username + ';PWD=' + password
    cnxn = pyodbc.connect(conn_string)
    # create sqlalchemy engine
    params = urllib.parse.quote_plus(conn_string)

    sqlalchemy_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
# cursor = cnxn.cursor()



# Get exchange setup
exchange = conf.settings()["exchange"]
# Get tickers
tickers = pd.read_sql("select ticker from update_ticker_list "
                      "where create_date = (select max(create_date) from update_ticker_list)", cnxn)
# tickers = ['AAPL']
# print(tickers)
# Get Stock candles
for index, row in tickers.iterrows():
    try:
        ticker = row.ticker
        # print(ticker)
        # get last update date for the candle
        last_timestamp = pd.read_sql("select max(data_timestamp) from Candles where ticker = '" + str(ticker)+ "'", cnxn)

        if last_timestamp.iloc[0, 0]==None:
            start_timestamp = 1577836800
        else:
            start_timestamp = last_timestamp.iloc[0, 0]
        # print(start_timestamp)
        stock_candles = pd.DataFrame(finnhub_client.stock_candles(ticker, candle_freq, start_timestamp,
                                                                  convert_date_datetime(datetime.datetime.today())))

        stock_candles["ticker"] = str(ticker)
        stock_candles["exchange"] = str(exchange)
        stock_candles = stock_candles.reindex(columns=['ticker', 'exchange',"h", "l", "o", "c", "v", "s", "t"])
        stock_candles.rename(columns={"h":"high_price", "c":"close_price",  "l":"low_price", "o":"open_price", "s":"status", "t":"data_timestamp", "v":"volume"}, inplace=True)
        # print(stock_candles)
        stock_candles.to_sql("candles", sqlalchemy_engine, if_exists="append", index=False)

        # stock_candles.to_sql("update_ticker_list", sqlalchemy_engine, if_exists="append", index=False)
        sleep(1)

    except Exception as e:
        # cnxn.rollback()
        # cursor.close()
        print('"', datetime.datetime.today(), '" , "Error when getting data with ticker \'', ticker, '\': ', e, '"')
