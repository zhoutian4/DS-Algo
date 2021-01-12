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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import event


def send_email(subject, mail_body):
    #The mail addresses and password
    sender_address = conf.email()["sender"]
    sender_pass = conf.email()["sender_pwd"]
    receiver_address = conf.email()["receiver"]
    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = subject  # The subject line
    mail_content = mail_body
    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    # Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
    session.starttls()  # enable security
    session.login(sender_address, sender_pass)  # login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

def convert_date_datetime(date = datetime.datetime.today()):

    return int(time.mktime(date.timetuple()))


# Start Program
try:
    start_time = str(datetime.datetime.today())
    # setup api_key
    api_key = conf.finnhub()["api_key"]
    # api_key = "sandbox_bv838gn48v6vtpa0emu0"
    # Setup client
    finnhub_client = finnhub.Client(api_key=api_key)
    candle_freq = conf.settings()["candle_freq"]
    # print(candle_freq)
    # test

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


        @event.listens_for(sqlalchemy_engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True
    # cursor = cnxn.cursor()



    # set error during execution flag
    error_flag = 0

    # Get exchange setup
    exchange = conf.settings()["exchange"]
    # Get tickers
    tickers = pd.read_sql("select ticker from update_ticker_list "
                          "where create_date = (select max(create_date) from update_ticker_list)", cnxn)
    # tickers = pd.DataFrame(pd.Series(['AAPL']), columns=["ticker"])
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
                start_timestamp = last_timestamp.iloc[0, 0] + 1
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
            # print('"', datetime.datetime.today(), '" , "Error when getting data with ticker \'', ticker, '\': ', e, '"')
            try:
                error_flag = 1
                error_sql = "insert into error_rec (error_time, error_info) values ('" + str(datetime.datetime.today()).replace("'","''") + \
                             "', 'Error when getting data with ticker [" + str(ticker) + "]: " + str(e).replace("'","''") + "')"
                with cnxn.cursor() as cursor:
                    cursor.execute(error_sql)
                send_email("Algo-Trading executed with error!",
                           'Error detail: "'+ str(datetime.datetime.today()) + '" , "Error when getting data with ticker \'' + str(ticker)+ '\': '+ str(e)+ '"')

            except Exception as e2:
                error_flag = 1
                print('"', datetime.datetime.today(), '" , "Error when adding error message to database: ' + str(e2) +'", "'
                      + 'Original Error when getting data with ticker \'', ticker, '\': ', e, '"')


finally:
    if error_flag == 0:
       send_email("Algo-Trading executed correctly", "Started at: " + start_time + " Finished at: "+ str(datetime.datetime.today())+" algo trading executed successfully")