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
from sqlalchemy import create_engine, event
import urllib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from twilio.rest import Client

def send_sms(sms_body):
    # Your Account Sid and Auth Token from twilio.com/console
    # and set the environment variables. See http://twil.io/secure
    account_sid = conf.sms()["SID"]
    auth_token = conf.sms()["auth_token"]
    from_number = conf.sms()["from_phone"]
    to_number = conf.sms()["to_phone"]
    client = Client(account_sid, auth_token)

    message = client.messages \
        .create(
        body=sms_body,
        from_=from_number,
        to=to_number
    )


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
    # set error during execution flag
    error_flag = 0
    error_num = 0

    ticker = ""
    ticker_no_data = ""
    ticker_success = 0

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

        sqlalchemy_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)



    # cursor = cnxn.cursor()





    # Get exchange setup
    exchange = conf.settings()["exchange"]
    # Get tickers
    tickers = pd.read_sql("select ticker from update_ticker_list "
                          "where create_date = (select max(create_date) from update_ticker_list)", cnxn)
    # tickers = pd.DataFrame(pd.Series(['AAPL','AAAPL', 'bbb']), columns=["ticker"])
    # print(tickers)
    # Get Stock candles
    for index, row in tickers.iterrows():
        try:
            ticker = row.ticker
            # print(ticker)
            # get last update date for the candle
            last_timestamp = pd.read_sql("select max(data_timestamp) from Candles where ticker = '" + str(ticker)+ "'", cnxn)

            if last_timestamp.iloc[0, 0]==None:
                # start_timestamp = 1577836800 # 2020.01.01
                start_timestamp = 846806400 # 1996.01.01
            else:
                start_timestamp = last_timestamp.iloc[0, 0] + 1
            # print(start_timestamp)
            finnhub_candles = finnhub_client.stock_candles(ticker, candle_freq, start_timestamp,
                                                                      convert_date_datetime(datetime.datetime.today()))
            if finnhub_candles["s"] == "ok":
                stock_candles = pd.DataFrame(finnhub_candles)

                stock_candles["ticker"] = str(ticker)
                stock_candles["exchange"] = str(exchange)
                stock_candles = stock_candles.reindex(columns=['ticker', 'exchange',"h", "l", "o", "c", "v", "s", "t"])
                stock_candles.rename(columns={"h":"high_price", "c":"close_price",  "l":"low_price", "o":"open_price", "s":"status", "t":"data_timestamp", "v":"volume"}, inplace=True)
                # print(stock_candles)
                stock_candles.to_sql("candles", sqlalchemy_engine, if_exists="append", index=False, method="multi")
                ticker_success = ticker_success + 1
            else:
                ticker_no_data = ticker_no_data + str(ticker) +","
            # stock_candles.to_sql("update_ticker_list", sqlalchemy_engine, if_exists="append", index=False)
            sleep(1)

        except Exception as e:

            # cnxn.rollback()
            # cursor.close()
            # print('"', datetime.datetime.today(), '" , "Error when getting data with ticker \'', ticker, '\': ', e, '"')
            try:
                error_flag = 1
                error_num = error_num + 1
                last_error_time = str(datetime.datetime.today())
                error_sql = "insert into error_rec (error_time, error_info) values ('" + str(datetime.datetime.today()).replace("'","''") + \
                             "', 'Error when getting data with ticker [" + str(ticker) + "]: " + str(e).replace("'","''") + "')"
                with cnxn.cursor() as cursor:
                    cursor.execute(error_sql)


            except Exception as e2:
                error_flag = 1
                print('"', datetime.datetime.today(), '" , "Error when adding error message to database: ' + str(e2) +'", "'
                      + 'Original Error when getting data with ticker \'', ticker, '\': ', e, '"')

    if len(ticker_no_data) != 0:
        error_flag = 1
        ticker_no_data = ticker_no_data[:-1]
        error_num = error_num + ticker_no_data.count(",")

finally:
    sqlalchemy_engine.dispose()
    if error_flag == 0:
       send_sms("Algo-Trading executed correctly, Started at: " + str(start_time) + "\n Finished at: "
                + str(datetime.datetime.today()) + "\ntickers updated: " + str(ticker_success))
       send_email("Algo-Trading executed correctly", "Started at: " + str(start_time) + "\n Finished at: "
                  + str(datetime.datetime.today())+ "\ntickers updated: "
                  + str(ticker_success)+"\n Algo trading executed successfully")
    else:
        try: e
        except NameError: e = None
        if e is None:
            send_email("Algo-Trading executed finished with " + str(error_num) + " error(s)!",
                       "Algo-Trading executed finished with " + str(error_num) + " error(s)! \nStarted at: " + str(start_time) + "\n Finished at: "
                    + str(datetime.datetime.today()) + "\ntickers updated: " + str(ticker_success)
                    + "\n tickers list cannot update: " + str(ticker_no_data))

            send_sms("Algo-Trading execution finished with " + str(error_num) + " error(s), \nStarted at: " + str(start_time) + "\n Finished at: "
                    + str(datetime.datetime.today()) + "\ntickers updated: " + str(ticker_success))
        else:
            send_email("Algo-Trading executed finished with " + str(error_num) + " error(s)!",
                       "Algo-Trading executed finished with " + str(error_num) + " error(s)! \nStarted at: " + str(start_time) + "\n Finished at: "
                    + str(datetime.datetime.today()) + "\ntickers updated: " + str(ticker_success)
                    + "\n tickers list cannot update: " + str(ticker_no_data)
                     +  'Last Error detail: "' + str(last_error_time) + '" , "Error when getting data with ticker \'' + str(
                           ticker) + '\': ' + str(e) + '"')
            send_sms("Algo-Trading execution finished with " + str(error_num) + " error(s), \nStarted at: " + str(start_time) + "\n Finished at: "
                    + str(datetime.datetime.today()) + "\ntickers updated: " + str(ticker_success) + "last error @ " + str(last_error_time)
                      + 'Error when getting data with ticker \'' + str(ticker) + '\': ' + str(e))