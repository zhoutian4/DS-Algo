

import requests
import pandas as pd
import pyodbc
import psycopg2
from time import sleep
import config_parser as conf

# Start Program
# setup api_key
api_key = conf.finnhub()["api_key"]
# api_key = "sandbox_bv838gn48v6vtpa0emu0"

sqlserver_engine = conf.settings()["db"]
if sqlserver_engine == "postgresql":
    # setup postgreSQL server
    server = conf.postgresql()["host"]
    database = conf.postgresql()["db"]
    username = conf.postgresql()["user"]
    password = conf.postgresql()["passwd"]
    cnxn = psycopg2.connect(host=server, dbname=database, user=username, password=password)
elif sqlserver_engine == "mssql":
    # setup SQL server
    server = conf.mssql()["host"]
    database = conf.mssql()["db"]
    username = conf.mssql()["user"]
    password = conf.mssql()["passwd"]
    cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()


def update_company_profile(exchange='US', sqlserver_engine=sqlserver_engine,cnxn=cnxn, cursor_func=cursor, token = api_key):
    symbols = pd.read_sql("select symbol from Stock_Symbol where Exchange_Code = '" + str(exchange).replace("'", "''") + "'", cnxn)
    for symbol in symbols.iloc(0):
        r = requests.get('https://finnhub.io/api/v1/stock/profile2?symbol=' + str(symbol[0])+ '&token=' + str(token))
        # print('https://finnhub.io/api/v1/stock/profile2?symbol=' + str(symbol[0])+ '&token=' + str(token))
        # df = pd.DataFrame(r.json(), index=[0])
        df = pd.DataFrame(r.json(), index=[0])
        df.rename(columns={'name': 'companyname'}, inplace=True)
        # print(df.columns)
        # cursor_func.execute
        if not df.empty:
            for index, row in df.iterrows():
                # query = ("INSERT INTO Company_Info ([Country], [Currency], [Exchange], [Industry], [IPO], [logo], [Market_Capitalization], [Name], [Phone], [Share_Outstanding], [Ticker], [Weburl]) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                #                     str(row.country).replace("'", "''"), str(row.currency).replace("'", "''"), str(row.exchange).replace("'", "''"),
                #                     str(row.finnhubIndustry).replace("'", "''"), str(row.ipo).replace("'", "''"), str(row.logo).replace("'", "''"),
                #                     str(row.marketCapitalization).replace("'", "''"), str(row.name).replace("'", "''"), str(row.phone).replace("'", "''"),
                #                     str(row.shareOutstanding).replace("'", "''"), str(row.ticker).replace("'", "''"), str(row.weburl).replace("'", "''"))
                # print(query)
                if sqlserver_engine == "mssql":
                    cursor_func.execute("INSERT INTO Company_Info_Update ([Country], [Currency], [Exchange], [Industry], [IPO], [logo], [Market_Capitalization], [Company_Name], [Phone], [Share_Outstanding], [Ticker], [Weburl], [Exchange_Code]) values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        str(row.country).replace("'", "''"), str(row.currency).replace("'", "''"), str(row.exchange).replace("'", "''"),
                                        str(row.finnhubIndustry).replace("'", "''"), str(row.ipo).replace("'", "''"), str(row.logo).replace("'", "''"),
                                        str(row.marketCapitalization).replace("'", "''"), str(row.companyname).replace("'", "''"), str(row.phone).replace("'", "''"),
                                        str(row.shareOutstanding).replace("'", "''"), str(row.ticker).replace("'", "''"), str(row.weburl).replace("'", "''"), str(exchange).replace("'", "''"))

                    cursor_func.commit()
                elif sqlserver_engine == "postgresql":

                    country_postgre = str(row.country).replace("'", "''")
                    currency_postgre = str(row.currency).replace("'", "''")
                    exchange_postgre = str(row.exchange).replace("'", "''")
                    industry_postgre = str(row.finnhubIndustry).replace("'", "''")
                    ipo_postgre = str(row.ipo).replace("'", "''")
                    logo_postgre = str(row.logo).replace("'", "''")
                    capital_postgre = str(row.marketCapitalization).replace("'", "''")
                    companyname_postgre = str(row.companyname).replace("'", "''")
                    phone_postgre = str(row.phone).replace("'", "''")
                    share_postgre = str(row.shareOutstanding).replace("'", "''")
                    ticker_postgre = str(row.ticker).replace("'", "''")
                    weburl_postgre = str(row.weburl).replace("'", "''")
                    exchangecode_postgre = str(exchange).replace("'", "''")



                    cursor_func.execute(f"INSERT INTO Company_Info_Update([Country], [Currency], [Exchange], "
                                        f"[Industry], [IPO], [logo], [Market_Capitalization], [Company_Name], "
                                        f"[Phone], [Share_Outstanding], [Ticker], [Weburl], [Exchange_Code]) values("
                                        f"‘{country_postgre}’, ’{currency_postgre}’, ’{exchange_postgre}’, "
                                        f"’{industry_postgre}’, ’{ipo_postgre}’, ’{logo_postgre}’, "
                                        f"’{capital_postgre}’, ’{companyname_postgre}’, ’{phone_postgre}’, "
                                        f"’{share_postgre}’, ’{ticker_postgre}’, ’{weburl_postgre}’, "
                                        f"’{exchangecode_postgre}’)")

                    cursor_func.commit()
        sleep(1)

def get_stock_symbol(exchange='US', sqlserver_engine=sqlserver_engine, api_key_func=api_key, cursor_func=cursor ):

    r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange=' + str(exchange) + '&token=' + str(api_key_func))
    df = pd.DataFrame(r.json())
    for index, row in df.iterrows():
        if sqlserver_engine == "mssql":
            cursor_func.execute("INSERT INTO Stock_Symbol_Update (Exchange, Currency, Description, Display_Symbol, Symbol, Security_Type, Mic, Figi) values (?,?,?,?,?,?,?,?)",
                      str(exchange).replace("'", "''"), str(row.currency).replace("'", "''"), str(row.description).replace("'", "''"), str(row.displaySymbol).replace("'", "''"), str(row.symbol).replace("'", "''"), str(row.type).replace("'", "''"), str(row.mic).replace("'", "''"), str(row.figi).replace("'", "''"))
            cursor_func.commit()
        elif sqlserver_engine == "postgresql":

            exchange_postgre = str(exchange).replace("'", "''")
            currency_postgre = str(row.currency).replace("'", "''")
            description_postgre = str(row.description).replace("'", "''")
            displaysymbol_postgre = str(row.displaySymbol).replace("'", "''")
            symbol_postgre = str(row.symbol).replace("'", "''")
            type_postgre = str(row.type).replace("'", "''")
            mic_postgre = str(row.mic).replace("'", "''")
            figi_postgre = str(row.figi).replace("'", "''")

            cursor_func.execute(f"INSERT INTO Stock_Symbol_Update (Exchange, Currency, Description, Display_Symbol, "
                                f"Symbol, Security_Type, Mic, Figi) values ('{exchange_postgre}', "
                                f"'{currency_postgre}', '{description_postgre}', '{displaysymbol_postgre}', "
                                f"'{symbol_postgre}', '{type_postgre}', '{mic_postgre}', '{figi_postgre}')")


            # cursor_func.commit()

def get_candles(symbol, cnxn_func=cnxn, sqlserver_engine=sqlserver_engine,cnxn=cnxn, cursor_func=cursor, token = api_key):
    # o:open price
#h: high price
# l: low price
# c list of closed price
# v: volumn data
#t timestamp
# s status
    pass


def update_stock_symbol_table(sqlserver_engine=sqlserver_engine,cnxn_func=cnxn):
    if sqlserver_engine == "mssql":
        is_proc = pd.read_sql("SELECT count(*) FROM sysobjects WHERE id = object_id(N'[dbo].[usp_MergeStockSymbol]') AND "
                            "OBJECTPROPERTY(id, N'IsProcedure') = 1",cnxn_func).iloc[0,0]
        if is_proc:
            # cnxn_func.execute("EXEC usp_MergeStockSymbol")
            pass
        else:
            cnxn_func.execute("CREATE PROCEDURE usp_MergeStockSymbol AS SET NOCOUNT ON MERGE Stock_Symbol AS Trgt"
            "USING Stock_Symbol_Update as Src ON Trgt.Symbol = Src.Symbol AND Trgt.Exchange = Src.Exchange"
            "WHEN NOT MATCHED BY TARGET THEN "
            "INSERT(Exchange, Currency, Mic, [Description], Figi, Display_Symbol, Symbol, Security_Type)"
            "VALUES (Src.Exchange, Src.Currency, Src.Mic, Src.[Description], Src.Figi, Src.Display_Symbol, Src.Symbol, src.Security_Type)"
            "WHEN MATCHED AND("
            "ISNULL(Trgt.Exchange, '') <> ISNULL(Src.Exchange, '') OR ISNULL(Trgt.Currency, '') <> ISNULL(Src.Currency, '') "
            "OR ISNULL(Trgt.Mic, '') <> ISNULL(Src.Mic, '') OR ISNULL(Trgt.[Description], '') <> ISNULL(Src.[Description], '') "
            "OR ISNULL(Trgt.Figi, '') <> ISNULL(Src.Figi, '') OR ISNULL(Trgt.Display_Symbol, '') <> ISNULL(Src.Display_Symbol, '') "
            "OR ISNULL(Trgt.Symbol, '') <> ISNULL(Src.Symbol, '') OR ISNULL(Trgt.Security_Type, '') <> ISNULL(Src.Security_Type, '')"
            ") THEN UPDATE SET "
            "Trgt.Exchange = Src.Exchange, Trgt.Currency = Src.Currency, Trgt.Mic = Src.Mic, Trgt.[Description] = Src.[Description], "
            "Trgt.Figi = Src.Figi, Trgt.Display_Symbol = Src.Display_Symbol, Trgt.Symbol = Src.Symbol, Trgt.Security_Type = Src.Security_Type; "
            "--WHEN NOT MATCHED BY SOURCE THEN DELETE; " 
            "DELETE FROM Stock_Symbol_Update")
            # cnxn_func.execute("EXEC usp_MergeStockSymbol")
            #
    elif sqlserver_engine == "postgresql":
        cnxn_func.execute("INSERT INTO stock_symbol (SELECT * FROM stock_symbol_update WHERE symbol NOT IN (SELECT symbol FROM stock_symbol));)")


def update_company_profile_table(sqlserver_engine=sqlserver_engine,cnxn_func=cnxn):
    pass

def update_candles_table(sqlserver_engine=sqlserver_engine,cnxn_func=cnxn):
    pass

try:
    exchange = conf.settings()["exchange"]
    get_stock_symbol(exchange)
    # update_company_profile()

    if sqlserver_engine == "postgresql":
        cnxn.commit()
    cnxn.close()
    # symbol='AMZN'
    # r = requests.get('https://finnhub.io/api/v1/stock/profile2?symbol=' + str(symbol)+ '&token=' + str(api_key))
    # # df = pd.DataFrame(r.json(), index=[0])
    # df = pd.Series(r.json())
    # # print(df)

    # dt = pd.DataFrame(r.json(), index=[0])
    # dt.rename(columns={'name': 'companyname'}, inplace=True)
    # print(dt.columns)
    # for index, row in dt.iterrows():
    #     print(row)
    #     print(row.companyname)

    # symbols = pd.read_sql('select top(10) symbol from Stock_Symbol', cnxn)
    # # print(symbols)
    # for symbol in symbols.iloc(0):
    #     print(symbol[0])
    # token=api_key
    # symbol = ['AAA']
    # r = requests.get('https://finnhub.io/api/v1/stock/profile2?symbol=' + str(symbol[0]) + '&token=' + str(token))
    # print('https://finnhub.io/api/v1/stock/profile2?symbol=' + str(symbol[0]) + '&token=' + str(token))
    # # df = pd.DataFrame(r.json(), index=[0])
    # df = pd.DataFrame(r.json(), index=[0])
    # df.rename(columns={'name': 'companyname'}, inplace=True)
    # print(df.empty)
    # print(df.columns)

except Exception as e:
    cnxn.rollback()
    cursor.close()
    print("Error")
    print(e)
