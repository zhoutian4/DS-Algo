

import requests
import pandas as pd
import pyodbc
from time import sleep

# Start Program
# setup api_key
api_key = "bv838gn48v6vtpa0emtg"
# api_key = "sandbox_bv838gn48v6vtpa0emu0"

# setup SQL server
server = 'George-PC'
database = 'Algo_Trade'
username = 'algorw'
password = 'password'
cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()


def update_company_profile(cursor_func=cursor, token = api_key):
    symbols = pd.read_sql('select symbol from Stock_Symbol', cnxn)
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
                cursor_func.execute("INSERT INTO Company_Info ([Country], [Currency], [Exchange], [Industry], [IPO], [logo], [Market_Capitalization], [Name], [Phone], [Share_Outstanding], [Ticker], [Weburl]) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                                    str(row.country).replace("'", "''"), str(row.currency).replace("'", "''"), str(row.exchange).replace("'", "''"),
                                    str(row.finnhubIndustry).replace("'", "''"), str(row.ipo).replace("'", "''"), str(row.logo).replace("'", "''"),
                                    str(row.marketCapitalization).replace("'", "''"), str(row.companyname).replace("'", "''"), str(row.phone).replace("'", "''"),
                                    str(row.shareOutstanding).replace("'", "''"), str(row.ticker).replace("'", "''"), str(row.weburl).replace("'", "''"))

                cursor_func.commit()
        sleep(1)

def get_stock_symbol(exchange='US', api_key_func=api_key, cursor_func=cursor ):

    r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange=' + str(exchange) + '&token=' + str(api_key_func))
    df = pd.DataFrame(r.json())
    for index, row in df.iterrows():
        cursor_func.execute("INSERT INTO Stock_Symbol (Currency, Description, Display_Symbol, Symbol, Type) values (?,?,?,?,?)",
                       str(row.currency).replace("'", "''"), str(row.description).replace("'", "''"), str(row.displaySymbol).replace("'", "''"), str(row.symbol).replace("'", "''"), str(row.type).replace("'", "''"))
    cursor_func.commit()




try:
    # get_stock_symbol()
    update_company_profile()


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
