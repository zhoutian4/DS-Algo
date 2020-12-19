# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
#
#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     print_hi('PyCharm')
#
# # See PyCharm help at https://www.jetbrains.com/help/pycharm/
# import finnhub

# Setup client

# finnhub_client = finnhub.Client(api_key="sandbox_bv838gn48v6vtpa0emu0")

import requests

import requests
import pandas as pd
import pyodbc

# setup api_key
# api_key = "bv838gn48v6vtpa0emtg"
api_key = "sandbox_bv838gn48v6vtpa0emu0"

# setup SQL server
server = 'George-PC'
database = 'Algo_Trade'
username = 'algorw'
password = 'password'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
try:
    r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange=US&token=' + str(api_key))
    # print(r)
    # print(r.json())
    # print(type(r.json()))
    # print(r.json()[9464:9469])
    df = pd.DataFrame(r.json())
    print(df)
    # print(df.shape())
    for index, row in df.iterrows():

        cursor.execute("INSERT INTO Stock_Symbol (Currency, Description, Display_Symbol, Symbol, Type) values (?,?,?,?,?)",
                       str(row.currency).replace("'", "''"), str(row.description).replace("'", "''"), str(row.displaySymbol).replace("'", "''"), str(row.symbol).replace("'", "''"), str(row.type).replace("'", "''"))

    cursor.commit()
    #Convert to Pandas Dataframe

    # print(pd.DataFrame(r))



    # Stock candles
    # res = finnhub_client.stock_candles('AAPL', 'D', 1590988249, 1591852249)
    # print(res)

    cursor.close()
except Exception as e:
    cursor.close()
    print("Error")
    print(e)
