import config_parser as conf
import psycopg2
import pyodbc
# print(cnf_p.mssql())
# print(cnf_p.postgresql())
# print(cnf_p.finnhub())
# data = cnf_p.finnhub()
# print(data.keys())
# print(data["api_key"])
import pandas as pd
import requests
# sqlserver_engine = conf.settings()["db"]
sqlserver_engine = "mssql"
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
# cursor = cnxn.cursor()
# r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange=US&token=bv838gn48v6vtpa0emtg')
# result = pd.DataFrame(r.json())
# # print(r.json())
# print(result.columns)
is_proc = pd.read_sql("SELECT count(*) FROM sysobjects WHERE id = object_id(N'[dbo].[MergeStockSymbol]') AND "
                            "OBJECTPROPERTY(id, N'IsProcedure') = 1",cnxn).iloc[0,0]
if is_proc:
    print(is_proc)
else:
    print("no")
# import psycopg2
# import sys
#
# con = None
#
# try:
#
#     con = psycopg2.connect(host=server, database=database, user=username,
#         password=password)
#
#     cur = con.cursor()
#     cur.execute('SELECT version()')
#
#     version = cur.fetchone()[0]
#     print(version)
#
# except psycopg2.DatabaseError as e:
#
#     print(f'Error {e}')
#     sys.exit(1)
#
# finally:
#
#     if con:
#         con.close()