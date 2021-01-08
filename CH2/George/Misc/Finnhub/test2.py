import requests
import pandas as pd
import datetime


# r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange=SS&token=bv838gn48v6vtpa0emtg')
# # print(r.json())
r = requests.get('https://finnhub.io/api/v1/stock/candle?symbol=AAPL&resolution=D&from=1609113600&to=1609620132&token=bv838gn48v6vtpa0emtg')
print(r.json())

data = pd.DataFrame(r.json())
print(data)
print(data.columns)
# data.to_csv('SS_Symbol.csv')


from datetime import timezone
dt = datetime.datetime(2020, 12, 31)
timestamp = dt.replace(tzinfo=timezone.utc).timestamp()
print(timestamp)

print(datetime.datetime(2012,4,1,0,0).timestamp())