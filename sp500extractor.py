import pandas_datareader as pdr
import quandl as ql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, VARCHAR
import mysql.connector
from datetime import datetime, timedelta,date
from configparser import ConfigParser 
from iexfinance.stocks import Stock
import os


#this stuff loads the keys
directory = os.path.dirname(os.path.abspath(__file__))
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

host = parser.get('marketmovers','host')
user = parser.get('marketmovers','user')
passwd = parser.get('marketmovers','passwd')
database = parser.get('marketmovers','database')

engine = parser.get('engines','marketmovers')

#production key
secretkey = parser.get('keys','secretkey')
#sandbox key
testkey = parser.get('keys','testkey')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#this is the iexfinance client
#iexcloud-v1 is live
#iexcloud-sandbox is sandbox
#secretkey = live testkey = sandbox
#need to make the switch in the environment variable too
os.environ['IEX_API_VERSION'] = 'iexcloud-v1'
key = secretkey   

#load SOI files and create useful vars
tickerSOI = os.path.join(directory, 'sp500.csv')
#datesList = os.path.join(directory, 'dateslist.csv')

tickers = pd.read_csv(tickerSOI, engine='python')

tickers = tickers['Symbol']

print(tickers)
#days = pd.read_csv(datesList, engine='python')

petable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\petable.csv')
pstable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\pstable.csv')
dtetable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\dtetable.csv')
ebitdatable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\ebitdatable.csv')
curdebttable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\curdebttable.csv')
evtable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\evtable.csv')
emptable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\emptable.csv')
grossprofittable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\grossprofittable.csv')
marketcaptable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\marketcaptable.csv')
pegtable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\pegtable.csv')
putcalltable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\putcalltable.csv')
profitmargintable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\profitmargintable.csv')
pbtable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\pbtable.csv')
revenuetable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\revenuetable.csv')
sharestable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sharestable.csv')
cashtable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cashtable.csv')
ttmepstable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\ttmepstable.csv')
ttmdivtable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\ttmdivtable.csv')

petable = petable[petable['ticker'].isin(tickers)]
pstable = pstable[pstable['ticker'].isin(tickers)]
dtetable = dtetable[dtetable['ticker'].isin(tickers)]
ebitdatable = ebitdatable[ebitdatable['ticker'].isin(tickers)]
curdebttable = curdebttable[curdebttable['ticker'].isin(tickers)]
evtable = evtable[evtable['ticker'].isin(tickers)]
emptable = emptable[emptable['ticker'].isin(tickers)]
grossprofittable = grossprofittable[grossprofittable['ticker'].isin(tickers)]
marketcaptable = marketcaptable[marketcaptable['ticker'].isin(tickers)]
pegtable = pegtable[pegtable['ticker'].isin(tickers)]
putcalltable = putcalltable[putcalltable['ticker'].isin(tickers)]
profitmargintable = profitmargintable[profitmargintable['ticker'].isin(tickers)]
revenuetable = revenuetable[revenuetable['ticker'].isin(tickers)]
sharestable = sharestable[sharestable['ticker'].isin(tickers)]
cashtable = cashtable[cashtable['ticker'].isin(tickers)]
ttmepstable = ttmepstable[ttmepstable['ticker'].isin(tickers)]
ttmdivtable = ttmdivtable[ttmdivtable['ticker'].isin(tickers)]

petable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500petable.csv',index=False)
pstable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500pstable.csv',index=False)
dtetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500dtetable.csv',index=False)
ebitdatable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500ebitdatable.csv',index=False)
curdebttable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500curdebttable.csv',index=False)
evtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500evtable.csv',index=False)
emptable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500emptable.csv',index=False)
grossprofittable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500grossprofittable.csv',index=False)
marketcaptable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500marketcaptable.csv',index=False)
pegtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500pegtable.csv',index=False)
putcalltable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500putcalltable.csv',index=False)
profitmargintable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500profitmargintable.csv',index=False)
pbtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500pbtable.csv',index=False)
revenuetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500revenuetable.csv',index=False)
sharestable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500sharestable.csv',index=False)
cashtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500cashtable.csv',index=False)
ttmepstable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500ttmepstable.csv',index=False)
ttmdivtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sp500ttmdivtable.csv',index=False)

print("CSVs created")
