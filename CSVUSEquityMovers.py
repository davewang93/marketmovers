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
tickerSOI = os.path.join(directory, 'equityuniverse.csv')
#datesList = os.path.join(directory, 'dateslist.csv')


usequityclosetable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityclosetable.csv')
usequityclosetable.set_index('date', inplace=True)
usequityvolumetable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityvolumetable.csv')
usequityvolumetable.set_index('date', inplace=True)
usequityopentable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityopentable.csv')
usequityopentable.set_index('date', inplace=True)
usequityintradaytable = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityintradaytable.csv')
usequityintradaytable.set_index('date', inplace=True)


tickers = pd.read_csv(tickerSOI, engine='python')
#days = pd.read_csv(datesList, engine='python')

closetable = pd.DataFrame()
volumetable = pd.DataFrame()
intradaytable = pd.DataFrame()
opentable = pd.DataFrame()
#for testing purposes
days= 0

#for each ticker in the file, pulls price data for specified date, and pushes to mysql db under associated table name
for index,row in tickers.iterrows():

    symbol = row['Symbol']
    exchange = row['Exchange']
    stock = Stock(symbol, token=key,output_format = 'pandas')
    closeslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    volumeslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    intradayslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    openslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    #for testing:
    #closeslice = pd.DataFrame({'date':(datetime.today()-timedelta(days=1)).strftime("%m/%d/%Y")},index=[0])
    try:
        quote = stock.get_quote()
        if exchange == 'XNYS':
            close = quote['close']
            close = close.reset_index(drop = True)
            closeslice[symbol] = close
            closeslice = closeslice.reset_index(drop = True)
            closeslice.set_index('date', inplace=True)
            closetable = pd.concat([closeslice, closetable],axis = 1)
            #print(closetable)
            print("Appended close for " + symbol)

            open = quote['open']
            open = open.reset_index(drop = True)
            openslice[symbol] = open
            openslice = openslice.reset_index(drop = True)
            openslice.set_index('date', inplace=True)
            opentable = pd.concat([openslice, opentable],axis = 1)
            #print(opentable)
            print("Appended open for " + symbol)

        else:
            close = quote['iexClose']
            close = close.reset_index(drop = True)
            closeslice[symbol] = close
            closeslice = closeslice.reset_index(drop = True)
            closeslice.set_index('date', inplace=True)
            closetable = pd.concat([closeslice, closetable],axis = 1)
            #print(closetable)
            print("Appended close for " + symbol)

            open = quote['iexOpen']
            open = open.reset_index(drop = True)
            openslice[symbol] = open
            openslice = openslice.reset_index(drop = True)
            openslice.set_index('date', inplace=True)
            opentable = pd.concat([openslice, opentable],axis = 1)
            #print(opentable)
            print("Appended open for " + symbol)

        volume = quote['volume']
        volume = volume.reset_index(drop = True)
        volumeslice[symbol] = volume
        volumeslice = volumeslice.reset_index(drop = True)
        volumeslice.set_index('date', inplace=True)
        volumetable = pd.concat([volumeslice, volumetable],axis = 1)
        #print(volumetable)
        print("Appended volume for " + symbol)

        intraday = quote['changePercent']
        intraday = intraday.reset_index(drop = True)
        #print(intradaytable)
        intradayslice[symbol] = intraday
        intradayslice = intradayslice.reset_index(drop = True)
        intradayslice.set_index('date', inplace=True)
        intradaytable = pd.concat([intradayslice, intradaytable],axis = 1,sort=True)
        #print(intradaytable)
        print("Appended intraday for " + symbol)

    except Exception:
        print("skipped " + symbol)
        pass

closetable = pd.concat([closetable,usequityclosetable],sort=True)
volumetable = pd.concat([volumetable,usequityvolumetable],sort=True)
opentable = pd.concat([opentable,usequityopentable],sort=True)
intradaytable = pd.concat([intradaytable,usequityintradaytable],sort=True)

closetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityclosetable.csv')
volumetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityvolumetable.csv')
opentable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityopentable.csv')
intradaytable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\usequityintradaytable.csv')

print("CSVs created")