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

'''
# Create a new DB in mySQL w/ block below
mydb = mysql.connector.connect(
        host = host,
        user = user,
        passwd = passwd,
    )

#create cursor
cursor = mydb.cursor()

#create a db
cursor.execute("CREATE DATABASE marketmovers")
'''

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

'''
1. Take ticker universe
2. pull quotes for each ticker
3. save relevant quote point to correct table - Stock.get_quote(**kwargs)
    a. ie closing price - goes to closing price table. result should be something like this

    Date - Appl - Msft - Tsla
    1.1.21  1       2       3
    1.2.21  2       2       1
    1.3.21  3       2.5     2
    1.4.21  4       3       1.5

    b. for US equities there should be 4 tables generated
        1. closing price table - 24hr movers
        2. volume table =- 
        3. intraday pct change table
        4. open and previous close - overnight table (this might require to much date adjustments) - or just a table for open, than we do the manipulation
           later using the close price table and the open price table

4. this script will just update the main data tables - then separate script will generate the end user table.
'''
#this is the iexfinance client
#iexcloud-v1 is live
#iexcloud-sandbox is sandbox
#secretkey = live testkey = sandbox
#need to make the switch in the environment variable too
os.environ['IEX_API_VERSION'] = 'iexcloud-v1'
key = secretkey   

#load SOI files and create useful vars
tickerSOI = os.path.join(directory, 'equityuniverse995.csv')
#datesList = os.path.join(directory, 'dateslist.csv')

tickers = pd.read_csv(tickerSOI, engine='python')
#days = pd.read_csv(datesList, engine='python')

closetable = pd.DataFrame()
volumetable = pd.DataFrame()
intradaytable = pd.DataFrame()
opentable = pd.DataFrame()
#for testing purposes
days= 1
'''

'''
#for each ticker in the file, pulls price data for specified date, and pushes to mysql db under associated table name
for index,row in tickers.iterrows():

    symbol = row['Symbol']
    stock = Stock(symbol, token=key,output_format = 'pandas')
    closeslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    volumeslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    intradayslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    openslice = pd.DataFrame({'date':(date.today()-timedelta(days=days))},index=[0])
    #for testing:
    #closeslice = pd.DataFrame({'date':(datetime.today()-timedelta(days=1)).strftime("%m/%d/%Y")},index=[0])
    try:
        quote = stock.get_quote()
   
    except Exception:
        print("skipped " + symbol)
        pass

    close = quote['close']
    close = close.reset_index(drop = True)
    closeslice[symbol] = close
    closeslice = closeslice.reset_index(drop = True)
    closeslice.set_index('date', inplace=True)
    closetable = pd.concat([closeslice, closetable],axis = 1)
    #print(closetable)
    print("Appended close for " + symbol)

    volume = quote['volume']
    volume = volume.reset_index(drop = True)
    volumeslice[symbol] = volume
    volumeslice = volumeslice.reset_index(drop = True)
    volumeslice.set_index('date', inplace=True)
    volumetable = pd.concat([volumeslice, volumetable],axis = 1)
    #print(volumetable)
    print("Appended volume for " + symbol)

    open = quote['open']
    open = open.reset_index(drop = True)
    openslice[symbol] = open
    openslice = openslice.reset_index(drop = True)
    openslice.set_index('date', inplace=True)
    opentable = pd.concat([openslice, opentable],axis = 1)
    #print(opentable)
    print("Appended open for " + symbol)

    intraday = quote['changePercent']
    intraday = intraday.reset_index(drop = True)
    #print(intradaytable)
    intradayslice[symbol] = intraday
    intradayslice = intradayslice.reset_index(drop = True)
    intradayslice.set_index('date', inplace=True)
    intradaytable = pd.concat([intradayslice, intradaytable],axis = 1,sort=True)
    #print(intradaytable)
    print("Appended intraday for " + symbol)


try:
    closetable.to_sql('usequityclosetable', engine, if_exists='append')
    #closetable.to_sql('usequityclosetable', engine, if_exists='append', index_label='index', dtype={closetable.index.name:VARCHAR(5)})
except:
    closedata = pd.read_sql('SELECT * FROM usequityclosetable', engine)
    closedata.set_index('date', inplace=True)
    closeddf = pd.concat([closedata,closetable],sort=True)
    #df2.set_index('date', inplace=True)
    print(closeddf)
    closeddf.to_sql('usequityclosetable', engine, if_exists = 'replace')

try:
    volumetable.to_sql('usequityvolumetable', engine, if_exists='append')
    #closetable.to_sql('usequityclosetable', engine, if_exists='append', index_label='index', dtype={closetable.index.name:VARCHAR(5)})
except:
    volumedata = pd.read_sql('SELECT * FROM usequityvolumetable', engine)
    volumedata.set_index('date', inplace=True)
    volumedf = pd.concat([volumedata,volumetable],sort=True)
    #df2.set_index('date', inplace=True)
    print(volumedf)
    volumedf.to_sql('usequityvolumetable', engine, if_exists = 'replace', ROW_FORMAT='DYNAMIC')

try:
    opentable.to_sql('usequityopentable', engine, if_exists='append')
    #closetable.to_sql('usequityclosetable', engine, if_exists='append', index_label='index', dtype={closetable.index.name:VARCHAR(5)})
except:
    opendata = pd.read_sql('SELECT * FROM usequityopentable', engine)
    opendata.set_index('date', inplace=True)
    opendf = pd.concat([opendata,opentable],sort=True)
    #df2.set_index('date', inplace=True)
    print(opendf)
    opendf.to_sql('usequityopentable', engine, if_exists = 'replace')


try:
    intradaytable.to_sql('usequityintradaytable', engine, if_exists='append')
    #closetable.to_sql('usequityclosetable', engine, if_exists='append', index_label='index', dtype={closetable.index.name:VARCHAR(5)})
except:
    intradaydata = pd.read_sql('SELECT * FROM usequityintradaytable', engine)
    intradaydata.set_index('date', inplace=True)
    indtradaydf = pd.concat([intradaydata,intradaytable],sort=True)
    #df2.set_index('date', inplace=True)
    print(indtradaydf)
    indtradaydf.to_sql('usequityintradaytable', engine, if_exists = 'replace')

