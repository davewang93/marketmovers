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
tickerSOI = os.path.join(directory, 'priceratiouniverse.csv')
#datesList = os.path.join(directory, 'dateslist.csv')

tickers = pd.read_csv(tickerSOI, engine='python')
#days = pd.read_csv(datesList, engine='python')
petable = pd.DataFrame()
pstable = pd.DataFrame()
dtetable = pd.DataFrame()
ebitdatable = pd.DataFrame()
curdebttable = pd.DataFrame()
evtable = pd.DataFrame()
emptable = pd.DataFrame()
grossprofittable = pd.DataFrame()
marketcaptable = pd.DataFrame()
pegtable = pd.DataFrame()
putcalltable = pd.DataFrame()
profitmargintable = pd.DataFrame()
pbtable = pd.DataFrame()
revenuetable = pd.DataFrame()
sharestable = pd.DataFrame()
cashtable = pd.DataFrame()
ttmepstable = pd.DataFrame()
ttmdivtable = pd.DataFrame()


for index,row in tickers.iterrows():

    symbol = row['Symbol']
    stock = Stock(symbol, token=key,output_format = 'pandas')

    try:
        stats = stock.get_advanced_stats()
        
        
    except Exception:
        print("skipped " + symbol)
        pass

    statstrans = stats.transpose()
    statstrans.to_csv(f'D:\OneDrive\David\src\MarketMovers\CSVs\Stock Stats\{symbol}.csv')

    pe = stats['peRatio']
    petable = pd.concat([petable, pe])
    print("Appended pe for " + symbol)

    ps = stats['priceToSales']
    pstable = pd.concat([pstable, ps])
    print("Appended ps for " + symbol)

    dte = stats['debtToEquity']
    dtetable = pd.concat([dtetable, dte])
    print("Appended dte for " + symbol)

    ebitda = stats['EBITDA']
    ebitdatable = pd.concat([ebitdatable, ebitda])
    print("Appended ebitda for " + symbol)

    curdebt = stats['currentDebt']
    curdebttable = pd.concat([curdebttable, curdebt])
    print("Appended curdebt for " + symbol)

    ev = stats['enterpriseValue']
    evtable = pd.concat([evtable, ev])
    print("Appended ev for " + symbol)

    emp = stats['employees']
    emptable = pd.concat([emptable, emp])
    print("Appended emp for " + symbol)

    grossprofit = stats['grossProfit']
    grossprofittable = pd.concat([grossprofittable, grossprofit])
    print("Appended grossprofit for " + symbol)

    marketcap = stats['marketcap']
    marketcaptable = pd.concat([marketcaptable, marketcap])
    print("Appended marketcap for " + symbol)

    peg = stats['pegRatio']
    pegtable = pd.concat([pegtable, peg])
    print("Appended peg for " + symbol)

    putcall = stats['putCallRatio']
    putcalltable = pd.concat([putcalltable, putcall])
    print("Appended putcall for " + symbol)

    profitmargin = stats['profitMargin']
    profitmargintable = pd.concat([profitmargintable, profitmargin])
    print("Appended profitmargin for " + symbol)

    pb = stats['priceToBook']
    pbtable = pd.concat([pbtable, pb])
    print("Appended pb for " + symbol)

    revenue = stats['revenue']
    revenuetable = pd.concat([revenuetable, revenue])
    print("Appended revenue for " + symbol)

    shares = stats['sharesOutstanding']
    sharestable = pd.concat([sharestable, shares])
    print("Appended shares for " + symbol)

    cash = stats['totalCash']
    cashtable = pd.concat([cashtable, cash])
    print("Appended cash for " + symbol)

    ttmeps = stats['ttmEPS']
    ttmepstable = pd.concat([ttmepstable, ttmeps])
    print("Appended ttmeps for " + symbol)

    ttmdiv = stats['ttmDividendRate']
    ttmdivtable = pd.concat([ttmdivtable, ttmdiv])
    print("Appended ttmdiv for " + symbol)
    
    

petable.rename(columns={ petable.columns[0]: 'pe ratio' }, inplace = True)   
petable.index.names = ['ticker']
pstable.rename(columns={ pstable.columns[0]: 'ps ratio' }, inplace = True)   
pstable.index.names = ['ticker']
dtetable.rename(columns={ dtetable.columns[0]: 'dte' }, inplace = True)   
dtetable.index.names = ['ticker']
ebitdatable.rename(columns={ ebitdatable.columns[0]: 'ebitda' }, inplace = True)   
ebitdatable.index.names = ['ticker']
curdebttable.rename(columns={ curdebttable.columns[0]: 'curdebt' }, inplace = True)   
curdebttable.index.names = ['ticker']
evtable.rename(columns={ evtable.columns[0]: 'ev' }, inplace = True)   
evtable.index.names = ['ticker']
emptable.rename(columns={ emptable.columns[0]: 'employees' }, inplace = True)   
emptable.index.names = ['ticker']
grossprofittable.rename(columns={ grossprofittable.columns[0]: 'gross profit' }, inplace = True)   
grossprofittable.index.names = ['ticker']
marketcaptable.rename(columns={ marketcaptable.columns[0]: 'market cap' }, inplace = True)   
marketcaptable.index.names = ['ticker']
pegtable.rename(columns={ pegtable.columns[0]: 'peg' }, inplace = True)   
pegtable.index.names = ['ticker']
putcalltable.rename(columns={ putcalltable.columns[0]: 'putcall' }, inplace = True)   
putcalltable.index.names = ['ticker']
profitmargintable.rename(columns={ profitmargintable.columns[0]: 'profit margin' }, inplace = True)   
profitmargintable.index.names = ['ticker']
pbtable.rename(columns={ pbtable.columns[0]: 'pb ratio' }, inplace = True)   
pbtable.index.names = ['ticker']
revenuetable.rename(columns={ revenuetable.columns[0]: 'revenue' }, inplace = True)   
revenuetable.index.names = ['ticker']
sharestable.rename(columns={ sharestable.columns[0]: 'shares' }, inplace = True)   
sharestable.index.names = ['ticker']
cashtable.rename(columns={ cashtable.columns[0]: 'cash' }, inplace = True)   
cashtable.index.names = ['ticker']
ttmepstable.rename(columns={ ttmepstable.columns[0]: 'ttmeps' }, inplace = True)   
ttmepstable.index.names = ['ticker']
ttmdivtable.rename(columns={ ttmdivtable.columns[0]: 'ttmdiv' }, inplace = True)   
ttmdivtable.index.names = ['ticker']

petable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\petable.csv')
pstable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\pstable.csv')
dtetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\dtetable.csv')
ebitdatable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\ebitdatable.csv')
curdebttable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\curdebttable.csv')
evtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\evtable.csv')
emptable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\emptable.csv')
grossprofittable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\grossprofittable.csv')
marketcaptable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\marketcaptable.csv')
pegtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\pegtable.csv')
putcalltable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\putcalltable.csv')
profitmargintable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\profitmargintable.csv')
pbtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\pbtable.csv')
revenuetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\revenuetable.csv')
sharestable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\sharestable.csv')
cashtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cashtable.csv')
ttmepstable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\ttmepstable.csv')
ttmdivtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\ttmdivtable.csv')

print("CSVs created")
