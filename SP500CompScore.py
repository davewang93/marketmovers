from matplotlib.pyplot import close
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
from scipy.stats import zscore
from yaml import warnings
import warnings
warnings.filterwarnings("ignore")


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

prevdate = 1
curdate = 0
numberofmovers = 30

filterdf = pd.read_csv('sp500.csv')
columns = filterdf['Symbol']

output = pd.DataFrame()

closedata = pd.read_csv('csvs/usequityclosetable.csv', index_col='date')
closedata2 = closedata[columns]
closedata3 = closedata.drop(columns,axis=1)

closedata.sort_values(by='date', inplace=True)
closedata = closedata.pct_change().cumsum().dropna(how='all')
closedata = closedata.apply(zscore).dropna(axis=1, how='all')
closedata.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\equityzsearchable.csv')
disttable = closedata.stack().reset_index()
closedata['Market Avg'] = closedata.mean(axis=1)

output = closedata['Market Avg']

closedata2.sort_values(by='date', inplace=True)
closedata2 = closedata2.pct_change().cumsum().dropna(how='all')
closedata2 = closedata2.apply(zscore).dropna(axis=1, how='all')
disttable2 = closedata2.stack().reset_index()
closedata2['SP500 Avg'] = closedata2.mean(axis=1)
#output2 = closedata2['SP500 Avg']

closedata3.sort_values(by='date', inplace=True)
closedata3 = closedata3.pct_change().cumsum().dropna(how='all')
closedata3 = closedata3.apply(zscore).dropna(axis=1, how='all')

closedata3['Market Ex 500 Avg'] = closedata3.mean(axis=1)

output = pd.concat([output, closedata2['SP500 Avg']], axis=1)

output = pd.concat([output, closedata3['Market Ex 500 Avg']], axis=1)

print(output.head())
output.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\equityzscores.csv')

latestmarket = closedata.iloc[-1]
latestmarket = latestmarket.drop('Market Avg')
latestmarket.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\All Market Extremes.csv')
latestmarketgainers = latestmarket.nlargest(25)
latestmarketlosers = latestmarket.nsmallest(25)
latestmarkettable = pd.concat([latestmarketgainers, latestmarketlosers])
latestmarkettable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\Market Extremes.csv')

latestsp500 = closedata2.iloc[-1]
latestsp500 = latestsp500.drop('SP500 Avg')
latestsp500.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\All SP500 Extremes.csv')
latestsp500gainers = latestsp500.nlargest(10)
latestsp500losers = latestsp500.nsmallest(10)
latestsp500table = pd.concat([latestsp500gainers, latestsp500losers])
latestsp500table.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\SP500 Extremes.csv')

disttable.columns = ['date','ticker','zscore']
disttable = disttable.set_index('date')
disttable['bins'] = pd.cut(x=disttable['zscore'], bins=[-100,-3,-2,-1,0,1,2,3,100])
disttable = disttable.groupby(['date','bins'])['bins'].count().to_frame(name = 'count').reset_index()
disttable = disttable.pivot(index='date', columns='bins', values="count")
disttable = disttable.div(disttable.sum(axis=1), axis=0).multiply(100)
print(disttable)
disttable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\Distributions.csv')

disttable2.columns = ['date','ticker','zscore']
disttable2 = disttable2.set_index('date')
disttable2['bins'] = pd.cut(x=disttable2['zscore'], bins=[-100,-3,-2,-1,0,1,2,3,100])
disttable2 = disttable2.groupby(['date','bins'])['bins'].count().to_frame(name = 'count').reset_index()
disttable2 = disttable2.pivot(index='date', columns='bins', values="count")
disttable2 = disttable2.div(disttable2.sum(axis=1), axis=0).multiply(100)
print(disttable2)
disttable2.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\SP500Distributions.csv')

'''
corrtable = closedata2.drop('SP500 Avg',axis=1).corr()
corrtable = corrtable.where(np.triu(np.ones(corrtable.shape)).astype(np.bool))
corrtable = corrtable.stack().reset_index()
corrtable.columns = ['Ticker1', 'Ticker2', 'Corr']
corrtable = corrtable.query("Ticker1 != Ticker2")
corrtable = corrtable.nlargest(25,'Corr')

corrtable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\Market Corr.csv')
'''