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

#this stuff loads the keys
directory = os.path.dirname(os.path.abspath(__file__))
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

columns = ['Bitcoin','Ethereum']
btccol = ['Bitcoin']
ethcol = ['Ethereum']
filterdf = pd.read_csv('topscp.csv')
columnstop = filterdf['Symbol']

output = pd.DataFrame()

closedata = pd.read_csv('csvs/cryptoprice.csv', index_col='date')
closedata2 = closedata[columns]
closedata3 = closedata[columnstop]
closedata4 = closedata[btccol]
closedata5 = closedata[ethcol]
closedata.sort_values(by='date', inplace=True)
closedata = closedata.pct_change().dropna(how='all').add(1).cumprod()
closedata = closedata.apply(zscore).dropna(axis=1, how='all')
disttable = closedata.stack().reset_index()
closedata.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptozsearchable.csv')
closedata['Market Avg'] = closedata.mean(axis=1)

output = closedata['Market Avg']

closedata2.sort_values(by='date', inplace=True)
closedata2 = closedata2.pct_change().dropna(how='all').add(1).cumprod()
closedata2 = closedata2.apply(zscore).dropna(axis=1, how='all')
closedata2['BTCETH Avg'] = closedata2.mean(axis=1)
#output2 = closedata2['SP500 Avg']

closedata4.sort_values(by='date', inplace=True)
closedata4 = closedata4.pct_change().dropna(how='all').add(1).cumprod()
closedata4 = closedata4.apply(zscore).dropna(axis=1, how='all')

closedata3.sort_values(by='date', inplace=True)
closedata3 = closedata3.pct_change().dropna(how='all').add(1).cumprod()
closedata3 = closedata3.apply(zscore).dropna(axis=1, how='all')
disttable2 = closedata3.stack().reset_index()
closedata3['Top SCP Avg'] = closedata3.mean(axis=1)
print(disttable2)
closedata3 = pd.concat([closedata3, closedata4['Bitcoin']], axis=1)
closedata3.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\topscpzsearchable.csv')

closedata5.sort_values(by='date', inplace=True)
closedata5 = closedata5.pct_change().dropna(how='all').add(1).cumprod()
closedata5 = closedata5.apply(zscore).dropna(axis=1, how='all')

output = pd.concat([output, closedata2['BTCETH Avg']], axis=1)

output = pd.concat([output, closedata3['Top SCP Avg']], axis=1)

output = pd.concat([output, closedata4['Bitcoin']], axis=1)
output = pd.concat([output, closedata5['Ethereum']], axis=1)

#print(output.head())

print(output)
output.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptozscores.csv')


disttable.columns = ['date','ticker','zscore']
disttable = disttable.set_index('date')
disttable['bins'] = pd.cut(x=disttable['zscore'], bins=[-100,-3,-2,-1,0,1,2,3,100])
disttable = disttable.groupby(['date','bins'])['bins'].count().to_frame(name = 'count').reset_index()
disttable = disttable.pivot(index='date', columns='bins', values="count")
disttable = disttable.div(disttable.sum(axis=1), axis=0).multiply(100)
print(disttable)
disttable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\CryptoDistributions.csv')


disttable2.columns = ['date','ticker','zscore']
disttable2 = disttable2.set_index('date')
disttable2['bins'] = pd.cut(x=disttable2['zscore'], bins=[-100,-3,-2,-1,0,1,2,3,100])
disttable2 = disttable2.groupby(['date','bins'])['bins'].count().to_frame(name = 'count').reset_index()
disttable2 = disttable2.pivot(index='date', columns='bins', values="count")
disttable2 = disttable2.div(disttable2.sum(axis=1), axis=0).multiply(100)
print(disttable2)
disttable2.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\TopSCPDistributions.csv')

volumedata = pd.read_csv('csvs/cryptototalvolume.csv', index_col='date')
volumedatascp = volumedata[columnstop]
volumedataexscp = volumedata.drop(columnstop,axis=1)

volumedata['tmv'] = volumedata.sum(axis=1)
volumedata = volumedata[['tmv']]

volumedatascp['spcv'] = volumedatascp.sum(axis=1)
volumedatascp = volumedatascp[['spcv']]

volumedataexscp['exspcv'] = volumedataexscp.sum(axis=1)
volumedataexscp = volumedataexscp[['exspcv']]

volumetable = pd.concat([volumedata, volumedatascp,volumedataexscp], axis=1)
print(volumetable)
volumetable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptoaggvolume.csv')