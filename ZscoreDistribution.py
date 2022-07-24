from itertools import groupby
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
closedata2.sort_values(by='date', inplace=True)
closedata2 = closedata2.pct_change().cumsum().dropna(how='all')
closedata2 = closedata2.apply(zscore).dropna(axis=1, how='all')

output = closedata.stack().reset_index()
output.columns = ['date','ticker','zscore']
output = output.set_index('date')
output['bins'] = pd.cut(x=output['zscore'], bins=[-100,-3,-2,-1,0,1,2,3,100])
output = output.groupby(['date','bins'])['bins'].count().to_frame(name = 'count').reset_index()
output = output.pivot(index='date', columns='bins', values="count")
output = output.div(output.sum(axis=1), axis=0).multiply(100)
print(output)
output.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\Distributions.csv')

output2 = closedata2.stack().reset_index()
output2.columns = ['date','ticker','zscore']
output2 = output2.set_index('date')
output2['bins'] = pd.cut(x=output2['zscore'], bins=[-100,-3,-2,-1,0,1,2,3,100])
output2 = output2.groupby(['date','bins'])['bins'].count().to_frame(name = 'count').reset_index()
output2 = output2.pivot(index='date', columns='bins', values="count")
output2 = output2.div(output2.sum(axis=1), axis=0).multiply(100)
print(output2)
output2.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\SP500Distributions.csv')