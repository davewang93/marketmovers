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

prevdate = 1
curdate = 0
numberofmovers = 30

marketcaprank = pd.read_csv('csvs/cryptomarketcaprank.csv')
marketcaprank = marketcaprank.drop('date',1)
marketcapcolumns = marketcaprank.columns
marketcaprank[marketcapcolumns] = marketcaprank[marketcapcolumns].apply(pd.to_numeric, errors='coerce')
marketcaprangetable = ((marketcaprank.iloc[curdate] - marketcaprank.iloc[prevdate]))
marketcaprangetable = marketcaprangetable.transpose()
marketcaprangetable = pd.DataFrame(marketcaprangetable)
#print(marketcaprangetable)
#print(type(marketcaprangetable))
marketcaprangetable.index.names = ['ticker']
marketcaprangetable.rename(columns={ marketcaprangetable.columns[0]: 'rank change' }, inplace = True)
marketcaprangetablegainers = marketcaprangetable.nlargest(numberofmovers,'rank change')
marketcaprangetablelosers = marketcaprangetable.nsmallest(numberofmovers,'rank change')
marketcaprangetable = pd.concat([marketcaprangetablegainers, marketcaprangetablelosers])
marketcaprangetable.reset_index(inplace = True)
marketcaprangetable.to_sql('cryptomarketcaprank', engine, if_exists='replace')

volumedata = pd.read_csv('csvs/cryptototalvolume.csv')
volumedata = volumedata.drop('date',1)
volumecolumns = volumedata.columns
volumedata[volumecolumns] = volumedata[volumecolumns].apply(pd.to_numeric, errors='coerce')
volumedata.replace(0, np.nan, inplace=True)
volumerangetable = ((volumedata.iloc[curdate] - volumedata.iloc[prevdate])/volumedata.iloc[[prevdate]]*100)
volumerangetable = volumerangetable.transpose()
volumerangetable.index.names = ['ticker']
volumerangetable.rename(columns={ volumerangetable.columns[0]: "pct change" }, inplace = True)
volumerangetablegainers = volumerangetable.nlargest(numberofmovers,'pct change')
volumerangetablelosers = volumerangetable.nsmallest(numberofmovers,'pct change')
volumerangetable = pd.concat([volumerangetablegainers, volumerangetablelosers])
volumerangetable.reset_index(inplace = True)
volumerangetable.to_sql('cryptototalvolume', engine, if_exists='replace')

circulatingdata = pd.read_csv('csvs/cryptocirculatingsupply.csv')
circulatingdata = circulatingdata.drop('date',1)
circulatingcolumns = circulatingdata.columns
circulatingdata[circulatingcolumns] = circulatingdata[circulatingcolumns].apply(pd.to_numeric, errors='coerce')
circulatingrangetable = ((circulatingdata.iloc[curdate] - circulatingdata.iloc[prevdate])/circulatingdata.iloc[[prevdate]]*100)
circulatingrangetable = circulatingrangetable.transpose()
circulatingrangetable.index.names = ['ticker']
circulatingrangetable.rename(columns={ circulatingrangetable.columns[0]: "pct change" }, inplace = True)
circulatingrangetablegainers = circulatingrangetable.nlargest(numberofmovers,'pct change')
circulatingrangetablelosers = circulatingrangetable.nsmallest(numberofmovers,'pct change')
circulatingrangetable = pd.concat([circulatingrangetablegainers, circulatingrangetablelosers])
circulatingrangetable.reset_index(inplace = True)
circulatingrangetable.to_sql('cryptocirculatingsupply', engine, if_exists='replace')

pricedata = pd.read_csv('csvs/cryptoprice.csv')
pricedata = pricedata.drop('date',1)
pricecolumns = pricedata.columns
pricedata[pricecolumns] = pricedata[pricecolumns].apply(pd.to_numeric, errors='coerce')
pricerangetable = ((pricedata.iloc[curdate] - pricedata.iloc[prevdate])/pricedata.iloc[[prevdate]]*100)
pricerangetable = pricerangetable.transpose()
pricerangetable.index.names = ['ticker']
pricerangetable.rename(columns={ pricerangetable.columns[0]: "pct change" }, inplace = True)
pricerangetablegainers = pricerangetable.nlargest(numberofmovers,'pct change')
pricerangetablelosers = pricerangetable.nsmallest(numberofmovers,'pct change')
pricerangetable = pd.concat([pricerangetablegainers, pricerangetablelosers])
pricerangetable.reset_index(inplace = True)
pricerangetable.to_sql('cryptopricechangecalc', engine, if_exists='replace')

pricechangedata = pd.read_csv('csvs/cryptopricechange.csv')
pricechangedata = pricechangedata.iloc[[curdate]]
pricechangedata = pricechangedata.drop('date',1)
pricechangecolumns = pricechangedata.columns
pricechangedata[pricechangecolumns] = pricechangedata[pricechangecolumns].apply(pd.to_numeric, errors='coerce')
pricechangedata = pricechangedata
pricechangedata = pricechangedata.transpose()
pricechangedata.index.names = ['ticker']
pricechangedata.rename(columns={ pricechangedata.columns[0]: "pct change" }, inplace = True)
pricechangegainers = pricechangedata.nlargest(numberofmovers,'pct change')
pricechangelosers = pricechangedata.nsmallest(numberofmovers,'pct change')
pricechangedata = pd.concat([pricechangegainers, pricechangelosers])
pricechangedata.reset_index(inplace = True)
pricechangedata.to_sql('cryptopricechange', engine, if_exists='replace')

print("DBs created 6")