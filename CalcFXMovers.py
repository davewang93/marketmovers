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

exporthost = parser.get('marketmovers','host')
exportuser = parser.get('marketmovers','user')
exportpasswd = parser.get('marketmovers','passwd')
exportdatabase = parser.get('marketmovers','database')

exportengine = parser.get('engines','marketmovers')

importhost = parser.get('pricedb','host')
importuser = parser.get('pricedb','user')
importpasswd = parser.get('pricedb','passwd')
importdatabase = parser.get('pricedb','database')

importengine = parser.get('engines','pricedbengine')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = importhost,
    user = importuser,
    passwd = importpasswd,
    database = importdatabase,
)

#connect to db using sqlalchemy
exportengine = create_engine(exportengine)
importengine = create_engine(importengine)

prevdate = 1
curdate = 0
numberofmovers = 10

closedata = pd.read_sql('SELECT * FROM fxpricemaintable ORDER BY date DESC', importengine)
closedata = closedata.drop('date',1)
closecolumns = closedata.columns
closedata[closecolumns] = closedata[closecolumns].apply(pd.to_numeric, errors='coerce')
closingrangetable = ((closedata.iloc[curdate] - closedata.iloc[prevdate])/closedata.iloc[[prevdate]]*100)
closingrangetable = closingrangetable.transpose()
closingrangetable.index.names = ['ticker']
closingrangetable.rename(columns={ closingrangetable.columns[0]: "pct change" }, inplace = True)
closingrangetablegainers = closingrangetable.nlargest(numberofmovers,'pct change')
closingrangetablelosers = closingrangetable.nsmallest(numberofmovers,'pct change')
closingrangetable = pd.concat([closingrangetablegainers, closingrangetablelosers])
closingrangetable.reset_index(inplace = True)
closingrangetable.to_sql('dailyfxmoversclose', exportengine, if_exists='replace')

