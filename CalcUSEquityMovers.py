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
numberofmovers = 25

closedata = pd.read_sql('SELECT * FROM usequityclosetable ORDER BY date DESC', engine)
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
closingrangetable.to_sql('995dailyusequitymoversclose', engine, if_exists='replace')

volumedata = pd.read_sql('SELECT * FROM usequityvolumetable ORDER BY date DESC', engine)
volumedata = volumedata.drop('date',1)
volumecolumns = volumedata.columns
volumedata[volumecolumns] = volumedata[volumecolumns].apply(pd.to_numeric, errors='coerce')
volumerangetable = ((volumedata.iloc[prevdate] - volumedata.iloc[curdate])/volumedata.iloc[[prevdate]]*100)
volumerangetable = volumerangetable.transpose()
volumerangetable.index.names = ['ticker']
volumerangetable.rename(columns={ volumerangetable.columns[0]: "pct change" }, inplace = True)
volumerangetablegainers = volumerangetable.nlargest(numberofmovers,'pct change')
volumerangetablelosers = volumerangetable.nsmallest(numberofmovers,'pct change')
volumerangetable = pd.concat([volumerangetablegainers, volumerangetablelosers])
volumerangetable.reset_index(inplace = True)
volumerangetable.to_sql('995dailyusequitymoversvolume', engine, if_exists='replace')

intradaydata = pd.read_sql('SELECT * FROM usequityintradaytable ORDER BY date DESC Limit 1', engine)
intradaydata = intradaydata.drop('date',1)
intradaycolumns = intradaydata.columns
intradaydata[intradaycolumns] = intradaydata[intradaycolumns].apply(pd.to_numeric, errors='coerce')
intradaydata = intradaydata*100
intradaydata = intradaydata.transpose()
intradaydata.index.names = ['ticker']
intradaydata.rename(columns={ intradaydata.columns[0]: "pct change" }, inplace = True)
intradayrangetablegainers = intradaydata.nlargest(numberofmovers,'pct change')
intradayrangetablelosers = intradaydata.nsmallest(numberofmovers,'pct change')
intradaydata = pd.concat([intradayrangetablegainers, intradayrangetablelosers])
intradaydata.reset_index(inplace = True)
intradaydata.to_sql('995dailyusequitymoversintraday', engine, if_exists='replace')

opendata = pd.read_sql('SELECT * FROM usequityopentable ORDER BY date DESC', engine)
opendata = opendata.drop('date',1)
opencolumns = opendata.columns
opendata[opencolumns] = opendata[opencolumns].apply(pd.to_numeric, errors='coerce')
overnightrangetable = ((opendata.iloc[curdate] - closedata.iloc[prevdate])/closedata.iloc[[prevdate]]*100)
overnightrangetable = overnightrangetable.transpose()
overnightrangetable.index.names = ['ticker']
overnightrangetable.rename(columns={ overnightrangetable.columns[0]: "pct change" }, inplace = True)
overnightrangetablegainers = overnightrangetable.nlargest(numberofmovers,'pct change')
overnightrangetablelosers = overnightrangetable.nsmallest(numberofmovers,'pct change')
overnightrangetable = pd.concat([overnightrangetablegainers, overnightrangetablelosers])
overnightrangetable.reset_index(inplace = True)
overnightrangetable.to_sql('995dailyusequitymoversovernight', engine, if_exists='replace')
