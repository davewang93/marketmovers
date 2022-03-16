from lib2to3.pgen2.pgen import DFAState
from pycoingecko import CoinGeckoAPI
import pandas_datareader as pdr
import pandas as pd
import numpy as np 
import os
from datetime import datetime, timedelta,date
from configparser import ConfigParser 


directory = os.path.dirname(os.path.abspath(__file__))
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

host = parser.get('marketmovers','host')
user = parser.get('marketmovers','user')
passwd = parser.get('marketmovers','passwd')
database = parser.get('marketmovers','database')

engine = parser.get('engines','marketmovers')

cg = CoinGeckoAPI()


trend = cg.get_search_trending()
trend = trend['coins']
trend = pd.concat([pd.DataFrame(l) for l in trend],axis=1).T
#trend = trend.values()
print(trend)
print(type(trend))
trend.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\trending24.csv')