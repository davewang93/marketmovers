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


cryptomarketcaprank = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptomarketcaprank.csv')
cryptomarketcaprank.set_index('date', inplace=True)
cryptototalvolume = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptototalvolume.csv')
cryptototalvolume.set_index('date', inplace=True)
cryptocirculatingsupply = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptocirculatingsupply.csv')
cryptocirculatingsupply.set_index('date', inplace=True)
cryptopricechange = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptopricechange.csv')
cryptopricechange.set_index('date', inplace=True)
cryptoprice = pd.read_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptoprice.csv')
cryptoprice.set_index('date', inplace=True)

cg = CoinGeckoAPI()

data = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '1'))
data2 = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '2'))
data3 = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '3'))
data4 = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '4'))

maintable = pd.concat([data,data2,data3,data4])
maintable.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptostats.csv')

trend = cg.get_search_trending()
trend = trend['coins']
trend = pd.concat([pd.DataFrame(l) for l in trend],axis=1).T
#print(trend)
#print(type(trend))
trend.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cgtrending24.csv')

#print(maintable)
#testing purposes
days = 0

marketcaprank = maintable[['name','market_cap_rank']]
marketcaprank.set_index('name',inplace=True)
marketcaprank = marketcaprank.transpose()
marketcaprank.reset_index(drop=True, inplace=True)
marketcaprank.columns.name = None
marketcaprank['date'] = date.today()-timedelta(days=days)
marketcaprank.set_index('date',inplace=True)

totalvolume = maintable[['name','total_volume']]
totalvolume.set_index('name',inplace=True)
totalvolume = totalvolume.transpose()
totalvolume.reset_index(drop=True, inplace=True)
totalvolume.columns.name = None
totalvolume['date'] = date.today()-timedelta(days=days)
totalvolume.set_index('date',inplace=True)

circulatingsupply = maintable[['name','circulating_supply']]
circulatingsupply.set_index('name',inplace=True)
circulatingsupply = circulatingsupply.transpose()
circulatingsupply.reset_index(drop=True, inplace=True)
circulatingsupply.columns.name = None
circulatingsupply['date'] = date.today()-timedelta(days=days)
circulatingsupply.set_index('date',inplace=True)

pricechange = maintable[['name','price_change_percentage_24h']]
pricechange.set_index('name',inplace=True)
pricechange = pricechange.transpose()
pricechange.reset_index(drop=True, inplace=True)
pricechange.columns.name = None
pricechange['date'] = date.today()-timedelta(days=days)
pricechange.set_index('date',inplace=True)

price = maintable[['name','current_price']]
price.set_index('name',inplace=True)
price = price.transpose()
price.reset_index(drop=True, inplace=True)
price.columns.name = None
price['date'] = date.today()-timedelta(days=days)
price.set_index('date',inplace=True)

marketcaprank = pd.concat([marketcaprank,cryptomarketcaprank],sort=True)
totalvolume = pd.concat([totalvolume,cryptototalvolume],sort=True)
circulatingsupply = pd.concat([circulatingsupply,cryptocirculatingsupply],sort=True)
pricechange = pd.concat([pricechange,cryptopricechange],sort=True)
price = pd.concat([price,cryptoprice],sort=True)

marketcaprank.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptomarketcaprank.csv')
totalvolume.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptototalvolume.csv')
circulatingsupply.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptocirculatingsupply.csv')
pricechange.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptopricechange.csv')
price.to_csv(r'D:\OneDrive\David\src\MarketMovers\CSVs\cryptoprice.csv')

print("CSVs created")
