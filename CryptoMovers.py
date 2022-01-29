from lib2to3.pgen2.pgen import DFAState
from pycoingecko import CoinGeckoAPI
import pandas_datareader as pdr
import pandas as pd
import numpy as np 
import os
from datetime import datetime, timedelta,date
import time

#this stuff loads the keys
directory = os.path.dirname(os.path.abspath(__file__))


cryptomarketcaprank = pd.read_csv('csvs/cryptomarketcaprank.csv')
cryptomarketcaprank.set_index('date', inplace=True)
cryptototalvolume = pd.read_csv('csvs/cryptototalvolume.csv')
cryptototalvolume.set_index('date', inplace=True)
cryptocirculatingsupply = pd.read_csv('csvs/cryptocirculatingsupply.csv')
cryptocirculatingsupply.set_index('date', inplace=True)
cryptopricechange = pd.read_csv('csvs/cryptopricechange.csv')
cryptopricechange.set_index('date', inplace=True)

cg = CoinGeckoAPI()

data = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '1'))
data2 = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '2'))
data3 = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '3'))
data4 = pd.DataFrame(cg.get_coins_markets(vs_currency = 'USD',order = 'market_cap_rank',per_page = '250',page  = '4'))

maintable = pd.concat([data,data2,data3,data4])

#print(maintable)
#maintable.to_csv('coingeckotop.csv')

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

marketcaprank = pd.concat([marketcaprank,cryptomarketcaprank],sort=True)
totalvolume = pd.concat([totalvolume,cryptototalvolume],sort=True)
circulatingsupply = pd.concat([circulatingsupply,cryptocirculatingsupply],sort=True)
pricechange = pd.concat([pricechange,cryptopricechange],sort=True)

marketcaprank.to_csv('csvs/cryptomarketcaprank.csv')
totalvolume.to_csv('csvs/cryptototalvolume.csv')
circulatingsupply.to_csv('csvs/cryptocirculatingsupply.csv')
pricechange.to_csv('csvs/cryptopricechange.csv')

print('files generated')
