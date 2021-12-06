from bs4 import BeautifulSoup
import requests
import geopandas as gpd
import pandas as pd
from geopandas.tools import geocode
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import types


class ZillowScraper():
    results = []
    city = 'philadelphia'
    #headers = requests.utils.default_headers()
    #headers.update({
    #'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
    #})
    headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
           'accept-encoding': 'gzip, deflate, br',
           'accept-language': 'en-US,en;q=0.8',
           'upgrade-insecure-requests': '1',
           'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
        }

    def fetch(self, url, params):
        print('HTTP GET request to URL: %s' % url, end='')
        res = requests.get(url, params=params, headers=self.headers)
        print(' | Status code: %s' % res.status_code)
        return res    

    def parse(self, html):
        content = BeautifulSoup(html, 'lxml')
        cards = content.findAll('article', {'class': 'list-card'})
        for card in cards:
            try:
                addr = card.find('address', {'class': 'list-card-addr'}).text
            except:
                addr = ''
            try: 
                bds = card.find('ul', {'class': 'list-card-details'}).findAll('li')[0].text.split(' ')[0]
            except:
                bds = ''
            try:
                ba = card.find('ul', {'class': 'list-card-details'}).findAll('li')[1].text.split(' ')[0]
            except:
                ba = ''
            try: 
                price = card.find('div', {'class': 'list-card-price'}).text
            except:
                price = ''          
            try:
                sqft = card.find('ul', {'class': 'list-card-details'}).findAll('li')[2].text.split(' ')[0]
            except:
                sqft = ''
            try:
                image = card.find('img', {'class':""}).get('src')
            except:
                image = ''
            
            self.results.append({
                'price': price,
                'addr': addr,
                'beds': bds,
                'baths': ba,
                'sqft': sqft,
                'img': image
            })
    
    def to_gdf(self):
        data = pd.DataFrame.from_dict(self.results)
        data = data[data.addr != '']
        data = data.drop_duplicates()
        data['price'] = data['price'].str.replace('\W', '', regex=True)
        data['sqft'] = data['sqft'].str.replace('\W', '', regex=True)
        data['beds'] = data['beds'].str.replace('\W', '', regex=True)
        data['baths'] = data['baths'].str.replace('\W', '', regex=True) 
        data['beds'] = data['beds'].replace({'Studio':0,'studio':0})   
        #data = data.fillna(-1)
        #data['price'] = data['price'].astype(int)
        #data['sqft'] = data['sqft'].astype(int)
        #data['baths'] = data['baths'].astype(float)
        #data['beds'] = data['beds'].astype(int)
        
        # conditions = [
        #     data.addr.str.contains('#'),
        #     data.addr.str.contains('APT'),
        #     data.addr.str.contains('UNIT')
        #     ]
        
        # choices = [
        #     data.addr.apply(lambda x: x[x.find('#'):]),
        #     data.addr.apply(lambda x: x[x.find('APT'):]),
        #     data.addr.apply(lambda x: x[x.find('UNIT'):])
        #     ]
        
        # choices2 = [
        #     data.addr.apply(lambda x: x[x.find('#')]),
        #     data.addr.apply(lambda x: x[x.find('APT')]),
        #     data.addr.apply(lambda x: x[x.find('UNIT')])
        #     ]
        
        # data['apt'] = np.select(conditions, choices, default = '')
        # data['addr'] = np.select(conditions, choices2, default = data.addr)
        
        geo = geocode(data['addr'], provider='nominatim', user_agent='mikes_project', timeout=4)
        join = pd.merge(geo, data, left_index=True, right_index=True)
        join = join.drop(['address'], axis = 1)
        return join
    
    
    # def rem_apt(series):
    #     x = str(series).split(' ')
    #     ex_dict = {k:v for k,v in enumerate(x)}
    #     dict_len = len(ex_dict)
    #     rep_all_after=[]
    #     for key,value in ex_dict.items():
    #         for item in road_types:
    #             if item in value:
    #                 if item == value:
    #                     rem_key = int(key+1)
    #                     while rem_key < dict_len:
    #                         rep_all_after.append(rem_key)
    #                         rem_key= rem_key+1
    
    #     for t in rep_all_after:
    #         ex_dict.pop(t,None)
    #     return " ".join(ex_dict.values())
    
    
    def to_postgis(self, join, city):
        engine = create_engine("connection string here") 
        join.to_postgis(city, engine, if_exists='replace', dtype={'price':types.Integer, 'beds':types.Integer, 'sqft':types.Integer})
        print("execute done")
 
    def run(self):
        for page in range(1, 10):
            url = 'https://www.zillow.com/philadelphia-pa/fsbo/'
            params = {
                'searchQueryState': '{"pagination":{"currentPage": %s},"usersSearchTerm":"Philadelphia, PA","mapBounds":{"west":-75.52722412207032,"east":-74.7238488779297,"south":39.92545251238079,"north":40.06485258312806},"regionSelection":[{"regionId":13271,"regionType":6}],"isMapVisible":true,"mapZoom":11,"filterState":{"pmf":{"value":false},"sort":{"value":"globalrelevanceex"},"pf":{"value":false}},"isListVisible":true}' % page
                } 
            res = self.fetch(url, params)
            self.parse(res.text)        
        join = self.to_gdf()
        self.to_postgis(join, self.city)
    
if __name__ == '__main__':
    scraper = ZillowScraper()
    scraper.run()
    
