from connection import dbConnection
import urllib2, datetime, httplib
from urlparse import urlparse
from bs4 import BeautifulSoup
from socket import error as SocketError
import bson

class Expand_Url(object):

    def __init__(self,db_name=''):
        self.db_name = db_name
        self.cache = dbConnection()
        self.cache.create_mongo_connections(mongo_options=[self.db_name]) #fix this

    def check_cache(self,short_url):
        raw_data = self.cache.m_connections[self.db_name].find_one({'short_url':short_url},
                                                                   {'_id':0})

        if raw_data == None:
            url_data = self.expand(short_url)
            try:
                self.cache.m_connections[self.db_name].insert(url_data)
            except bson.errors.InvalidStringData as e:
                pass
            url_data.pop('_id',None)
            return url_data
        else:
            return raw_data

    def expand(self,url):
        response = None
        url_data = {'short_url':url}

        expanded_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        url_data['expanded_at'] = expanded_at

        while True:
            try:
                response = urllib2.urlopen(url)
                if response.url == url:
                    break
                else:
                    url = response.url

            except urllib2.HTTPError as e:
                url_data['error'] = e.code
                return url_data

            except urllib2.URLError as e:
                url_data['error'] = str(e.args[0])
                return url_data

            except SocketError as e:
                url_data['error'] = str(e.args[0])
                return url_data # loop this

            except httplib.BadStatusLine as e:
                url_data['error'] = 'HTTP ERROR: Bad Status Line'
                return url_data

            except httplib.IncompleteRead as e:
                url_data['error'] = 'HTTP ERROR: Incomplete Read'
                return url_data

            else:
                url_data['long-url'] = response.url
                url_data['domain'] = urlparse(url_data['long-url']).netloc

                soup = BeautifulSoup(response)
                try:
                    url_data['title'] = soup.title.string
                except AttributeError as e:
                    pass
                try:
                    url_data['meta-description'] = soup.find("meta", {"name":"description"})['content']
                except TypeError as e:
                    pass
                try:
                    url_data['meta-keywords'] = soup.find("meta", {"name":"keywords"})['content']
                except TypeError as e:
                    pass

        return url_data

if __name__ == '__main__':
    test_url = 'http://t.co/ceH5odp6Y9'
    expander = Expand_Url(db_name='url_test')
    expanded = expander.check_cache(test_url)
    print expanded
