import urllib2
from urlparse import urlparse
from bs4 import BeautifulSoup

URL = 'http://t.co/EzlKnfAeUi'
URL2 = 'http://www.adfadfasdf.com'

def expand(url):
    url_data = {'url':url}

    try:
        response = urllib2.urlopen(url)

    except urllib2.HTTPError as e:
        print 'The server couldn\'t fulfill the request.'
        print 'Error code: ', e.code
        return e.code

    except urllib2.URLError as e:
        print 'We failed to reach a server.'
        print 'Reason: ', e.reason
        return e.reason

    else:
        url_data['long-url'] = response.url
        url_data['domain'] = urlparse(url_data['long-url']).netloc

        soup = BeautifulSoup(response)
        url_data['title'] = soup.title.string
        url_data['meta-description'] = soup.find("meta", {"name":"description"})['content']
        url_data['meta-keywords'] = soup.find("meta", {"name":"keywords"})['content']

        return url_data

def main():
    print expand(url=URL)
    print expand(url=URL2)

if __name__ == '__main__':
    main()
