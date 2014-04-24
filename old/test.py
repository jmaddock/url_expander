from expand_url import Expand_Url

URLs = ['http://www.ebay.com', 'http://somelab.net/foo', 'http://uw.edu/foo','http://seattle.somelab.net/test.txt', 'http://somelab.net']

test = Expand_Url(db_name='url_test')

for x in URLs:
    print test.check_cache(x)
