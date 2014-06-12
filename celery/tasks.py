from celery import Celery
from expand_url import Expand_Url
from urlparse import urlsplit
from datetime import datetime,timedelta
from email.utils import parsedate_tz
from connection import dbConnection
import re, simplejson, time, config_info, sys, hashlib

app = Celery('tasks', backend='amqp', broker='amqp://')
#app.conf.CELERY_RESULTS_BACKEND = 'amqp'

@app.task(ignore_result=False)
def process(tweet_in):
    log = open(config_info.tweet_log,'a')
    # who is expanding urls on our server
    expander = Expand_Url(db_name=config_info.cache_db)

    # List of punct to remove from string for track keyword matching
    punct = re.escape('!"$%&\'()*+,-./:;<=>?@[\\]^`{|}~')

    # List of words we are tracking
    track_list = config_info.track_list
    track_set = set(track_list)

    try:
        """ load our tweet stripping it while we do """
        tweet = simplejson.loads(tweet_in.strip())

        """ let's process! """
        try :

            if type(tweet['entities']) is not dict:
                raise KeyError

            # Insert Counts
            tweet['counts'] = {
                'urls': len(tweet['entities']['urls']),
                'hashtags': len(tweet['entities']['hashtags']),
                'user_mentions': len(tweet['entities']['user_mentions'])
                };

            """ Insert list of hashtags and mentions
            this is used to calculate our analytics
            later on """
            tweet['hashtags'] = [tag['text'].lower() for tag in tweet['entities']['hashtags']]
            tweet['mentions'] = [mention['screen_name'].lower() for mention in tweet['entities']['user_mentions']]

            tweet['hashtags'].sort()
            tweet['mentions'].sort()

            tweet['text_hash'] = hashlib.md5(tweet['text'].encode("utf-8")).hexdigest()

            # Check to see if we have a retweet
            if tweet.has_key("retweeted_status") and tweet['truncated']== True:
                # Track rule matches
                tweet['track_kw'] = {}

                rt_hashtags = [
                        tag['text'].lower()
                        for tag in tweet['retweeted_status']['entities']['hashtags']
                    ]
                rt_mentions = [
                        mention['screen_name'].lower()
                        for mention in tweet['retweeted_status']['entities']['user_mentions']
                    ]

                untion_hashtags = set(tweet['hashtags']).union(set(rt_hashtags))
                untion_mentions = set(tweet['mentions']).union(set(rt_hashtags))
                tweet['track_kw']['hashtags'] = list(untion_hashtags.intersection(track_set))
                tweet['track_kw']['mentions'] = list(untion_mentions.intersection(track_set))
                tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                rt_text = re.sub('[%s]' % punct, ' ', tweet['retweeted_status']['text'])
                tweet_text = tweet_text.lower().split()
                rt_text = rt_text.lower().split()
                union_text = set(rt_text).union(set(tweet_text))
                tweet['track_kw']['text'] = list(union_text.intersection(track_set))

            else:
                # Track rule matches
                tweet['track_kw'] = {}
                tweet['track_kw']['hashtags'] = list(set(tweet['hashtags']).intersection(track_set))
                tweet['track_kw']['mentions'] = list(set(tweet['mentions']).intersection(track_set))
                tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                tweet_text = tweet_text.lower().split()
                tweet['track_kw']['text'] = list(set(tweet_text).intersection(track_set))

            """ url expansion """
            tweet['entities']['urls'] = map(expander.check_cache, tweet['entities']['urls'])

        except KeyError :
            return None
        #    """ no entities or is not a dict"""
        #    pass

        # Print tweet as JSON to stdout
        if tweet.has_key('text'):
            log.write(str(tweet['id']) + '\n')
            return simplejson.dumps(tweet,ensure_ascii=False).encode('utf-8')

        else:
            return None

    # Looks like the line wasn't valid JSON
    except ValueError:
        print "tweet not processed: line"
        return None

@app.task(ignore_result=True)
def insert(tweet_list):
    db_name = [config_info.tweet_db,config_info.processing_errors_db]
    db = dbConnection()
    db.create_mongo_connections(mongo_options=db_name)

    insert_queue = []

    for tweet_in in tweet_list:
        if tweet_in is not None:
            try:
                tweet = simplejson.loads(tweet_in)
                #f = open(config_info.tweet_log, "a")
                tweet['created_ts'] = to_datetime(tweet['created_at'])
                tweet['user']['created_ts'] = to_datetime(tweet['user']['created_at'])
                insert_queue.append(tweet)
                #f.write(tweet_in + '\n')

            except ValueError, e:
                #print "tweet not processed: %s" % (line)
                error = {}
                error['error'] = str(e)
                error['tweet'] = tweet
                db.m_connections[db_name[1]].insert(error)
                print ' [x] %s : %s' % (e, tweet)
                pass
                #except TypeError, e:
                #        #print "tweet not processed: %s" % (line)
                #        print e
                #        pass
            except KeyError, e:
                error = {}
                error['error'] =str(e)
                error['process'] = 'insert'
                error['tweet'] = tweet
                db.m_connections[db_name[1]].insert(error)
                print " [x] malformed tweet : %s" % (tweet)
                pass
            except OverflowError, e:
                error = {}
                error['error'] = str(e)
                error['process'] = 'insert'
                error['tweet'] = tweet
                db.m_connections[db_name[1]].insert(error)
                print " [x] malformed date in tweet: %s" % (tweet)
                pass

    log = open(config_info.tweet_log,'a')
    log.write('inserted %d tweets' % len(insert_queue))
    db.m_connections[db_name[0]].insert(insert_queue)

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt

def test():
    for line in sys.stdin:
        process(line)

if __name__ == '__main__':
    test()
