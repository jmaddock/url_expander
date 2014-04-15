import sys, simplejson, time
from celery import group, signature
from load_tweets import process_tweet
from connection import dbConnection

def load_tweets_from_stdin():
    tweet_queue = []
    for line in sys.stdin:
        line = line.strip()
        try:
            tweet = simplejson.loads(line)
            tweet_queue.append(tweet)
        except ValueError as e:
            print e,line

    return tweet_queue

def load_tweets_from_mongo(db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    tweets = db.m_connections.find({'counts.urls':{'$gte':1}})
    tweet_queue = [x for x in tweets]
    return tweet_queue


def working_main():
    tweet_queue = load_tweets()
    result = [process_tweet.delay(tweet) for tweet in tweet_queue]
    done = False
    while done is False:
        done = True
        for x in result:
            if x.ready() is False:
                done = False
            else:
                try:
                    tweet = x.get()
                    print tweet['id']
                except: #fix this
                    pass

def threaded_expand(db='False'):
    tweet_queue = load_tweets_from_stdin()
    result = group(process_tweet.s(tweet) for tweet in tweet_queue)()
    while not result.ready():
        time.sleep(15)
    if result.successful():
        for x in result.get():
            if not 'info' in x:
                print x['entities']['urls']

if __name__ == '__main__':
    threaded_expand()
