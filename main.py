import sys, simplejson, time
from celery import group, signature
from load_tweets import process_tweet

def load_tweets():
    tweet_queue = []
    for line in sys.stdin:
        line = line.strip()
        try:
            tweet = simplejson.loads(line)
            tweet_queue.append(tweet)
        except ValueError as e:
            print e,line

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

def dev_main():
    tweet_queue = load_tweets()
    result = group(process_tweet.s(tweet) for tweet in tweet_queue)()
    #task = group([process_tweet(tweet_queue[0]),process_tweet(tweet_queue[1]),process_tweet(tweet_queue[2])])
    #result = task.apply_async()
    if result.successful():
        for x in result.get():
            if not 'info' in x:
                print x['id']

if __name__ == '__main__':
    dev_main()
