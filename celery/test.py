import sys, timeit, config_info
from celery import group
from tasks import process,insert
from billiard.exceptions import TimeLimitExceeded

def process_tweets(tweet_list):
    processed = group(process.s(x) for x in tweet_list)()
    return processed.get()

def insert_tweets(tweet_list):
    insert.delay(tweet_list)

def main():
    tweet_list = []
    error_log = open(config_info.error_log,'a')

    for line in sys.stdin:
        line = line.strip()
        if line != '':
            tweet_list.append(line)
            if len(tweet_list) >= 1000 or line == None:
                try:
                    processed_tweets = process_tweets(tweet_list)
                    insert_tweets(processed_tweets)
                except httplib.IncompleteRead as e:
                    for x in tweet_list:
                        tweet = simplejson.loads(tweet_in)
                        error_log.write(tweet['text'])

                tweet_list = []

if __name__ == '__main__':
    main()
