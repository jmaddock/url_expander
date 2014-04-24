from expand_url import Expand_Url
from urlparse import urlsplit
from datetime import datetime,timedelta
from email.utils import parsedate_tz
from TweetQueue import TweetQueue
import re, simplejson, time, logging

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt

def process_tweet(tweet_in):
    track_list = ['boston','marathon','bomb','blast','explosion','watertown','mit','mitshooting']
    # Turn it into a set
    track_set = set(track_list)
    punct = re.escape('!"$%&\'()*+,-./:;<=>?@[\\]^`{|}~')
    expander = Expand_Url(db_name='url_cache')
    try:
        tweet = simplejson.loads(tweet_in)
        if not tweet.has_key("info"):
            print " [x] accepted tweet ID %s" % tweet['id']
            if tweet.has_key("entities"):

            # Insert Counts
                tweet['counts'] = {
                    'urls': len(tweet['entities']['urls']),
                    'hashtags': len(tweet['entities']['hashtags']),
                    'user_mentions': len(tweet['entities']['user_mentions'])
                };

                tweet['hashtags'] = []
                tweet['mentions'] = []

                # Insert list of hashtags and mentions
                for index in range(len(tweet['entities']['hashtags'])):
                    tweet['hashtags'].append(tweet['entities']['hashtags'][index]['text'].lower())
                    for index in range(len(tweet['entities']['user_mentions'])):
                        tweet['mentions'].append(tweet['entities']['user_mentions'][index]['screen_name'].lower())

                        tweet['hashtags'].sort()
                        tweet['mentions'].sort()

                # begin url expansion
                for index in range(len(tweet['entities']['urls'])):
                    ourl = tweet['entities']['urls'][index]['expanded_url']

                    # if the expanded_url field is empty, try expanding the 'url' field instead
                    if ourl is None:
                        ourl = tweet['entities']['urls'][index]['url']

                    if ourl:
                        try:
                            expanded = expander.check_cache(ourl)
                            tweet['entities']['urls'][index].update(expanded)
                            # Catch any exceptions related to URL or expanding errors
                            # and make sure we record why
                            #except (URLError, APIError, UnicodeWarning, UnicodeError) as e:
                            #	tweet['entities']['urls'][index]['expansion_error'] = e.msg;
                            # this catches errors which seem to emanate from unicode errors
                            # this should be checked on occasion to ensure it really is a unicode error
                        except KeyError as e:
                            tweet['entities']['urls'][index]['expansion_error'] = "Possible Unicode Error";
                    # end url expansion

                        # Track rule matches
                        tweet['track_kw'] = {}
                        tweet['track_kw']['hashtags'] = list(set(tweet['hashtags']).intersection(track_set))
                        tweet['track_kw']['mentions'] = list(set(tweet['mentions']).intersection(track_set))
                        tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                        tweet_text = tweet_text.lower().split()
                        tweet['track_kw']['text'] = list(set(tweet_text).intersection(track_set))

                        # Convert dates

                        # Print tweet as JSON to stdout
                        #print tweet['text'],tweet['entities']['urls']
            result = simplejson.dumps(tweet)
            return result
            #print " [x] processed tweet ID %s" % tweet['id']
        else:
            print " [x] processed %s tweets" % tweet['info']['activity_count']

    except ValueError as e:
        print ' [x] %s, %s' % (e,tweet_in)
        return '%s, %s' % (e,tweet_in)

def queue_handler(ch, method, properties, body):
    tweet = process_tweet(tweet_in=body)
    enqueue = TweetQueue(queue_name='insert_tweets')
    enqueue.enqueue_tweets(n=tweet)

def main():
    dequeue = TweetQueue(queue_name='expand_tweets')
    logging.basicConfig()

    dequeue.dequeue_tweets(n=queue_handler)

if __name__ == '__main__':
    main()
