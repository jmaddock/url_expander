from celery import Celery
from expand_url import Expand_Url
from urlparse import urlsplit
import re, simplejson

app = Celery('load_tweets', backend='amqp', broker='amqp://', )

@app.task(ignore_result=False)
def process_tweet(tweet_in):
    punct = re.escape('!"$%&\'()*+,-./:;<=>?@[\\]^`{|}~')
    expander = Expand_Url(db_name='url_test')
    tweet = tweet_in
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
                #tweet['track_kw'] = {}
                #tweet['track_kw']['hashtags'] = list(set(tweet['hashtags']).intersection(track_set))
                #tweet['track_kw']['mentions'] = list(set(tweet['mentions']).intersection(track_set))
                tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                tweet_text = tweet_text.lower().split()
                #tweet['track_kw']['text'] = list(set(tweet_text).intersection(track_set))

                # Convert dates
                #tweet['created_ts'] = to_datetime(tweet['created_at'])
                #tweet['user']['created_ts'] = to_datetime(tweet['user']['created_at'])

                # Print tweet as JSON to stdout
                #print tweet['text'],tweet['entities']['urls']
    #result = simplejson.dumps(tweet)
    return tweet
