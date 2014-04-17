from expand_url import Expand_Url
from urlparse import urlsplit
import re, simplejson, pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

def process_tweet(tweet_in):

    punct = re.escape('!"$%&\'()*+,-./:;<=>?@[\\]^`{|}~')
    expander = Expand_Url(db_name='url_test')
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
            result = simplejson.dumps(tweet)
            print " [x] processed tweet ID %s" % tweet['id']
            return result
        else:
            print " [x] file complete"

    except ValueError as e:
        return str(e,tweet_in)

def on_request(ch, method, props, body):
    n = body

    #print " [.] fib(%s)"  % (n,)
    response = process_tweet(n)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='rpc_queue')

print " [x] Awaiting RPC requests"
channel.start_consuming()
