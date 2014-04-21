from expand_url import Expand_Url
from urlparse import urlsplit
from datetime import datetime,timedelta
from email.utils import parsedate_tz
import re, simplejson, pika, time

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt

def process_tweet(tweet_in):
    track_list = ['boston','marathon','bomb','blast','explosion','watertown','mit','mitshooting']
    # Turn it into a set
    track_set = set(track_list)
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
            print " [x] processed tweet ID %s" % tweet['id']
        else:
            print " [x] file complete"

    except ValueError as e:
        return '%s, %s' % (e,tweet_in)

def on_request(ch, method, props, body):
    n = body
    response = process_tweet(n)

    if response != None:
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                         body=response.encode('utf-8'))
        ch.basic_ack(delivery_tag = method.delivery_tag)
    else:
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
