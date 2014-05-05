from datetime import datetime, timedelta
import time
from email.utils import parsedate_tz
import simplejson
import re
import hashlib
import string
from collections import defaultdict
import MySQLdb
from connection import dbConnection
import sys,logging,pika
from TweetQueue import TweetQueue
import config_info

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt

def single_update(ch, method, properties, body):
    db_name = [config_info.tweet_db,config_info.processing_errors_db]
    db = dbConnection()
    db.create_mongo_connections(mongo_options=db_name)
    try:
        tweet = simplejson.loads(body)
        tweet['created_ts'] = to_datetime(tweet['created_at'])
        tweet['user']['created_ts'] = to_datetime(tweet['user']['created_at'])
        db.m_connections[db_name[0]].update({'id':tweet['id']},
                                            {'$set':{
                                                'entities.urls':tweet['entities']['urls']}
                                         })
        print " [x] inserted tweet ID %s" % tweet['id']

    except ValueError, e:
        #print "tweet not processed: %s" % (line)
        error = {}
        error['error'] = str(e)
        error['tweet'] = body
        db.m_connections[db_name[1]].insert(error)
        print ' [x] %s : %s' % (e, body)
        pass
        #except TypeError, e:
        #        #print "tweet not processed: %s" % (line)
        #        print e
        #        pass
    except KeyError, e:
        error = {}
        error['error'] =str(e)
        error['process'] = 'insert'
        error['tweet'] = body
        db.m_connections[db_name[1]].insert(error)
        print " [x] malformed tweet : %s" % (body)
        pass
    except OverflowError, e:
        error = {}
        error['error'] = str(e)
        error['process'] = 'insert'
        error['tweet'] = body
        db.m_connections[db_name[1]].insert(error)
        print " [x] malformed date in tweet: %s" % (body)
        pass
    except MySQLdb.ProgrammingError, e:
        error = {}
        error['error'] = str(e)
        error['process'] = 'insert'
        error['tweet'] = body
        db.m_connections[db_name[1]].insert(error)
        print " [x] error in URL: %s" % line
        pass

    ch.basic_ack(delivery_tag = method.delivery_tag)

def main():
    # Connect to mongo

    punct = re.escape('!"$%&\'()*+,-./:;<=>?@[\\]^`{|}~')

    dequeue = TweetQueue(queue_name=config_info.insert_queue)
    logging.basicConfig()

    dequeue.dequeue_tweets(n=single_update)

if __name__ == '__main__':
    main()
