#! /usr/bin/python
# coding: utf-8
"""
Script to process tweets via stdin, expand URLs, add counts of urls/hashtags/mentions, and list of hashtags/mentions.

"""

import sys
from datetime import datetime, timedelta
import time
from email.utils import parsedate_tz
from pymongo import Connection
import simplejson
import re
import hashlib
import string
from collections import defaultdict
import MySQLdb
from connection import dbConnection
import pika

# Connect to mongo
db_names = ['gnip_boston','gnip_processing_errors']
db = dbConnection()
db.create_mongo_connections(mongo_options=[db_name,error_log])

# List of punct to remove from string for track keyword matching
punct = re.escape('!"$%&\'()*+,-./:;<=>?@[\\]^`{|}~')

# keep track of line numbers
#line_num = 0
tweet_total = 0

# Stuff 50k tweets into this sucker
tweets_list = []

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='mongo_insert',durable=True)

print ' [*] Waiting for messages. To exit press CTRL+C'

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt

def batch_insert(ch, method, properties, body):
    global tweets_list
    global tweet_total
    global db
    global db_name
    try:
        tweet = simplejson.loads(body)

        if len(tweets_list) == 1000:
            tweet_total += len(tweets_list)
            print "inserting 5k tweets - %i total" % tweet_total
            db.m_connections[db_name].insert(tweets_list)
            tweets_list = []
        else:
            tweets_list.append(tweet)

    except ValueError, e:
        #print "tweet not processed: %s" % (line)
        print e, body
        pass
        #except TypeError, e:
        #        #print "tweet not processed: %s" % (line)
        #        print e
        #        pass
    except KeyError, e:
        print "malformed tweet : %s" % (line)
        pass
    except OverflowError, e:
        print "malformed date in tweet: %s" % (line)
        pass
    except MySQLdb.ProgrammingError, e:
        print "error in URL: %s" % line
        pass

def single_insert(ch, method, properties, body):
    try:
        tweet = simplejson.loads(body)
        tweet['created_ts'] = to_datetime(tweet['created_at'])
        tweet['user']['created_ts'] = to_datetime(tweet['user']['created_at'])
        db.m_connections[db_name].insert(tweet)

    except ValueError, e:
        #print "tweet not processed: %s" % (line)
        print e, body
        pass
        #except TypeError, e:
        #        #print "tweet not processed: %s" % (line)
        #        print e
        #        pass
    except KeyError, e:
        print "malformed tweet : %s" % (line)
        pass
    except OverflowError, e:
        print "malformed date in tweet: %s" % (line)
        pass
    except MySQLdb.ProgrammingError, e:
        print "error in URL: %s" % line
        pass

def single_update(ch, method, properties, body):
    try:
        tweet = simplejson.loads(body)
        tweet['created_ts'] = to_datetime(tweet['created_at'])
        tweet['user']['created_ts'] = to_datetime(tweet['user']['created_at'])
        db.m_connections[db_name].update({'id':tweet['id']},
                                         {'$set':{
                                             'entities.urls':tweet['entities']['urls']}
                                      })
        #print " [x] inserted tweet ID %s" % tweet['id']

    except ValueError, e:
        #print "tweet not processed: %s" % (line)
        error = {}
        error['error'] = str(e)
        error['tweet'] = body
        db.m_connections[error_log].insert(error)
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
        db.m_connections[error_log].insert(error)
        print " [x] malformed tweet : %s" % (body)
        pass
    except OverflowError, e:
        error = {}
        error['error'] = str(e)
        error['process'] = 'insert'
        error['tweet'] = body
        db.m_connections[error_log].insert(error)
        print " [x] malformed date in tweet: %s" % (body)
        pass
    except MySQLdb.ProgrammingError, e:
        error = {}
        error['error'] = str(e)
        error['process'] = 'insert'
        error['tweet'] = body
        db.m_connections[error_log].insert(error)
        print " [x] error in URL: %s" % line
        pass


channel.basic_qos(prefetch_count=1)
channel.basic_consume(single_update,
                      queue='mongo_insert',
                      no_ack=True)

channel.start_consuming()
