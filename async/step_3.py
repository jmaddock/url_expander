import sys,logging
from TweetQueue import TweetQueue

def single_update(ch, method, properties, body):
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

def main():
    dequeue = TweetQueue(queue_name='expand_tweets')
    logging.basicConfig()

    dequeue.dequeue_tweets(n=single_update)

if __name__ == '__main__':
    main()
