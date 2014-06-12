# database names
# database for processed tweets
tweet_db = 'expanded_boston_gnip'
# database for the url expansion cache
cache_db = 'test_url_cache'
# database for tweets that could not be inserted
processing_errors_db = 'gnip_processing_errors'

# queue names -> only used for rabbitmq, not celery
# queue name for unprocessed tweets
expander_queue = 'expand_tweets'
# queue name for unprocessed tweets waiting to be inserted
insert_queue = 'insert_tweets'

# log file names
# tweet IDs that have been processed and inserted
tweet_log = 'processed_tweets'
# tweet IDs that have been quarantined
error_log = 'unprocessed_tweets'

# key word track list
# enter the key words used for collection
track_list = ['boston','marathon','bomb','blast','explosion','watertown','mit','mitshooting']
