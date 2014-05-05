import sys,logging,config_info
from TweetQueue import TweetQueue

def main():
    enqueue = TweetQueue(queue_name=config_info.expander_queue)
    logging.basicConfig()

    for line in sys.stdin:
        line = line.strip()
        if line != '':
            enqueue.enqueue_tweets(line)

if __name__ == '__main__':
    main()
