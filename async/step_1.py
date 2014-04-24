import sys,logging
from TweetQueue import TweetQueue

def main():
    enqueue = TweetQueue(queue_name='expand_tweets')
    logging.basicConfig()

    for line in sys.stdin:
        line = line.strip()
        if line != '':
            enqueue.enqueue_tweets(line)

if __name__ == '__main__':
    main()
