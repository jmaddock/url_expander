import pika,time

class TweetQueue(object):
    def __init__(self,queue_name):
        self.queue_name=queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        self.connection.add_backpressure_callback(self.backpressure)

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name,
                                   durable=True)

    def backpressure(self):
        print ' [x] BACKPRESSURE WARNING'
        time.wait(15)

    def enqueue_tweets(self,n):
        if n != None:
            message = n.encode('utf-8')
        else:
            message = n
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=str(message),
                                   properties=pika.BasicProperties(
                                       delivery_mode = 2, # make message persistent
                                   ))

    def dequeue_tweets(self,n):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(n,
                                   queue=self.queue_name,
                                   no_ack=False)
        self.channel.start_consuming()

    def check_queue_size(self):
        self.channel.queue_declare(self._on_callback,
                                   queue=self.queue_name,
                                   durable=True,
                                   passive=True)
