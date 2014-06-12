import pika,time

# Queue Class for Url Expander using Rabbitmq

class TweetQueue(object):

    # create queue handle object connected to a single message queue
    # pre: message queue name
    def __init__(self,queue_name):
        self.queue_name=queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        self.connection.add_backpressure_callback(self.backpressure)

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name,
                                   durable=True)

    # wait 15 seconds after receiving a backpressure callback to avoid memory
    # overflow from full queue
    def backpressure(self):
        print ' [x] BACKPRESSURE WARNING'
        time.wait(15)

    # insert tweet into the queue
    # pre: tweet of type string
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

    # consume from the queue until canceled,
    # pre: method reference to worker method
    def dequeue_tweets(self,n):
        self.channel.basic_qos(prefetch_count=100) # adjust for network latency
        self.channel.basic_consume(n,
                                   queue=self.queue_name,
                                   no_ack=False) # task completion acknoledgement
        self.channel.start_consuming()

    # check the size of the queue
    # NOT COMPLETE
    def check_queue_size(self):
        self.channel.queue_declare(self._on_callback,
                                   queue=self.queue_name,
                                   durable=True,
                                   passive=True)
