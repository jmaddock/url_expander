import pika

class TweetQueue(object):
    def __init__(self,queue_name):
        self.queue_name=queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name,durable=True)

    def enqueue_tweets(self,n):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=str(n),
                                   properties=pika.BasicProperties(
                                       delivery_mode = 2, # make message persistent
                                   ))

    def dequeue_tweets(self,n):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(n,
                              queue=self.queue_name,
                                   no_ack=False)
        self.channel.start_consuming()
