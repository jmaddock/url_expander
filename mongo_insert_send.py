import pika,sys

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='mongo_insert',durable=True)

for tweet in sys.stdin:
    channel.basic_publish(exchange='',
                          routing_key='mongo_insert',
                          body=tweet)
connection.close()
