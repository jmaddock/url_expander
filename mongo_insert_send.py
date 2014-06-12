import pika,sys,simplejson

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='mongo_insert',durable=True)

for tweet in sys.stdin:
    try:
        out = simplejson.loads(tweet)
        print out['id']
    except:
        pass
    channel.basic_publish(exchange='',
                          routing_key='mongo_insert',
                          body=tweet)
connection.close()
