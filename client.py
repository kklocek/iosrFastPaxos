import pika
import time
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=json.dumps({'command': 'set', 'key': 'message', 'value': 'Hello World!'}))
                      
print " [x] Sent 'Hello World!'"

time.sleep(5)

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=json.dumps({'command': 'print'}))

connection.close()