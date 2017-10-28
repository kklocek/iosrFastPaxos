import pika
import pykka


## Actor definition

class Node(pykka.ThreadingActor):
    def __init__(self, greeting='Hi there!'):
        super(Node, self).__init__()
        self.greeting = greeting

    def on_receive(self, message):
        print 'I received: ', message

actor_ref = Node.start(greeting='Hi you!')


## Setting up communication

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
    actor_ref.tell({'msg': body})

channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)

print 'Waiting for messages. To exit press CTRL+C'
channel.start_consuming()