import pika
import pykka
import json


## Actor definition

class Node(pykka.ThreadingActor):
    def __init__(self, database = {}):
        super(Node, self).__init__()
        self.database = database

    def on_receive(self, message):
        print 'I received: ', message
        msg_body = json.loads(message['msg'])
        if msg_body['command'] == 'set':
            self._set_value(msg_body)
        elif msg_body['command'] == 'print':
            self._print_database()

    def _print_database(self):
        print self.database

    def _set_value(self, msg_body):
        self.database[msg_body['key']] = msg_body['value']


actor_ref = Node.start()


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