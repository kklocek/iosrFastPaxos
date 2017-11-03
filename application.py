import pykka
from sqs_listener import SqsListener


## Actor definition

class Node(pykka.ThreadingActor):
    def __init__(self, database = {}):
        super(Node, self).__init__()
        self.database = database

    def on_receive(self, message):
        print('I received: ', message)
        msg_body = message['msg']
        if msg_body['command'] == 'set':
            self._set_value(msg_body)
        elif msg_body['command'] == 'print':
            self._print_database()

    def _print_database(self):
        print(self.database)

    def _set_value(self, msg_body):
        self.database[msg_body['key']] = msg_body['value']


actor_ref = Node.start()


## Setting up communication

class MyListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        actor_ref.tell({'msg': body})

listener = MyListener('iosrFastPaxos_node1', error_queue='iosrFastPaxos_node1_error', 
    region_name='us-east-2')

print('Waiting for messages. To exit press CTRL+C')
listener.listen()
