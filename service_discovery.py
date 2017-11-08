import pykka
from sqs_listener import SqsListener
import json
import boto3

## Actor definition

sqs = boto3.resource('sqs')

class ServiceDiscoveryNode(pykka.ThreadingActor):
    def __init__(self, database = {}):
        super(ServiceDiscoveryNode, self).__init__()
        self.database = database

    def on_receive(self, message):
        print('I received: ', message)
        msg_body = message['msg']
        if msg_body['command'] == "hello":
            self.database[msg_body['node_name']] = msg_body['node_address']
            self.broadcast_all()

    def broadcast_all(self):
        msg_body = json.dumps({"nodes" : self.database})
        for node in self.database:
            queue = sqs.get_queue_by_name(QueueName = self.database[node])
            queue.send_message(MessageBody = msg_body)


actor_ref = ServiceDiscoveryNode.start()


## Setting up communication

class ServiceDiscoveryListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        actor_ref.tell({'msg': body})

listener = ServiceDiscoveryListener('iosrFastPaxos_discovery', error_queue='iosrFastPaxos_discovery_error', 
    region_name='us-east-2')

print('Waiting for messages. To exit press CTRL+C')
listener.listen()
