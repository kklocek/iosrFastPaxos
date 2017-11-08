import pykka
from sqs_listener import SqsListener
import json
import boto3

class ServiceDiscoveryNode(pykka.ThreadingActor):
    def __init__(self, sqs, database = {}):
        super(ServiceDiscoveryNode, self).__init__()
        self.database = database
        self.sqs = sqs

    def on_receive(self, message):
        print('I received: ', message)
        msg_body = message['msg']
        if msg_body['command'] == "hello":
            self.database[msg_body['node_name']] = msg_body['node_address']
            self.broadcast_all()

    def broadcast_all(self):
        msg_body = json.dumps({"nodes" : self.database})
        for node in self.database:
            queue = self.sqs.get_queue_by_name(QueueName = self.database[node])
            queue.send_message(MessageBody = msg_body)

class ServiceDiscoveryListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        actor_ref.tell({'msg': body})

if __name__ == '__main__':
    sqs = boto3.resource('sqs')
    actor_ref = ServiceDiscoveryNode.start(sqs)

    listener = ServiceDiscoveryListener('iosrFastPaxos_discovery', error_queue='iosrFastPaxos_discovery_error', region_name='us-east-2')
    print('Waiting for messages. To exit press CTRL+C')
    listener.listen()
