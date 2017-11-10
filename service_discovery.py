import pykka
from sqs_listener import SqsListener
import json
import _thread
import boto3
from sqs_launcher import SqsLauncher
from wsgiref.simple_server import make_server

class ServiceDiscoveryNode(pykka.ThreadingActor):

    alive = {}
    counter = 0

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
        elif msg_body['command'] == "get_nodes":
            response = {'command': 'service_discovery', 'nodes' : self.database}
            print("send msg to " + msg_body['id'])
            launcher = SqsLauncher(msg_body['id'])
            launcher.launch_message(response)
        elif msg_body['command'] == 'alive':
            self.counter += 1
            if msg_body['coordinator']:
                self.alive[msg_body['node_id']] = 1
            else:
                self.alive[msg_body['node_id']] = 0
            self._check_counter()

    def broadcast_all(self):
        msg_body = {'command': 'service_discovery', 'nodes' : self.database}
        for node in self.database:
            print("send msg to " + node)
            launcher = SqsLauncher(node)
            launcher.launch_message(msg_body)

    def _check_counter(self):
        if self.counter > ((3*len(self.database))/2):
            if 1 not in self.alive.values():
                self._init_recovery()
            self.alive.clear()
            self.counter = 0

    def _init_recovery(self):
        new_coordinator_address = sorted(self.alive)[0][0]
        msg_body = {'command': 'new_coordinator', 'node_id': new_coordinator_address}
        self.broadcast_new_coordinator(msg_body)

    def broadcast_new_coordinator(self, msg_body):
        for node in self.database:
            print("send new coordinator to " + node)
            launcher = SqsLauncher(node)
            launcher.launch_message(msg_body)


sqs = boto3.resource('sqs')
actor_ref = ServiceDiscoveryNode.start(sqs)

class ServiceDiscoveryListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        actor_ref.tell({'msg': body})

listener = ServiceDiscoveryListener('iosrFastPaxos_discovery', 
    error_queue='iosrFastPaxos_discovery_error', region_name='us-east-2', interval=0.2)


def listen_queue():
    listener.listen()


def application(environ, start_response):
    response = 'welcome'
    status = '200 OK'
    headers = [('Content-type', 'text/plain')]
    start_response(status, headers)
    return [response]


_thread.start_new_thread(listen_queue, ())

if __name__ == "__main__":
    httpd = make_server('', 8000, application)
    httpd.serve_forever()
    
