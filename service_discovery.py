import pykka
from sqs_listener import SqsListener
import json
import _thread
import boto3
from sqs_launcher import SqsLauncher
from wsgiref.simple_server import make_server

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
        elif msg_body['command'] == "get_nodes":
            response = {'command': 'service_discovery', 'nodes' : self.database}
            print("send msg to " + msg_body['id'])
            launcher = SqsLauncher(msg_body['id'])
            launcher.launch_message(response)

    def broadcast_all(self):
        msg_body = {'command': 'service_discovery', 'nodes' : self.database}
        for node in self.database:
            print("send msg to " + node)
            launcher = SqsLauncher(node)
            launcher.launch_message(msg_body)

sqs = boto3.resource('sqs')
actor_ref = ServiceDiscoveryNode.start(sqs)

class ServiceDiscoveryListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        actor_ref.tell({'msg': body})

listener = ServiceDiscoveryListener('iosrFastPaxos_discovery', 
    error_queue='iosrFastPaxos_discovery_error', region_name='us-east-2', interval=1)


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
    
