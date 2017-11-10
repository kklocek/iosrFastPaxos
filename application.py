import _thread
import os
from sqs_listener import SqsListener
from wsgiref.simple_server import make_server
from node import Node


node_id = os.environ.get('NODE_ID', 'iosrFastPaxos_node1')

print("Node id: ", node_id)

actor_ref = Node.start(node_id)


## Setting up communication

class MyListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        print('in handle')
        actor_ref.tell({'msg': body})


listener = MyListener(node_id, error_queue=node_id + '_error',
                      region_name='us-east-2', interval=0.1)

def listen_queue():
    print('Waiting for messages.')
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
    print("Serving on port 8000...")
    httpd.serve_forever()