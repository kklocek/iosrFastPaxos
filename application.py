import _thread
import logging
import logging.handlers
import os
from sqs_listener import SqsListener
from wsgiref.simple_server import make_server
from node import Node


# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Handler 
LOG_FILE = './application.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1048576, backupCount=5)
handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add Formatter to Handler
handler.setFormatter(formatter)

# add Handler to Logger
logger.addHandler(handler)


actor_ref = Node.start(os.environ.get('NODE_ID', 'iosrFastPaxos_node1'), logger=logger)


## Setting up communication

class MyListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        print('in handle')
        actor_ref.tell({'msg': body})


listener = MyListener('iosrFastPaxos_node1', error_queue='iosrFastPaxos_node1_error',
                      region_name='us-east-2', interval=0.1)

def listen_queue():
    logger.info('Waiting for messages.')
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
    logger.info("Serving on port 8000...")
    httpd.serve_forever()
