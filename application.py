import operator
import json
import datetime
import _thread
import logging
import logging.handlers
import pykka
from sqs_listener import SqsListener
from sqs_launcher import SqsLauncher
from wsgiref.simple_server import make_server


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

## Actor definition

class Node(pykka.ThreadingActor):
    node_id = None
    is_coordinator = None
    coordinator_address = None

    # accepted proposed_id - after accepted! msg
    accepted_id = None
    # after accept msg
    proposed_id = None

    accepted = {}

    # value: count - based on accept messages
    # TODO question: is quorum id always for proposal id?
    quorum = {}
    minimal_quorum = None

    def __init__(self, id, database={}):
        super(Node, self).__init__()
        self.node_id = id
        self.database = database

    def on_receive(self, message):
        logger.info(self.node_id + ' received: ' + message)
        msg_body = message['msg']
        if msg_body['command'] == 'print':
            self._print_database()
        elif msg_body['command'] == 'any':  # P1a   prepare
            self._handle_any(msg_body)
        elif msg_body['command'] == 'accept':   # P2a   client has sent request
            self._handle_proposal(msg_body)
        elif msg_body['comand'] == 'accept_to_coordinator' and self.is_coordinator: # P2b   node has sent  his value
            self._handle_accept_to_coordinator(msg_body)
        elif msg_body['command'] == 'accepted': # P2b   coordinator has chosen value
            self._handle_accepted(msg_body)

    def _handle_any(self, msg_body):
        if self._check_id(msg_body['key'], msg_body['id']):
            self.proposed_id = msg_body['id']
            self._send_any_accepted()
        else:
            self._send_any_not_accepted()

    def _handle_proposal(self, msg_body):
        key = msg_body['key']
        if (not key in self.accepted) or self._check_id(key, msg_body['id']):
            proposal = {'proposed_id': msg_body['id'], 'proposed_value': msg_body['value']}
            self.accepted[key] = proposal
            self._send_proposal_accepted(key)
        else:
            self._send_proposal_not_accepted(key, msg_body['id'])
            logger.info(self.node_id + ' received outdated proposal: ' + (msg_body['id']))

    def _handle_accept_to_coordinator(self, msg_body):
        if msg_body['value'] in self.quorum.keys():
            self.quorum[msg_body['value']] +=1
        else:
            self.quorum[msg_body['value']] =1

    def _handle_accepted(self, msg_body):
        key = msg_body['key']
        if key in self.accepted and msg_body['id'] == self.accepted[key]['id'] and msg_body['value'] == \
                self.accepted[key]['value']:
            self.database[key] = msg_body['value']
            del self.accepted[key]
        else:
            # What if we crash on accepted?
            pass

    def _create_id(self):
        new_id = {}
        time_stamp = str(datetime.datetime.now())
        new_id['time'] = time_stamp
        new_id['id'] = self.node_id
        return json.dumps(new_id)

    def _check_id(self, key, id):
        if self.accepted[key]['proposed_id']['time'] < id['time']:
            return True
        elif self.accepted[key]['proposed_id']['time'] > id['time']:
            return False
            # TODO what if same time

    def _print_database(self):
        logger.info(self.database)

    # sort quorum by value (count of accepted values from nodes)
    # then get most popular element, check if count is enough for minimal quorum, return accepted value
    def _check_quorum(self):
        most_popular = sorted(self.quorum.items(), key=operator.itemgetter(1), reverse=True)[0]
        if most_popular[1] >= self.minimal_quorum:
            return most_popular[0]
        else:
            return False

    # TODO send messages to coordinator
    def _send_any_accepted(self):
        pass

    def _send_any_not_accepted(self):
        pass

    def _send_proposal_accepted(self, key):
        launcher = SqsLauncher(self.coordinator_address)
        launcher.launch_message({'command': 'accepted', 'key': key, 'id': self.accepted[key]['id']})

    def _send_proposal_not_accepted(self, key, id):
        launcher = SqsLauncher(self.coordinator_address)
        launcher.launch_message({'command': 'not_accepted', 'key': key, 'id': id})



actor_ref = Node.start('node1')


## Setting up communication

class MyListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        print('in handle')
        actor_ref.tell({'msg': body})


listener = MyListener('iosrFastPaxos_node1', error_queue='iosrFastPaxos_node1_error',
                      region_name='us-east-2')

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