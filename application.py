import pykka
import json
import datetime
import _thread
from sqs_listener import SqsListener
from sqs_launcher import SqsLauncher
from wsgiref.simple_server import make_server


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

    def __init__(self, id, database={}):
        super(Node, self).__init__()
        self.node_id = id
        self.database = database

    def on_receive(self, message):
        print(self.node_id, ' received: ', message)
        msg_body = message['msg']
        if msg_body['command'] == 'print':
            self._print_database()
        elif msg_body['command'] == 'any':  # P1a   prepare
            self._handle_any(msg_body)
        elif msg_body['command'] == 'accept':  # P2a   client has sent request
            self._handle_proposal(msg_body)
        elif msg_body['command'] == 'accepted':  # P2b   coordinator has chosen value
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
            print(self.node_id, ' received outdated proposal: ', (msg_body['id']))

    def _handle_accepted(self, msg_body):
        key = msg_body['key']
        if key in self.accepted and msg_body['id'] == self.accepted[key]['id'] and msg_body['value'] == self.accepted[key]['value']:
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
        #TODO what if same time

    def _print_database(self):
        print(self.database)

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
        actor_ref.tell({'msg': body})


listener = MyListener('iosrFastPaxos_node1', error_queue='iosrFastPaxos_node1_error',
                      region_name='us-east-2')

def listen_queue():
    print('Waiting for messages.')
    listener.listen()


def application(environ, start_response):
    response = 'welcome'
    status = '200 OK'
    headers = [('Content-type', 'text/plain')]
    start_response(status, headers)
    return [response]


if __name__ == '__main__':
    _thread.start_new_thread(listen_queue)
    httpd = make_server('', 8000, application)
    print("Serving on port 8000...")
    httpd.serve_forever()