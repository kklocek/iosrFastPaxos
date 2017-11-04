import pykka
import json
import datetime
from sqs_listener import SqsListener


## Actor definition

class Node(pykka.ThreadingActor):

    node_id = None
    is_coordinator = None
    coordinator_address = None

    # accepted proposed_id - after accepted! msg
    accepted_id = None
    # after accept msg
    proposed_id = None
    # commited value
    accepted_value = None
    # value to be commited
    proposed_value = None

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
        if self._check_id(msg_body['id']):
            self.proposed_id = msg_body['id']
            self._send_any_accepted()
        else:
            self._send_any_not_accepted()

    def _handle_proposal(self, msg_body):
        if self._check_id(msg_body['id']):
            self.proposed_id = msg_body['id']
            self.proposed_value = msg_body['value']
            self._send_proposal_accepted()
        else:
            self._send_proposal_not_accepted()
            print(self.node_id, ' received outdated proposal: ', (msg_body['id']))

    def _handle_accepted(self, msg_body):
        self.accepted_id = msg_body['id']
        self.accepted_value = msg_body['value']

    def _create_id(self):
        new_id = {}
        time_stamp = str(datetime.datetime.now())
        new_id['time'] = time_stamp
        new_id['id'] = self.node_id
        return json.dump(new_id)

    def _check_id(self, id):
        if self.proposed_id['time'] < id['time']:
            return True
        elif self.proposed_id['time'] > id['time']:
            return False
        #TODO what if same time

    def _print_database(self):
        print(self.database)

    # TODO send messages to coordinator
    def _send_any_accepted(self):
        pass

    def _send_any_not_accepted(self):
        pass

    def _send_proposal_accepted(self):
        pass

    def _send_proposal_not_accepted(self):
        pass


actor_ref = Node.start()


## Setting up communication

class MyListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        actor_ref.tell({'msg': body})


listener = MyListener('iosrFastPaxos_node1', error_queue='iosrFastPaxos_node1_error',
                      region_name='us-east-2')

print('Waiting for messages. To exit press CTRL+C')
listener.listen()
