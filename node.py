import json
import datetime
import operator
import traceback
import pykka
from sqs_launcher import SqsLauncher

## Actor definition

class Node(pykka.ThreadingActor):
    node_id = None
    is_coordinator = None
    coordinator_address = 'iosrFastPaxos_client1'

    # accepted proposed_id - after accepted! msg
    accepted_id = None
    # after accept msg
    proposed_id = None

    accepted = {}

    # value: count - based on accept messages
    # TODO question: is quorum id always for proposal id?
    quorum = {}
    minimal_quorum = None

    def __init__(self, id, database={}, logger=None):
        super(Node, self).__init__()
        self.node_id = id
        self.database = database
        self.logger = logger

    def on_receive(self, message):
        try:
            self.logger.info(self.node_id + ' received: ' + str(message))
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
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)

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
            self.logger.info(self.node_id + ' received outdated proposal: ' + (msg_body['id']))

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
        self.logger.info(self.database)

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
        launcher.launch_message({'command': 'accepted', 'key': key, 'id': self.accepted[key]['proposed_id']})

    def _send_proposal_not_accepted(self, key, id):
        launcher = SqsLauncher(self.coordinator_address)
        launcher.launch_message({'command': 'not_accepted', 'key': key, 'id': id})