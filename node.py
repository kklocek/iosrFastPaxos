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

    is_fast_round = None
    nodes_count = None

    # accepted proposed_id - after accepted! msg
    accepted_id = None
    # after accept msg
    proposed_id = None

    accepted = {}

    # value: count - based on accept messages
    # TODO question: is quorum id always for proposal id?
    quorums = []
    # classic round - 1/2 + s1
    #fast round 3/4 + 1
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
            elif msg_body['command'] == 'accept':   # P2a   client has sent request
                self._handle_accept(msg_body)
            elif msg_body['comand'] == 'accept_to_coordinator' and self.is_coordinator: # P2b   node has sent  his value
                self._handle_accept_to_coordinator(msg_body)
            elif msg_body['command'] == 'accepted': # P2b   coordinator has chosen value
                self._handle_accepted(msg_body)
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)

    def _handle_accept(self, msg_body):
        key = msg_body['key']
        if (not key in self.accepted) or self._check_id(key, msg_body['id']):
            proposal = {'proposed_id': msg_body['id'], 'proposed_value': msg_body['value']}
            self.accepted[key] = proposal
            self._send_proposal_accepted(key)
        else:
            self._send_proposal_not_accepted(key, msg_body['id'])
            self.logger.info(self.node_id + ' received outdated proposal: ' + (msg_body['id']))

    def _handle_accept_to_coordinator(self, msg_body):
        if self._is_in_quorums(msg_body):
            self._increment_quorum(msg_body)
        else:
            self.quorums.append[{'key': msg_body['key'], 'id': msg_body['id'], \
                'acceptedCount': 1}]
        self._check_quorums()

    def _is_in_quorums(self, msg_body):
        for quorum in self.quorums:
            if quorum['key'] == msg_body['key'] and quorum['id'] == msg_body['id']:
                return True
        return False

    def _increment_quorum(self, msg_body):
        for quorum in self.quorums:
            if quorum['key'] == msg_body['key'] and quorum['id'] == msg_body['id']:
                quorum['acceptedCount'] += 1

    def _handle_accepted(self, msg_body):
        key = msg_body['key']
        if key in self.accepted and msg_body['id'] == self.accepted[key]['id'] and msg_body['value'] == \
                self.accepted[key]['proposed_value']:
            self.database[key] = msg_body['value']
            del self.accepted[key]
        else:
            # What if we crash on accepted?
            pass

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
    def _check_quorums(self):
        for quorum in self.quorums:
            if quorum['acceptedCount'] >= self._calculate_quorum():
                self._send_response(quorum['id'])
                self._send_accepted()

    def _calculate_quorum(self):
        if self.is_fast_round:
            return int(3*self.nodes_count/4) + 1
        else:
            return int(self.nodes_count/2) + 1

    def _send_proposal_accepted(self, key):
        launcher = SqsLauncher(self.coordinator_address)
        launcher.launch_message({'command': 'accept_to_coordinator', 'key': key, 'id': self.accepted[key]['proposed_id']})

    def _send_proposal_not_accepted(self, key, id):
        launcher = SqsLauncher(self.coordinator_address)
        launcher.launch_message({'command': 'not_accepted', 'key': key, 'id': id})

    # coordinator response to client
    def _send_response(self, client_id):
        pass

    # commit msg to nodes
    def _send_accepted(self):
        pass