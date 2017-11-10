import json
import datetime
import operator
import traceback

import os
import pykka
from sqs_launcher import SqsLauncher

## Actor definition

class Node(pykka.ThreadingActor):
    node_id = None
    is_coordinator = True
    coordinator_address = 'iosrFastPaxos_node1'
    service_discovery_address = 'iosrFastPaxos_discovery'
    is_fast_round = True
    nodes_count = 1
    nodes_addresses = ['iosrFastPaxos_node1']

    accepted = {}

    # value: count - based on accept messages
    quorums = []
    read_quorums = []
    # classic round - 1/2 + s1
    #fast round 3/4 + 1
    minimal_quorum = None

    def __init__(self, id, database={}):
        super(Node, self).__init__()
        self.node_id = id
        self.database = database
        self.get_addresses()
        if self.node_id != self.coordinator_address:
            is_coordinator = False

    def get_addresses(self):
        launcher = SqsLauncher(self.service_discovery_address)
        launcher.launch_message({'command': 'hello', 'node_name': self.node_id, 'node_address': self.node_id})

    def on_receive(self, message):
        try:
            print(self.node_id + ' received: ' + str(message))
            msg_body = message['msg']
            if msg_body['command'] == 'print':
                self._print_database()
            elif msg_body['command'] == 'accept':   # P2a   client has sent request
                self._send_alive_msg()
                self._handle_accept(msg_body)
            elif msg_body['command'] == 'accept_to_coordinator' and self.is_coordinator: # P2b   node has sent  his value
                self._handle_accept_to_coordinator(msg_body)
            elif msg_body['command'] == 'accepted': # P2b   coordinator has chosen value
                self._handle_accepted(msg_body)
            elif msg_body['command'] == 'read':
                self._send_alive_msg()
                self._handle_read(msg_body)
            elif msg_body['command'] == 'read_result' and self.is_coordinator:
                self._handle_read_result(msg_body)
            elif msg_body['command'] == 'service_discovery':
                nodes = msg_body['nodes']
                self.nodes_count = len(nodes)
                self.nodes_addresses = nodes
                print(self.nodes_addresses)
            elif msg_body['command'] == 'new_coordinator':
                self.coordinator_address = msg_body['node_id']
                if msg_body['node_id'] == os.environ.get('NODE_ID', 'iosrFastPaxos_node1'):
                    self.is_coordinator = True
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)

    def _handle_accept(self, msg_body):
        key = msg_body['key']
        if (key not in self.accepted) or self._check_id(key, msg_body['id']):
            proposal = {'proposed_id': msg_body['id'], 'proposed_value': msg_body['value']}
            self.accepted[key] = proposal
            self._send_proposal_accepted(key)
        else:
            print(self.node_id + ' received outdated proposal: ' + (msg_body['id']))

    def _handle_accept_to_coordinator(self, msg_body):
        if self._is_in_quorums(msg_body):
            self._increment_quorum(msg_body)
        else:
            self.quorums.append({'key': msg_body['key'], 'id': msg_body['id'],
                'acceptedCount': 1})
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
        if key in self.accepted and msg_body['id'] == self.accepted[key]['proposed_id']:
            self.database[key] = self.accepted[key]['proposed_value']
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
        print(self.database)

    # sort quorum by value (count of accepted values from nodes)
    # then get most popular element, check if count is enough for minimal quorum, return accepted value
    def _check_quorums(self):
        for quorum in self.quorums:
            if quorum['acceptedCount'] >= self._calculate_quorum():
                self._send_response(quorum['id'])
                self._send_accepted(quorum)

    def _calculate_quorum(self):
        if self.is_fast_round:
            return int(3*self.nodes_count/4) + 1
        else:
            return int(self.nodes_count/2) + 1

    def _send_proposal_accepted(self, key):
        launcher = SqsLauncher(self.coordinator_address)
        launcher.launch_message({'command': 'accept_to_coordinator', 'key': key, 'id': self.accepted[key]['proposed_id']})

    # coordinator response to client
    def _send_response(self, id):
        launcher = SqsLauncher(id['client_id'])
        launcher.launch_message({'command': 'write_response', 'response': 'accepted', 'id': id['time']})

    # commit msg to nodes
    def _send_accepted(self, quorum):
       for node_address in self.nodes_addresses:
            launcher = SqsLauncher(node_address)
            launcher.launch_message({'command': 'accepted', 'key': quorum['key'], 'id': quorum['id']})

    def _handle_read(self, msg_body):
        key = msg_body['key']
        if key in self.database:
            launcher = SqsLauncher(self.coordinator_address)
            launcher.launch_message({'command': 'read_result', 'id': msg_body['id'], 'key': key, 'value': self.database[key]})

    def _handle_read_result(self, msg_body):
        if self._is_in_read_quorums(msg_body):
            self._increment_read_quorum(msg_body)
        else:
            self.read_quorums.append({'key': msg_body['key'], 'value': msg_body['value'], 'id': msg_body['id'], \
                'acceptedCount': 1})
        self._check_read_quorums()

    def _is_in_read_quorums(self, msg_body):
        for quorum in self.read_quorums:
            if quorum['key'] == msg_body['key'] and quorum['id'] == msg_body['id']:
                return True
        return False

    def _increment_read_quorum(self, msg_body):
         for quorum in self.read_quorums:
            if quorum['key'] == msg_body['key'] and quorum['id'] == msg_body['id']:
                quorum['acceptedCount'] += 1

    def _check_read_quorums(self):
        for quorum in self.read_quorums:
            if quorum['acceptedCount'] >= self._calculate_quorum():
                self._send_read_response(quorum['id'], quorum['key'], quorum['value'])

    def _send_read_response(self, client_id, key, value):
        launcher = SqsLauncher(client_id)
        launcher.launch_message({'command': 'read_response', 'key': key, 'value': value})

    def _send_alive_msg(self):
        launcher = SqsLauncher(self.service_discovery_address)
        launcher.launch_message(({'command': 'alive', 'node_id': os.environ.get('NODE_ID', 'iosrFastPaxos_node1'),
                                  'coordinator': self.is_coordinator}))
