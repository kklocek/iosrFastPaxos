import pytest
import mock
from service_discovery import ServiceDiscoveryNode

def test_writing_node():
    node = ServiceDiscoveryNode()
    message = {'msg': {'command': 'hello', 'node_name': 'chuck', 'node_address': 'iosrFastPaxos_discovery'}}

    node.on_receive(message)
    assert node.database == {'chuck': 'iosrFastPaxos_discovery'}

def test_wrong_message():
    node = ServiceDiscoveryNode()
    node.database = {}
    message = {'msg': {'command': 'foo'}}

    node.on_receive(message)
    assert node.database == {}
