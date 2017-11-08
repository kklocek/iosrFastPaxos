import pytest
import mock
from pytest_mock import mocker

from service_discovery import ServiceDiscoveryNode

class SqsMock():
    def get_queue_by_name(self, QueueName):
      return QueueMock()

class QueueMock():
    def send_message(self, MessageBody):
      pass

def test_writing_node(mocker):
    node = ServiceDiscoveryNode(SqsMock())
    message = {'msg': {'command': 'hello', 'node_name': 'chuck', 'node_address': 'iosrFastPaxos_discovery'}}

    node.on_receive(message)
    assert node.database == {'chuck': 'iosrFastPaxos_discovery'}

def test_wrong_message():
    node = ServiceDiscoveryNode(SqsMock())
    node.database = {}
    message = {'msg': {'command': 'foo'}}

    node.on_receive(message)
    assert node.database == {}
