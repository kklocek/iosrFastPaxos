import pytest
import mock
from pytest_mock import mocker
from .service_discovery import ServiceDiscoveryNode
from .integration import *

class SqsMock():
    def get_queue_by_name(self, QueueName):
      return QueueMock()

class QueueMock():
    def send_message(self, MessageBody):
      pass

class SqsLauncherMock():
    def __init__(self, foo):
      self.foo = foo
    def launch_message(self, msg):
      pass

def test_writing_node(mocker):
    m = mocker.patch('integration.get_sqs', lambda x: SqsLauncherMock(x))
    node = ServiceDiscoveryNode(SqsMock())
    message = {'msg': {'command': 'hello', 'node_name': 'iosrFastPaxos_discovery', 'node_address': 'iosrFastPaxos_discovery'}}

    node.on_receive(message)
    assert node.database == {'iosrFastPaxos_discovery': 'iosrFastPaxos_discovery'}

def test_wrong_message(mocker):
    m = mocker.patch('integration.get_sqs', lambda x: SqsLauncherMock(x))
    node = ServiceDiscoveryNode(SqsMock())
    node.database = {}
    message = {'msg': {'command': 'foo'}}

    node.on_receive(message)
    assert node.database == {}
