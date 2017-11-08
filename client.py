import json
import datetime
import boto3


nodes_queues = ['iosrFastPaxos_node1']
client_name = 'iosrFastPaxos_client1'
sqs = boto3.resource('sqs')


def get_value(key):
    msg_body = json.dumps({'command': 'read', 'key': key,
                'id': client_name})
    for queue_name in nodes_queues:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue.send_message(MessageBody=msg_body)

    read_queue = sqs.get_queue_by_name(QueueName=client_name)
    for message in read_queue.receive_messages(WaitTimeSeconds=5):
        response_body = json.loads(message.body)
        if response_body['command'] == 'read_response':
            message.delete()
            return response_body['value']


def set_value(key, value):
    msg_body = json.dumps({'command': 'accept', 'key': key, 'value': value, 
                'id': {'proposer_id': client_name, 'time': str(datetime.datetime.now())}})
    for queue_name in nodes_queues:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue.send_message(MessageBody=msg_body)
                            
    response_queue = sqs.get_queue_by_name(QueueName=client_name)
    for message in response_queue.receive_messages(WaitTimeSeconds=5):
        print("Response: ", message.body)
        message.delete()


while True:
    action = input("Enter 'g' to get value by key or 's' to save value: ")
    if action == 'g':
        key = input("Key: ")
        print("Value is: ", get_value(key))
    elif action == 's':
        key = input("Key: ")
        value = input("Value: ")
        set_value(key, value)
