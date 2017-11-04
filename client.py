import boto3


nodes_queues = ['iosrFastPaxos_node1']
response_queue_name = 'iosrFastPaxos_client1'
sqs = boto3.resource('sqs')


def get_value(key):
    pass

def set_value(key, value):
    for queue_name in nodes_queues:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue.send_message(MessageBody={'command': 'accept', 'key': key, 'value': value, 
                                'response_queue': response_queue_name})
                            
    response_queue = sqs.get_queue_by_name(QueueName=response_queue_name)
    for message in queue.receive_messages(WaitTimeSeconds=5):
        print("Response: ", message.body)


while True:
    action = input("Enter 'g' to get value by key or 's' to save value: ")
    if action == 'g':
        key = input("Key: ")
        print("Value is: ", get_value(key))
    elif action == 's':
        key = input("Key: ")
        value = input("Value: ")
        set_value(key, value)
