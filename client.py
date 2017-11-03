from sqs_launcher import SqsLauncher
import time


launcher = SqsLauncher('iosrFastPaxos_node1')

launcher.launch_message({'command': 'set', 'key': 'message', 'value': 'Hello World!'})

print(" [x] Sent 'Hello World!'")

time.sleep(5)

launcher.launch_message({'command': 'print'})