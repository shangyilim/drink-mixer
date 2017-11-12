
"""This application demonstrates how to perform basic operations on
subscriptions with the Cloud Pub/Sub API.

For more information, see the README.md under /pubsub and the documentation
at https://cloud.google.com/pubsub/docs.
"""

import time
import json
import serial

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.policy.thread import Policy
import grpc

class UnavailableHackPolicy(Policy):
    def on_exception(self, exception):
        """
        There is issue on grpc channel that launch an UNAVAILABLE exception now and then. Until
        that issue is fixed we need to protect our consumer thread from broke.
        https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2683
        """
        unavailable = grpc.StatusCode.UNAVAILABLE
        if getattr(exception, 'code', lambda: None)() in [unavailable]:
            print("OrbitalHack! - {}".format(exception))
            return
        return super(UnavailableHackPolicy, self).on_exception(exception)
 
def send_arduino_message(ser, message):
    ser.write(message);

def create_subscription(project, topic_name, subscription_name):
    """Create a new pull subscription on the given topic."""
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project, topic_name)
    subscription_path = subscriber.subscription_path(project, subscription_name)
    
    try:
        subscription = subscriber.create_subscription(subscription_path, topic_path)
        
        print('Subscription created: {}'.format(subscription))
    except:
        print('subscription already exist. Listening to existing subscription')


def receive_messages(project, subscription_name, serial):
    """Receives messages from a pull subscription."""
    #subscriber = pubsub_v1.SubscriberClient()
    #subscription_path = subscriber.subscription_path(
    #    project, subscription_name)

    subscriber = pubsub_v1.SubscriberClient(policy_class=UnavailableHackPolicy)
    subscription_path = subscriber.subscription_path(project, subscription_name)
    
    def callback(message):
        print('Received message: {}'.format(message.data))
        json_string = str(message.data)[3:-2]
        json_string = json_string.replace('\\\\', '')
        json_obj = json.loads(json_string)
        
        print('received intent {}'.format(json_obj['intent']))
        
        ingredients = json_obj['ingredient']

        ingredientArray = []
        for ingredient in ingredients:
            ingredientCodes = '';
            if ingredient == 'sprite':
                ingredientArray.append('c1')
            if ingredient == 'apple':
                ingredientArray.append('s1')
            if ingredient == 'tea':
                ingredientArray.append('o1')
                
        seperator = ','
        serialCommand = seperator.join(ingredientArray)
        
        message.ack()
        print('subscription acknowledged')
        send_arduino_message(serial, serialCommand.encode('utf-8'))
        print('sent serial command to arduino: {}'.format(serialCommand))
        
        if json_obj['intent'] == 'clean':
            time.sleep(10)
            send_arduino_message(serial, b'c0,s0,o0,b0')
        else:
            time.sleep(20)
            send_arduino_message(serial, b'c0,s0,o0,b0')
            
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    #subscriber.subscribe(subscription_path, callback=callback)
    subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)
    
    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)



if __name__ == '__main__':

    serial = serial.Serial('/dev/ttyUSB0', 9600);
    print('connected to ttyUSB0');
    create_subscription('sodamixer-cc5ba','SodaMixerMessages','SodaMixerMessages_subscriber')
    receive_messages('sodamixer-cc5ba', 'SodaMixerMessages_subscriber', serial)


