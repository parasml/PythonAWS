#--------------------------------------------------------------
 # To pull messages from SQS and send it to ECS_handler.py
 #------------------------
import os
import boto3
import json
from botocore.exceptions import ClientError
import base64

import config
import ECS_handler

sqs_client = boto3.client('sqs')


# To pull messages from SQS Queue ------------------------------
def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.
    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.
    :param queue_url: URL of the SQS queue to drain.
    """

    resp = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20
    )

    try:
        yield from resp['Messages']
    except KeyError:
        return

    entries = [
        {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
        for msg in resp['Messages']
    ]

    resp = sqs_client.delete_message_batch(
        QueueUrl=queue_url, Entries=entries
    )

    if len(resp['Successful']) != len(entries):
        raise RuntimeError(
            f"Failed to delete messages: entries={entries!r} resp={resp!r}"
        )


# Main function -------------------
def main():
    while True:
        try:
            for message in get_messages_from_queue(config.SQS_QUEUE_INVENTORY_URL):
                json_message = json.loads(message['Body'].replace("\'", "\""))

                #print("message = ", message)
                #print("json_message = ", json_message)
                #ECS_handler.ECS_handler(message)
                #print("Inside Inventory Main")
                ECS_handler.ECS_handler(json_message) 

        except Exception as e:
            #print("Error occured in Reading SQS Json = ", e)
            raise e


#----------------------------------------------------------
# Function Calls
#-----------------
if __name__ == "__main__":
    print("Inside Main class")
    main()