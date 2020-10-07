#--------------------------------------------------------------------------------
# To clear our dead SQS 
#------------------------

import pandas as pd
import urllib
import os
import boto3
import json
from datetime import datetime
from dateutil.parser import parse
from botocore.exceptions import ClientError
import base64
import json

import config


class clearDeadSQS:
    def __init__(self, metaDataInfo, lsMissedKeys):
        print("Class clearDeadSQS")

        self.metaDataInfo = metaDataInfo
        self.lsMissedKeys = lsMissedKeys

        self.sqs_client = boto3.client('sqs')
        self.s3 = boto3.client('s3', region_name='us-east-1')

        lsDeteDeadLogs = self.getDeadSQSEvents()

        print("lsDeteDeadLogs --- = ", lsDeteDeadLogs)

        self.deleteSQS_Msg(lsDeteDeadLogs)  # Delete SQS, once table schema name and dbname is confirmed.

        # If still there are missing files, we will re-send it to Inventory SQS -----
        if (len(lsMissedKeys) != 0):
            self.createNewQueue()
                       


    # To pull messages from SQS Queue ------------------------------
    def get_messages_from_queue(self, queue_url):
        """Generates messages from an SQS queue.

        Note: this continues to generate messages until the queue is empty.
        Every message on the queue will be deleted.

        :param queue_url: URL of the SQS queue to drain.

        """
        
        resp = self.sqs_client.receive_message(
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

        resp = self.sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )


    # getDeadSQSEvents function -------------------
    def getDeadSQSEvents(self):

        lsDeteDeadLogs = []
        print("Inside function getDeadSQSEvents")
        try:
            for message in self.get_messages_from_queue(config.SQS_QUEUE_DEAD_INVENTORY_URL):
                json_message = json.loads(message['Body'].replace("\'", "\""))

                #print("message = ", message)
                print("json_message = ", json_message)

                bFileExits = self.checkForFile(json_message)


                # If file is in Dead SQS ---------------
                if bFileExits:
                    self.sendToInventorySQS(json_message)

                    strReceiptHandle = message['ReceiptHandle']
                    lsDeteDeadLogs.append(strReceiptHandle)


        #self.deleteSQS_Msg(strReceiptHandle)

        except Exception as e:
            print("Error occured in Reading Json = ", e)
            raise e

        return lsDeteDeadLogs
     

    # check for file in s3 first --------------
    def checkForFile(self, json_message):
        
        #print("MSG = ", json_message['Records'])
        #print("-------------------------")

        # Check for table and dbname -------------------------------------
        try:
            s3Bucket=urllib.parse.unquote_plus(record['s3']['bucket']['name'])
            s3Key=urllib.parse.unquote_plus(record['s3']['object']['key'])  # This removes blank spaces from URL-----
        except:
            print("e: Record not found in Json ------")
            return False


        s3TableName = s3Key.split('/')[-2]
        s3SchemaName = s3Key.split('/')[1]
        s3DbName = s3Key.split('/')[0]

        s3TableName = config.convertStringToLower(s3TableName)
        s3DbName = config.convertStringToLower(s3DbName)

        # check for Particular table an db Messages only--------------------------
        if self.metaDataInfo['tablename'] == s3TableName & self.metaDataInfo['schemaname'] == s3SchemaName & self.metaDataInfo['dbname'] == s3DbName:

            # Check for particular file in S3------------------------------------
            record = json_message['Records'][0]

            print("s3Bucket = ", s3Bucket)
            print("s3Key = ", s3Key)

            # Check for file in raw s3 -------------
            try:
                self.s3.head_object(Bucket=s3Bucket, Key=s3Key)
            except ClientError:
                # Not found
                print("Not found")
                return False

            # Check if the missed file is in Dead SQS------
            if s3Key in self.lsMissedKeys:

                self.lsMissedKeys.remove('s3Key')
                return True
            else:
                return False

        else:
            return False

    

    # Send dead SQS to Inventory SQS --------------
    def sendToInventorySQS(self, json_message):
        
        # Send the SQS message
        sqs_queue_url = self.sqs_client.get_queue_url(
                        QueueName=config.SQS_QUEUE_INVENTORY_URL
                        )['QueueUrl']
        try:
            msg = self.sqs_client.send_message(QueueUrl=sqs_queue_url,
                                        MessageBody=json.dumps(json_message))
        except ClientError as e:
            print("Error while sending messages to SQS")
            return None

        return msg

    # To create new SQS and send it ------------------
    def createNewQueue(self):

        for s3key in self.lsMissedKeys:

            print("s3key NEW QUEUE = ", s3key)
            json_messageNew = ''
            #self.sendToInventorySQS(json_messageNew)

            strBodyMsg = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': ''}, 'requestParameters': {'sourceIPAddress': ''}, 'responseElements': {'x-amz-request-id': '', 'x-amz-id-2': ''}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': '', 'bucket': {'name': 'sterling-datalake-raw-data-int-us-east-1', 'ownerIdentity': {'principalId': ''}, 'arn': 'arn:aws:s3:::sterling-datalake-raw-data-int-us-east-1'}, 'object': {'key': '', 'size': 1234, 'eTag': '', 'versionId': '', 'sequencer': ''}}}]}

            strBodyMsg['Records'][0]['eventTime'] = str(datetime.utcnow())
            strBodyMsg['Records'][0]['s3']['object']['key'] = s3key
            strBodyMsg['Records'][0]['s3']['object']['size'] = 10

            jsonRecord = json.dumps(strBodyMsg)

            print("jsonRecord NEW QUEUE = ", jsonRecord)

            # To send new Event ----------
            response = self.sqs_client.send_message(
                QueueUrl=config.SQS_QUEUE_INVENTORY_URL,
                MessageBody=jsonRecord)

            '''
            # To check message (For testing purpose) -------
            response = client.receive_message(
                QueueUrl=config.SQS_QUEUE_INVENTORY_URL,
            )
            print("Receive message = ", response)

            # Clear Queue -------------------------------------
            response = client.purge_queue(
                QueueUrl=config.SQS_QUEUE_INVENTORY_URL,
            )
            #print("response = ", response)
            '''


   
    # To delete SQS -----------------------------------
    def deleteSQS_Msg(self, lsDeteDeadLogs):
    
        for strReceiptHandle in lsDeteDeadLogs:
            try:
                response = self.sqs_client.delete_message(
                    QueueUrl=config.SQS_QUEUE_DEAD_INVENTORY_URL,
                    ReceiptHandle=strReceiptHandle
                )
                print("response = ", response)
            except:
                print("Error while sending message to Inventory")


#---------------------------------------------------------------------------
# Function Calls
#-----------------
#oClearDeadSQS = clearDeadSQS()


