# --------------------------------------------------------------------
# Called from: main,py
# To read file of S3 (SQS)
# Store the Raw metadata to Postgresql
# Send to: Send the SQS Event(S3 file) to 'splitParquet.py' 
#-------------------------------------------------------

import boto3
import json
import os
from datetime import datetime, timedelta
from dateutil.parser import parse
import urllib
import re

import config
import postgresDB
import splitPaqruetFile


import awswrangler as wr
import pandas as pd

class ECS_handler:

    def __init__(self, event):
        
        self.event = event
        self.oPostgresDB = postgresDB.postgresDB()
        self.clientSQS = boto3.client('sqs', 'us-east-1')

        self.initEvent()

        #self.oPostgresDB.closeConnection()  # Closing postgres connection----


    def initEvent(self):

        print("Injestion Started *****************************")
        #print("Sqs Event Received = ", self.event)
        
        # s3 file record ( For ECS)-------
        msgBody = self.event
        record = msgBody['Records'][0]
        s3Key=urllib.parse.unquote_plus(record['s3']['object']['key'])  # This removes blank spaces from URL-----


        # Check for Duplicated file -------
        bFileExists = self.oPostgresDB.chekIfEntryExists(s3Key)
        if bFileExists == True:
            #print("Raw Parquet File Already Added: ", s3Key)
            return

        objMetaData = self.getEventProperties(record, s3Key)

        # If primary Key is not there, return
        if objMetaData['primarykey_Column'] is None:

            print("ERROR: Insertion, Not able to find primary key of table")
            # FIRE SNS**
            return



        # Check for particular format file --------------------------
        if (str(s3Key[-7: ]) == 'parquet'):   # Parquet File----
            
            objMetaData['filetype'] = 'parquet'
            #objMetaData = self.readParquetFile(objMetaData)
            self.oPostgresDB.insert_Raw_InventoryData(objMetaData)    #To add Raw Metadata -----
            splitPaqruetFile.splitPaqruetFile(objMetaData, self.oPostgresDB)
                
                
        elif (str(s3Key[-4: ]) == 'json'):    #Json files-----
            
            objMetaData['filetype'] = 'json'
            self.oPostgresDB.insert_Raw_InventoryData(objMetaData)    #To add Raw Metadata -----
            self.sendJsonToSQS(record)
            #splitPaqruetFile.splitPaqruetFile(objMetaData, self.oPostgresDB)
            
                
        elif (str(s3Key[-3: ]) == 'csv'):  # Csv Files-------
            
            objMetaData['filetype'] = 'csv'
        
        else:
            #print("No file extension is : ", s3Key)
            pass
            

    # Extarct event properties and store in object ---------
    def getEventProperties(self, record, s3Key):

            objMetaData = {}
            s3ObjectSize = record['s3']['object']['size']
            objMetaData['bucket'] = record['s3']['bucket']['name']
            objMetaData['s3key'] = s3Key
            objMetaData['filesize'] = s3ObjectSize
            objMetaData['eventtime'] = record['eventTime']

            lsS3Key = s3Key.split('/')
            objMetaData['filename'] = lsS3Key[-1]
            objMetaData['tablename'] = lsS3Key[-2]
            objMetaData['dbname'] = s3Key.split('/')[0]


            # Removing special characters and Lower the table name-------
            objMetaData['orignal-tablename'] = objMetaData['tablename'] 
            objMetaData['tablename'] = config.convertStringToLower(objMetaData['tablename'])
            objMetaData['dbname'] = config.convertStringToLower(objMetaData['dbname'])


            # To get Primary key -------
            objMetaData['primarykey_Column'], objMetaData['record-create-time_column'] = config.getPrimaryKey(objMetaData['dbname'], objMetaData['orignal-tablename'])

            # To get Schema Name (Schema name will be only inside S3 bucket)------
            nEndIndex = len(lsS3Key) - 2
            if nEndIndex > 0:
                lsSchema = lsS3Key[0:nEndIndex]
                objMetaData['schemaname'] = "/".join(lsSchema)
            else :
                objMetaData['schemaname'] = ''
        
            '''
            print("objMetaData = ", objMetaData)
            print("s3ObjectSize = ", s3ObjectSize)
            print("s3Key = ", s3Key)
            print("lsRecords[0] = ", lsRecords[0].keys())
            '''

            return objMetaData
        
        
    #Send event to JSON SQS (sqs-datalower-sfdc-us-east-1) 
    def sendJsonToSQS(self, record):
        
        #QueueUrl='https://sqs.us-east-1.amazonaws.com/909045093730/inventory-notification',
        response = self.clientSQS.send_message(
            QueueUrl=config.SQS_QUEUE_NOTIFICATION_URL,
            MessageBody=str(record),
            DelaySeconds=5,
            )
        
        #print("response = ", response)



    #*************************************************************************
    # For Testing purpose
    #***********************
    def readParquetFile(self, objMetaData):
    
        file = 's3://' + objMetaData['bucket'] + '/' + objMetaData['s3key']
        #print("Raw file to split = ", file)

        try:
            dfRaw = wr.s3.read_parquet(file, dataset=True)
        except:
            print("ERROR 1 RAW Parquet File doesn't exists: ", file)
            #LogError.LogError("Parquet File doesn't exists:" + file)
            objMetaData['recordcount'] = 0
            return objMetaData

        print("dfRaw.shape[0] = ", dfRaw.shape[0])
        objMetaData['recordcount'] = dfRaw.shape[0]

        return objMetaData

