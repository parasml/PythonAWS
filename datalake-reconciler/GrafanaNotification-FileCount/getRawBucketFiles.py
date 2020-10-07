import boto3
from datetime import datetime, timedelta
from botocore.errorfactory import ClientError
import awswrangler as wr

import config

class getRawBucketFiles:

    def __init__(self):

        self.s3 = boto3.client('s3', region_name='us-east-1')

    
    def getRawS3Key(self, metaDataInfo):

        self.metaDataInfo = metaDataInfo
        strTable = self.metaDataInfo['tablename']

        currentDateTime = self.metaDataInfo['currentdatetime']

        self.metaDataInfo['raw_tablename'] = self.GetRawTableName()

        print("metaDataInfo['raw_tablename'] = ", self.metaDataInfo['raw_tablename'])

        # If didn't get table name------------
        if self.metaDataInfo['raw_tablename'] == "":
            results = []
            return results

        strPrefix = self.metaDataInfo['s3key'] + self.metaDataInfo['raw_tablename']

        print("strPrefix = ", strPrefix)

        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket= config.RAW_BUCKET, Prefix=strPrefix)

        #startTime = '2020-05-25 00:00:00'
        #endTime = '2020-05-26 00:00:00'

        
        startTime = currentDateTime - timedelta(hours=1)
        startTime = str(startTime.replace(minute=0, second=0, microsecond=0))
        endTime = str(currentDateTime.replace(minute=0, second=0, microsecond=0))

        #print("startTime = ", startTime)
        #print("endTime = ", endTime)
        
        strFilter = "Contents[?to_string(LastModified)>=" +'\'\"'+ startTime +'\"\''+ '&&' + 'to_string(LastModified)<' +'\'\"'+ endTime +'\"\''+ "].Key"

        print("strFilter = ", strFilter)

        results = []

        filtered_iterator = pages.search(strFilter)
        #print("filtered_iterator = ", filtered_iterator)

        for key_data in filtered_iterator:
            #print("key_data = ", key_data)
            results.append(key_data)

        #print (sorted(response['Contents'], key=lambda item: item['LastModified']))
        print("results = ", results)

        return results

    # To get the Acutal Raw table name----------------------------------
    # We have the cuaretd table name as in parameters, need to get Raw Name
    def GetRawTableName(self):

        path = "s3://" +config.RAW_BUCKET+ "/"+self.metaDataInfo['s3key']

        lsDirectory = wr.s3.list_objects(path)

        for path in lsDirectory:

            tableName = path.split('/')[-2]
            rawTableName = config.convertStringToLower(tableName)
            
            if rawTableName == self.metaDataInfo['tablename']:
                
                print("RAWWWW table name = ", rawTableName)
                return tableName
        
        return ""


    # To check for the missing files ------------------------------------
    def checkForMissingFile(self, lsMissingS3Key):

        lsMissedKeys = []

        for s3Key in lsMissingS3Key:
            try:
                self.s3.head_object(Bucket=config.RAW_BUCKET, Key=s3Key)
            except ClientError:
                # Not found
                print("Not found")
                lsMissedKeys.append(s3Key)
        
        return lsMissedKeys



#-----------------------------------
'''
from datetime import datetime, timedelta
now = datetime.utcnow()

getRawBucketFiles(now)
'''