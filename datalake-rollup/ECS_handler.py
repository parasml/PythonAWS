#------------------------------------------------------------------------------
# Python class for files Roll up
#------------------------------------
import json
import boto3
import awswrangler as wr
import pandas as pd
import os
from datetime import datetime, timedelta
from dateutil.parser import parse

import config
import consolidateFiles
import LogError

class ECS_handler:

    def __init__(self, now, RollUP_LEVEL):

        # Dev----------
        #self.MAX_SIZE = config.MAX_SIZE
        self.BUCKET = config.CURATED_BUCKET
        self.KEY = config.KEY
        self.LEVEL = RollUP_LEVEL

        self.nowTime = now
        
        #self.MB_KB = pow(10,6)

        self.clientS3 = boto3.client('s3')

        # Functions calls ------------------
        self.Controller()

    # Main controller function --------------------------------
    def Controller(self):

        #print("Function: Controller -----")

        rollupTime, strPartition, strPartitionTarget = self.TimePartitionData()
        #print("strPartition = ", strPartition)

        lsTablePaths = self.ReadFolders()

        # Error log -----
        if len(lsTablePaths) == 0:
            strError = "No folders found in Curated Bucket"
            LogError.LogError(strError)
            return

        #print("lsTablePaths = ", lsTablePaths)

        for strTable in lsTablePaths:

            sourceTableFolder = 's3://'+self.BUCKET+'/'+ self.KEY+strTable+'/'+strPartition
            sourceTargetFolder = 's3://'+self.BUCKET+'/'+ self.KEY+strTable+'/'+strPartitionTarget

            #print("sourceTableFolder = ", sourceTableFolder)
            #if (strTable == 'SearchReq_max_col'): # To be commented ---------------
            lsParquetFiles = self.ReadParquetFiles(sourceTableFolder)

            if len(lsParquetFiles) == 0:
                
                strError = "No hour file found in table" + strTable 
                LogError.LogError(strError)
                continue

            metaDataInfo = {}  # To store the metadata info while writing into DB
            metaDataInfo['table'] = strTable
            metaDataInfo['sourceTableFolder'] = sourceTableFolder
            metaDataInfo['targetTableFolder'] = sourceTargetFolder
            metaDataInfo['level'] =self.LEVEL
            metaDataInfo['bucket'] = self.BUCKET
            metaDataInfo['s3key'] = self.KEY+strTable+'/'+strPartition
            metaDataInfo['dbname'] = self.KEY.split('/')[0]

            #self.ConsolidateParquetFiles(lsParquetFiles, metaDataInfo)

            consolidateFiles.consolidateFiles(lsParquetFiles, metaDataInfo, rollupTime)

            #return  # Need to comment this... # Need to create threads over here-----


    # Get rollup partitio time with buffer ------------------------
    def TimePartitionData(self):

        #now = datetime.utcnow()
        now = self.nowTime
        #print("now = ", now)

        # Function to add single digit with zero infront-----
        def AddSingleDigitWithZero(strDigit):

            if len(strDigit) == 1:
                return str('0'+strDigit)
            else:
                return strDigit


        if (self.LEVEL == 'hour'):
            rollupTime = now - timedelta(hours=0, minutes=config.BUFFER_HOUR)
            #strPartition = 'year='+str(rollupTime.year)+'/month='+str(rollupTime.month)+'/day='+str(rollupTime.day)+'/hour='+str(rollupTime.hour)+'/'
            strPartition = 'year='+str(rollupTime.year)+'/month='+AddSingleDigitWithZero(str(rollupTime.month))+'/day='+AddSingleDigitWithZero(str(rollupTime.day))+'/hour='+AddSingleDigitWithZero(str(rollupTime.hour))
            strPartitionTarget = 'year='+str(rollupTime.year)+'/month='+AddSingleDigitWithZero(str(rollupTime.month))+'/day='+AddSingleDigitWithZero(str(rollupTime.day))
            #print("rollupTime = ", rollupTime)

        elif (self.LEVEL == 'day'):
            rollupTime = now - timedelta(hours=config.BUFFER_DAY)
            strPartition = 'year='+str(rollupTime.year)+'/month='+AddSingleDigitWithZero(str(rollupTime.month))+'/day='+AddSingleDigitWithZero(str(rollupTime.day)) 
            strPartitionTarget = 'year='+str(rollupTime.year)+'/month='+AddSingleDigitWithZero(str(rollupTime.month))
            #print("rollupTime = ", rollupTime)

        elif (self.LEVEL == 'month'):
            rollupTime = now - timedelta(days=config.BUFFER_MONTH)
            strPartition = 'year='+str(rollupTime.year)+'/month='+AddSingleDigitWithZero(str(rollupTime.month))
            strPartitionTarget = 'year='+str(rollupTime.year)
            #print("rollupTime = ", rollupTime)

            
        return rollupTime, strPartition, strPartitionTarget

    # To read S3 table folder -------------------------------------
    def ReadFolders(self):

        lsTablePaths = []

        result = self.clientS3.list_objects(Bucket=self.BUCKET, Prefix=self.KEY, Delimiter='/')
        #print("result = ", result)
        
        # If no files --------------------------
        if result is None:
            return lsTablePaths
        

        for tableName in result.get('CommonPrefixes'):
            #print('sub folder : ', tableName.get('Prefix'))
            #lsTablePaths.append(tableName.get('Prefix'))
            strPath = tableName.get('Prefix')
            lsStr = strPath.split("/")
            strTableName = lsStr[-2]
            lsTablePaths.append(strTableName)

        #print("lsTablePaths = ", lsTablePaths)
        return lsTablePaths
        
    # Read parquet files from S3 Key (Returns only the list of parquet)-------------------------------
    def ReadParquetFiles(self, sourceFolder):

        lsFiles = wr.s3.list_objects(sourceFolder)
        lsParquetFiles = []

        if len(lsFiles) == 0:

            #print("No files found -----")
            return lsParquetFiles

        #print("111 lsFiles = ", lsFiles)
        for strFile in lsFiles:

            #print(strFile[-7:])
            fileExt = strFile[-7:]   #To check only for parquet files----------------

            if fileExt == 'parquet':
                lsParquetFiles.append(strFile)

        #print("lsParquetFiles = ", len(lsParquetFiles))

        return lsParquetFiles
        
