#---------------------------------------------------------------------
# To rollup or roll down parquet files
#------------------------------------------
import json
import boto3
import awswrangler as wr
import pandas as pd
import os
from datetime import datetime, timedelta
from dateutil.parser import parse

import config
import postgresDB
import LogError

class consolidateFiles:

    def __init__(self, lsParquetFiles, metaDataInfo, rollupTime):

        self.lsRollDownFiles = []
        self.lsRollUpFiles = []
        self.nSeqNum = 0  # To index output file------

        self.MAX_SIZE = config.MAX_SIZE
        self.MB_KB = pow(10,6)

        self.lsParquetFiles = lsParquetFiles
        self.metaDataInfo = metaDataInfo

        self.rollupTime = rollupTime

        self.LEVEL = metaDataInfo['level']

        self.oPostgresDB = postgresDB.postgresDB()

        self.SplitFilesOnSize()

    
        #print("self.lsRollDownFiles = ", len(self.lsRollDownFiles))
        #print("self.lsRollUpFiles = ", len(self.lsRollUpFiles))

        # To rollup files if any -------- 
        if len(self.lsRollUpFiles):
            self.RollUp_ParquetFiles()

        '''
        # To rolldown files if any -------- 
        if len(self.lsRollDownFiles):
            self.RollDown_ParquetFiles()
        '''

        # To delete files which got rolled up/down--------
        self.DeleteObject()


    # To split files on Rollup and Rolldown functionality------
    def SplitFilesOnSize(self):

        for file in self.lsParquetFiles:

            fileSize = wr.s3.size_objects(file)
            
            #print("fileSize[file] = ", fileSize[file])
            size = fileSize[file]/self.MB_KB

            #print("size = ", size)

            if (size >= self.MAX_SIZE):

                self.lsRollDownFiles.append(file)

            else:
                self.lsRollUpFiles.append(file)

            #print("self.lsRollDownFiles = ", len(self.lsRollDownFiles))
            #print("self.lsRollUpFiles = ", len(self.lsRollUpFiles))


    # Consolidate Parquet files as per the size needed -----------------------
    def RollUp_ParquetFiles(self):

        rollupFileSize = 0
        rollupFileCount = 0

        dfRollup = pd.DataFrame()

        for index, file in enumerate(self.lsRollUpFiles, start=0):
            
            fileSize = wr.s3.size_objects(file)
            
            rollupFileCount = rollupFileCount + 1

        
            size = fileSize[file]/self.MB_KB
            #print("fileSize = ", size)

            self.metaDataInfo['sourceFile'] = file
            self.metaDataInfo['filesize'] = size
            self.metaDataInfo['totalCount'] = rollupFileCount

            #---------------------------

            rollupFileSize += size
            #print("rollupFileSize = ", rollupFileSize)

            df = wr.s3.read_parquet(file, dataset=True)
           
            dfRollup = dfRollup.append(df, ignore_index=True)
            #print("dfRollup = ", dfRollup.size)
                    

            if rollupFileSize <= 0:
                continue

            elif rollupFileSize >= self.MAX_SIZE:

                self.metaDataInfo['dfRollup'] = dfRollup
                self.WriteParuetFile()
                rollupFileSize = 0
                rollupFileCount = 0
                
            else:

                if index == (len(self.lsRollUpFiles) - 1):

                    #print("lastttttttttttttttttttt")
                    self.metaDataInfo['dfRollup'] = dfRollup
                    self.WriteParuetFile()
                    rollupFileSize = 0
                    rollupFileCount = 0
                
                else:
                    continue


    # To break down parquet file-------------------------------------
    def RollDown_ParquetFiles(self):

        #print("Inside fuction breakDownParquetFile 1111")

        for file in self.lsRollDownFiles:

            #print("New file ******************")
            #print("file = ", file)
            fileSize = wr.s3.size_objects(file)
            
            size = fileSize[file]/self.MB_KB
            
            self.metaDataInfo['sourceFile'] = file
            self.metaDataInfo['totalCount'] = 1


            #print("fileSize = ", size)
            #print("self.MAX_SIZE = ", self.MAX_SIZE)
            ntotalChunks = int(size/self.MAX_SIZE)  # To create no of size buckets---

            #print("ntotalChunks = ", ntotalChunks)

            eachfilesize = self.metaDataInfo['filesize']/ntotalChunks

            dfRolldown = wr.s3.read_parquet(self.metaDataInfo['sourceFile'], dataset=True)
            
            ntotalRows = dfRolldown.shape[0]

            nRowsInEachTable = int(ntotalRows/ntotalChunks)
            #print("nRowsInEachTable = ", nRowsInEachTable)

            nCurrentRow = 0
            for chunk in range(ntotalChunks):
                #print("chunk = ", chunk)

                if chunk is not (ntotalChunks - 1):
                    #print("Inside ifff")
                    df = dfRolldown.iloc[nCurrentRow: nRowsInEachTable, :]
                    
                else:
                    df = dfRolldown.iloc[nCurrentRow: ntotalRows, :]

                self.metaDataInfo['dfRollup'] = df
                self.metaDataInfo['filesize'] = eachfilesize
                
                self.WriteParuetFile()
                #print("df = ", df.shape)

                nCurrentRow = nCurrentRow + nRowsInEachTable

    # To get custom file path -------------
    def getCustomFilePath(self):

        # Function to add single digit with zero infront-----
        def AddSingleDigitWithZero(strDigit):

            if len(strDigit) == 1:
                return str('0'+strDigit)
            else:
                return strDigit
        
       
        self.nSeqNum +=1
        strNum =  str(format(self.nSeqNum, '010d'))

        if self.LEVEL == 'hour':
            strPartition = str(self.rollupTime.year)+'-'+AddSingleDigitWithZero(str(self.rollupTime.month))+'-'+AddSingleDigitWithZero(str(self.rollupTime.day))+'-'+AddSingleDigitWithZero(str(self.rollupTime.hour))
        elif self.LEVEL == 'day':
            strPartition = str(self.rollupTime.year)+'-'+AddSingleDigitWithZero(str(self.rollupTime.month))+'-'+AddSingleDigitWithZero(str(self.rollupTime.day))
        elif self.LEVEL == 'month':
            strPartition = str(self.rollupTime.year)+'-'+AddSingleDigitWithZero(str(self.rollupTime.month))
            
            
        #strFilePath = strPartition + '-' + strNum + ".snappy.parquet"
        strFilePath = strPartition + '-' + self.LEVEL + '_' + strNum + '_' + str(datetime.utcnow()) + ".snappy.parquet"

        #print("o/p strFilePath = ", strFilePath)

        return strFilePath


    # Write Parquet file to S3 ---------------------------------------
    def WriteParuetFile(self):

        #print("self.metaDataInfo = ",self.metaDataInfo.keys()) 
        
        
        # To fix Boolean datatype -----------------------
        for column in self.metaDataInfo['dfRollup']:
            #print("column = ", type(dfRollup[column].dtype))
            dataType = str(self.metaDataInfo['dfRollup'][column].dtype)
            #print("dataType = ", dataType)

            if (dataType == 'boolean'):
                #print("column = ", column)
                self.metaDataInfo['dfRollup'][column].fillna(False, inplace=True)
                self.metaDataInfo['dfRollup'][column] = self.metaDataInfo['dfRollup'][column].astype('bool')

        #-------------------------------------------------------------

        #print("metaDataInfo['targetTableFolder'] = ", self.metaDataInfo['targetTableFolder'])

        strTargetPath = self.metaDataInfo['targetTableFolder'] + '/' + self.getCustomFilePath()

        parqueFile = wr.s3.to_parquet(
            df=self.metaDataInfo['dfRollup'],
            #path=self.metaDataInfo['targetTableFolder'],
            path= strTargetPath,
            dataset=False,
            s3_additional_kwargs={
                        "ServerSideEncryption": "aws:kms",
                        "SSEKMSKeyId": config.KMS_KEY_ARN
                        }
        )

        
        #print("parqueFile = ", parqueFile)
        #print("parqueFile = ", type(parqueFile['paths'][0]))
        fileTargetPath = str(parqueFile['paths'][0])
        strFile = fileTargetPath.split('/')[-1]
        #print("lsFile = ", lsFile[-1])

        #self.metaDataInfo['file'] = fileTargetPath
        self.metaDataInfo['file'] = strTargetPath

        now = datetime.now()
        self.metaDataInfo['fileID'] = str(now)+'_'+strFile

        self.pushRollUpMetaData()

    # To store metadata in Postgresql db -----------------------------
    def pushRollUpMetaData(self):

        self.metaDataInfo['eventtime'] = str(datetime.utcnow())  # Need to replace Lambda event time
        self.metaDataInfo['totalColumns'] = str(self.metaDataInfo['dfRollup'].shape[1])
        self.metaDataInfo['totalRows'] = str(self.metaDataInfo['dfRollup'].shape[0])
        self.metaDataInfo['filescount'] = str(self.metaDataInfo['totalCount'])
        self.metaDataInfo['filesize'] = str(self.metaDataInfo['filesize'])
        self.metaDataInfo['status'] = 'active'

        self.metaDataInfo['dfRollup'] = 'null'  # Not sending our dataframe....

        self.oPostgresDB.insert_Rollup_Data(self.metaDataInfo)

    # To delete an S3 object ------------------
    def DeleteObject(self):

        # Need to check DB before deleting-------
        wr.s3.delete_objects(self.lsParquetFiles)
        
    #-------------------------------------------------------------------------------



#-------------------------------------------------------------------------------
