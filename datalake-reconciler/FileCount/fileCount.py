#-------------------------------------------------------------------
# Reconciler: File count
#--------------------
import postgresDB
import getRawBucketFiles
import boto3
from datetime import datetime, timedelta

import config
import clearDeadSQS

class fileCount:

    def __init__(self, currentDateTime):
        
        #print("Reconciliation Filecount ****************************")

        self.clientS3 = boto3.client('s3')

        self.metaDataInfo = {}
        self.metaDataInfo['currentdatetime'] = currentDateTime
        self.metaDataInfo['s3key'] = config.KEY

        lsTablePaths = self.ReadFolders()

        #print("lsTablePaths = ", lsTablePaths)

        for strTable in lsTablePaths:

            self.metaDataInfo['tablename'] = strTable
            self.metaDataInfo['dbname'] = str(config.KEY.split('/')[0])

            self.oPostgresDB = postgresDB.postgresDB()
            self.PostgreSqlList = self.oPostgresDB.getRawS3Key(self.metaDataInfo)

            self.oGetRawBucketFiles = getRawBucketFiles.getRawBucketFiles()
            self.RawS3List =  self.oGetRawBucketFiles.getRawS3Key(self.metaDataInfo)

            self.compareCounts()


    # To read S3 table folder -------------------------------------
    def ReadFolders(self):

        lsTablePaths = []

        result = self.clientS3.list_objects(Bucket=config.RAW_BUCKET, Prefix=config.KEY, Delimiter='/')
        #print("result = ", result)
        
        
        for tableName in result.get('CommonPrefixes'):
            #print('sub folder : ', tableName.get('Prefix'))
            #lsTablePaths.append(tableName.get('Prefix'))
            strPath = tableName.get('Prefix')
            lsStr = strPath.split("/")
            strTableName = lsStr[-2]
            lsTablePaths.append(strTableName)

        #print("lsTablePaths = ", len(lsTablePaths))
        return lsTablePaths


    # To compare counts -------------------------
    def compareCounts(self):

        countRaw = len(self.RawS3List)
        contPostgres = len(self.PostgreSqlList)

       
        self.metaDataInfo['source_count'] = countRaw
        self.metaDataInfo['destination_count'] = contPostgres
        self.metaDataInfo['difference'] = countRaw - contPostgres
        self.metaDataInfo['created_date_time'] = datetime.utcnow()

        self.updateGrafanaTable()

    # send Data to Grafna -------------------------------------------
    def updateGrafanaTable(self):
        
        self.oPostgresDB.insertGrafanaTable(self.metaDataInfo)
        

#------------------------------------------------------
