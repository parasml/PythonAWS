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
        
        print("Reconciliation Filecount ****************************")

        self.clientS3 = boto3.client('s3')

        self.metaDataInfo = {}
        self.metaDataInfo['currentdatetime'] = currentDateTime
        #self.metaDataInfo['s3key'] = config.KEY

        self.metaDataInfo['tablename'] = config.GRAFANA_TABLE_NAME
        self.metaDataInfo['dbname'] = config.GRAFANA_DB_NAME
        self.metaDataInfo['schemaname'] = config.GRAFANA_SCHEMA_NAME
        self.metaDataInfo['s3key'] = self.metaDataInfo['dbname'] + "/" + self.metaDataInfo['schemaname']  + "/"

        self.oPostgresDB = postgresDB.postgresDB()
        self.PostgreSqlList = self.oPostgresDB.getRawS3Key(self.metaDataInfo)
        print("PostgreSqlList = ", self.PostgreSqlList)

        self.oGetRawBucketFiles = getRawBucketFiles.getRawBucketFiles()
        self.RawS3List =  self.oGetRawBucketFiles.getRawS3Key(self.metaDataInfo)

        self.RawS3List = self.RawS3List[:-1]  # To be commented ----------------------
        print("RawS3List = ", self.RawS3List)

        self.compareCounts()


    # To read S3 table folder -------------------------------------
    def getTableLocation(self):

        pass

        '''
        df_tableLocation = wr.catalog.get_table_location(database=self.metaDataInfo['dbname'], table=self.metaDataInfo['tablename'])

        strbucketIndex = df_tableLocation.find(config.RAW_BUCKET) + len(config.RAW_BUCKET) + 1

        self.metaDataInfo['s3key'] = df_tableLocation[strbucketIndex:]

        print("strbucketIndex = ", strbucketIndex)    
        print("self.metaDataInfo['s3key'] = ", self.metaDataInfo['s3key'])
        '''

    # To compare counts -------------------------
    def compareCounts(self):

        countRaw = len(self.RawS3List)
        contPostgres = len(self.PostgreSqlList)

       
        self.metaDataInfo['source_count'] = countRaw
        self.metaDataInfo['destination_count'] = contPostgres
        self.metaDataInfo['difference'] = countRaw - contPostgres
        self.metaDataInfo['created_date_time'] = datetime.utcnow()

        print("source_count", self.metaDataInfo['source_count'])
        print("destination_count", self.metaDataInfo['destination_count'])
        print("difference", self.metaDataInfo['difference'])
        print("created_date_time", self.metaDataInfo['created_date_time'])


        if (self.metaDataInfo['difference'] > 0):

            self.GetIndividualFile_S3()

        else:
            self.updateGrafanaTable()
        
        

    # To check missing files indiviually ---------
    def GetIndividualFile_S3(self):
        
        #lsMissingS3Key = list(set(self.RawS3List)^set(self.PostgreSqlList))  
        lsMissedKeys = list(set(self.RawS3List) - set(self.PostgreSqlList))  # We are looking for only positive Differences------

        #lsMissedKeys = self.oGetRawBucketFiles.checkForMissingFile(lsMissingS3Key)

        print("lsMissedKeys = ", len(lsMissedKeys))
        print("lsMissedKeys = ", lsMissedKeys)
        print("-------------")
        
        if len(lsMissedKeys) != 0:
            clearDeadSQS.clearDeadSQS(self.metaDataInfo, lsMissedKeys)

        # Raw and postres count is same now ------
        self.metaDataInfo['destination_count'] = self.metaDataInfo['source_count']
        self.metaDataInfo['difference'] = self.metaDataInfo['destination_count'] - self.metaDataInfo['source_count']
        self.metaDataInfo['created_date_time'] = datetime.utcnow()

        self.updateGrafanaTable()



    # send Data to Grafna -------------------------------------------
    def updateGrafanaTable(self):
        
        print("Function updateGrafanaTable ******************")
        self.oPostgresDB.insertGrafanaTable(self.metaDataInfo)
        

    # to send the miscount of files to SNS ---------------------------
    '''
    def sendSNS(self, lsMissedKeys):

        client = boto3.client('sns', 'us-east-1')

        response = client.publish(
            TopicArn=config.SNS_GRAPHANA,
            Message= "Missing files: " + str(lsMissedKeys),
            Subject='Datalake Reconcile: S3 Bucket Filecount discrepancies '
        )

        #print("response = ", response)
    '''



#------------------------------------------------------
