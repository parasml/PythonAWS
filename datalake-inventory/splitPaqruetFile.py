#---------------------------------------------------------------------------------
# Called from ES
# To split file records according to their date and copy accordingly
#--------------------------------------------------------------------

import awswrangler as wr
import config
import pandas as pd
pd.options.mode.chained_assignment = None
import re
import boto3
import uuid
import LogError
import time
from boto3.dynamodb.conditions import Key, Attr
import time
from datetime import datetime, timedelta

'''
import pandas as pd
import pyodbc
import platform
'''

class splitPaqruetFile:

    def __init__(self, objMetaData, oPostgresDB):

        print("class spliParuetFile 1111")
        self.objMetaData = objMetaData
        self.oPostgresDB = oPostgresDB

        self.readFile()

    # To each record of the file------------------------
    def readFile(self):
    
        file = 's3://' + self.objMetaData['bucket'] + '/' + self.objMetaData['s3key']
        print("Raw file to split = ", file)
        print("File Type = ", self.objMetaData['filetype'])
        print("--------------------------------")

        if self.objMetaData['filetype'] == 'json':

            try:
                dfRaw = wr.s3.read_json(file, lines=True)
            except:
                #print("JSON File doesn't exists: ", file)
                #LogError.LogError("JSON File doesn't exists:" + file)
                # Fire SNS*
                return

        elif self.objMetaData['filetype'] == 'parquet':

            try:
                dfRaw = wr.s3.read_parquet(file, dataset=True)
            except:
                print("ERROR 2 Parquet File doesn't exists: ", file)
                #LogError.LogError("Parquet File doesn't exists:" + file)
                return

        elif self.objMetaData['filetype'] == 'csv':
            pass



        # To store Primary key column -------------------------------
        if self.objMetaData['primarykey_Column'] != '':

            strPrimaryKey = config.MatchColumns(dfRaw.columns, self.objMetaData['primarykey_Column'])
        else:
            strPrimaryKey = None


        # To store 'Record Create Time' key column -------------------------------
        if self.objMetaData['record-create-time_column'] != '':
            #strRecordCreateKey = config.MatchColumns(dfRaw.columns, self.objMetaData['record-create-time_column'])
            strRecordCreateKey = None
        else:
            strRecordCreateKey = None


        # to check the records after grouping ----------------------
        totalFileRecords = dfRaw.shape[0]
        print("totalFileRecords = ", totalFileRecords)

        #print("df = ", dfRaw.shape)
        #print("df = ", dfRaw.columns)

        # Read particular column and then create small small dataframe-----
        dfRaw['timestamp'] = pd.to_datetime(dfRaw['dms_timestamp'])
        #dfRaw['date'] = dfRaw['timestamp'].dt.date
        #dfRaw['time'] = dfRaw['timestamp'].dt.time
        dfRaw['year'] = dfRaw['timestamp'].dt.year
        dfRaw['month'] = dfRaw['timestamp'].dt.month
        dfRaw['day'] = dfRaw['timestamp'].dt.day
        dfRaw['hour'] = dfRaw['timestamp'].dt.hour        
        #print("hou1r= ", dfRaw["hour"].head(2))
        

        #For Reconcilation Testing**************************************
        #self.TestReconcileRecords(dfRaw)
        #self.TestReconcileRecords_SQL(dfRaw)
        #****************************************************************


        dfGroup = dfRaw.groupby(['hour','day', 'month', 'year'])

        #print("--------------------------------")
        #print("dfRaw = ", dfRaw.shape[0])

        # Group records with hour------------------
        for key, value in dfGroup:

            #print("key = ", key)
            #print("value = ", value)
            dfReconcile = pd.DataFrame()
            df = dfGroup.get_group(key)

            #print("Group df = ", df.shape[0])
           
            #print("primarykey_Column = ", self.objMetaData['primarykey_Column'])

            dfReconcile = df[['Op', 'dms_timestamp', 'year', 'month', 'day', 'hour']]

            #dfReconcile['ttl'] = config.TTL_DD + int(time.time())   # Adding a ttl for 30 days ---------------
            #dfReconcile['ttl'] = 'null'

            #print("strPrimaryKey = ", strPrimaryKey)
                
            if strPrimaryKey is None:
                dfReconcile['primarykey_Value'] = 'null'
                # Fire SNS**
            else:
                try:
                    dfReconcile['primarykey_Value'] = df[strPrimaryKey].to_numpy()
                    #dfReconcile['primarykey_Value'] = df[strPrimaryKey].values
                except:
                    print("Primary Key Error")
                    #print(dfReconcile.shape[0], df.shape[0])
                    dfReconcile['primarykey_Value'] = 'null'
                    

            
            

                
            #print("strRecordCreateKey = ", strRecordCreateKey)
                
            if strRecordCreateKey is None:
                dfReconcile['record-create-time_column'] = 'null'
                # Fire SNS**
            else:
                '''
                try:
                    dfReconcile['record-create-time_column'] = df[strRecordCreateKey].to_numpy()
                except:
                    print("record-create-time Key Error")
                '''
                dfReconcile['record-create-time_column'] = 'null'
                    
            
            dfReconcile.columns = map(str.lower, dfReconcile.columns)  # To lower column
            df = df.drop(columns =['year', 'month', 'day', 'hour', 'timestamp'])

            '''
            print("df = ", df.shape[0])
            print("dfReconcile.shape = ", dfReconcile.shape[0])
            print("dfReconcile.columns = ", dfReconcile.columns)
            print("-----------------")
            '''
            
            if dfReconcile.shape[0] != df.shape[0]:
                print("MIS MATCHHHHHHHHH in Reconcile-----")

            
            self.pushReconcileData_DD(dfReconcile, False)   # Dynamo-----
            self.CopyFilesToTarget(df, key)                 # S3 Curated
            self.pushInventoryMetaData_PS(True)     # Postgres( curated details are added ) -----
            #self.pushReconcileData_DD(dfReconcile, True)   # Dynamo-----

            totalFileRecords -= df.shape[0]

        
        if totalFileRecords != 0:
            print("RECORDS LOST while Grouping")




        
            

            
    # Copy files to Curated Bucket -------------------------
    def CopyFilesToTarget(self, df, key):
        
        #print("Inside functio CopyFilesToTarget = ", key)

        # To get partition key------
        def getstrTimeStamp(key):

            strYear = str(key[3])
            strMonth = str(key[2])
            strDay = str(key[1])
            strHour = str(key[0])

            if len(strHour) == 1: strHour= '0' + strHour
            if len(strDay) == 1: strDay= '0' + strDay
            if len(strMonth) == 1: strMonth= '0' + strMonth

            dt_string = "year=" + strYear + "/month=" + strMonth + "/day=" + strDay + "/hour=" + strHour
            return dt_string

        dt_string = getstrTimeStamp(key)

        #print("dt_string = ", dt_string)
        
        if self.objMetaData['schemaname'] != '':
            target_key = self.objMetaData['schemaname'] + '/'+ self.objMetaData['tablename']+'/'+dt_string
        else:
            target_key = self.objMetaData['tablename']+'/'+dt_string

        self.objMetaData['curated_s3key'] = target_key
        strCuratedFilePath = "s3://" + config.CURATED_BUCKET + "/" + target_key
        #print("CuratedFilePath = ", strCuratedFilePath)

        #------------------------------------------
        parqueFile = wr.s3.to_parquet(
        df=df,
        path=strCuratedFilePath,
        dataset=True,
        s3_additional_kwargs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": config.KMS_KEY_ARN
                    }
        )

        fileTargetPath = str(parqueFile['paths'][0])    
        strFile = fileTargetPath.split('/')[-1]

        #self.objMetaData['eventtime'] = 
        self.objMetaData['filename'] = strFile
        self.objMetaData['filesize'] = 'NA'
        self.objMetaData['recordcount'] = df.shape[0]

        #print("self.objMetaData['recordcount'] = ", self.objMetaData['recordcount'])

        #print("parqueFile = ", parqueFile)
    

    # To push record metadata in dynamo DB --------
    def pushReconcileData_DD(self, dfReconcile, isCurated=False):

        
        dfDeletRecords = pd.DataFrame()
        dfDeletRecords = dfReconcile[dfReconcile['op'].str.lower() == 'd'] #To get all delete Records-----

        #print("OPERATION TYPEEEEEE = ", dfReconcile['op'].unique())
        dfReconcile = dfReconcile[dfReconcile['op'].str.lower() == 'i'] #To push only insert records-----
        
 
        dfReconcile['eventtime'] = self.objMetaData['eventtime']
        #dfReconcile['filename'] = self.objMetaData['filename']
        dfReconcile['tablename']  = self.objMetaData['tablename']

        dfReconcile['dbname'] = self.objMetaData['dbname']

        #dfReconcile["tablename"] = dfReconcile["tablename"].str.lower()
        #dfReconcile["dbname"] = dfReconcile["dbname"].str.lower()

        lsIDs = []
       
        for x in range(dfReconcile.shape[0]):
            
            #id = uuid.uuid1()
            id = str(self.objMetaData['dbname']) + '-' + str(self.objMetaData['tablename']) + '-' + str(dfReconcile['primarykey_value'].iloc[x])
            lsIDs.append(str(id))
        

           
        dfReconcile['id'] = lsIDs
        #print("dfReconcile Columns = ", dfReconcile.columns)
        #print("dfReconcile Shape = ", dfReconcile.shape[0])


        # Convert dataframe to list of dictionaries (JSON) that can be consumed by any no-sql database -------------
        dfJSON=dfReconcile.T.to_dict().values()

        oDynamoDB = boto3.resource('dynamodb')
        #----------------------------

        if isCurated == False:
            table = oDynamoDB.Table(config.DYNAMODB_RAW_RECONCILE_TABLE_NAME)
        else:
            table = oDynamoDB.Table(config.DYNAMODB_CURATED_RECONCILE_TABLE_NAME)


        #print("table = ", table)
        #print('-------------')
        for itemRow in dfJSON:
            table.put_item(Item=itemRow)

        
        if isCurated == False:
            # To remove delete operations records from DD-------
            if dfDeletRecords.shape[0] != 0:
                #print("dfDeletRecords = ", dfDeletRecords.columns)
                #print("dfDeletRecords['primarykey_value'] = ", dfDeletRecords['primarykey_value'])
                self.removeDeleteRecords_DD(dfDeletRecords['primarykey_value'].to_numpy(), table)
                #self.removeDeleteRecords_DD(dfDeletRecords['primarykey_value'].values, table)
                #print("---COMPLETE DELETE TIME %s seconds ---" % (time.time() - start_time0))
        

    # To store file metadata in Postgresql db (Curated) -----------------------------
    def pushInventoryMetaData_PS(self, isCurated=False):
        if isCurated == False:
            self.oPostgresDB.insert_Raw_InventoryData(self.objMetaData)
            pass
        else:
            #self.oPostgresDB.insert_Curated_InventoryData(self.objMetaData)
            pass


    # To remove records from DD (operation 'delete') --------------------------------
    def removeDeleteRecords_DD(self, lsDeletRecords, table):

        #print("Length Primary Key Id = ", len(lsDeletRecords))
        #print("lsDeletRecords = ", lsDeletRecords)
        #start_time = time.time()

        for strPrimaryKey in lsDeletRecords:

            strId = self.objMetaData['dbname'] + '-' + self.objMetaData['tablename'] + '-' + str(strPrimaryKey)
            #print("strId = ", strId)
                
            try:
                table.delete_item(
                    Key={
                        'id': strId

                    }
                )
            except Exception as e:
                print("Error wile deleting record: ", e)

        #print("Enddd------------")
        #print("----DELETE TIME %s seconds ---" % (time.time() - start_time))

    '''
    #--------------------------------------------------------------------------------------------
    # For testing purpose only (Storing Records in DD)
    #----------------------------------------------------
    def TestReconcileRecords(self, dfRaw):

        # test-renconcile-raw
        if self.objMetaData['tablename'] == 'bgorders':
            dfTest = dfRaw[['dms_timestamp', 'day', 'hour', 'BgOrderID', 'Op']]
        elif self.objMetaData['tablename'] == 'searchreq':
            dfTest = dfRaw[['dms_timestamp', 'day', 'hour', 'ReqID', 'Op']]

        dfTest['tablename'] = self.objMetaData['tablename']
        dfTest['dbname'] = self.objMetaData['dbname']
        dfTest['s3key'] = self.objMetaData['s3key']

        #print("Testing dfTest = ", dfTest.shape[0])

        lsIDs = []
       
        for x in range(dfTest.shape[0]):
            
            id = uuid.uuid1()
            lsIDs.append(str(id))

        dfTest['id'] = lsIDs

         # Convert dataframe to list of dictionaries (JSON) that can be consumed by any no-sql database -------------
        dfJSON=dfTest.T.to_dict().values()

        oDynamoDB = boto3.resource('dynamodb')
        #----------------------------

        table = oDynamoDB.Table('test-renconcile-raw')

        for itemRow in dfJSON:
            table.put_item(Item=itemRow)


        
    #--------------------------------------------------------------------------------------------
    # For testing purpose only (Storing Records in MS-SQL (On Premise) database)
    #----------------------------
    def TestReconcileRecords_SQL(self, dfRaw):

        # test-renconcile-raw
        if self.objMetaData['tablename'] == 'bgorders':
            dfTest = dfRaw[['dms_timestamp', 'BgOrderID', 'Op']]
            dfTest['tablename'] = self.objMetaData['tablename']
            dfTest.rename(columns={"BgOrderID": "tableprimarykey"})

        elif self.objMetaData['tablename'] == 'searchreq':
            dfTest = dfRaw[['dms_timestamp', 'ReqID', 'Op']]
            dfTest['tablename'] = self.objMetaData['tablename']
            dfTest.rename(columns={"ReqID": "tableprimarykey"})

        #print("Testing dfTest = ", dfTest.shape[0])

        lsIDs = []
       
        for x in range(dfTest.shape[0]):
            
            id = uuid.uuid1()
            lsIDs.append(str(id))

        dfTest['Id'] = lsIDs


        if platform.system() == "Linux":
            sql_conn = ('DRIVER={FreeTDS};SERVER=10.25.12.158;PORT=1433;DATABASE=Abshire_test_datalake;UID=dmstest;PWD=dmstest@1234;TDS_Version=8.0')
        elif platform.system() == "Windows":
            sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};PORT=1433;SERVER=10.25.12.158;DATABASE=Abshire_test_datalake;UID=dmstest;PWD=dmstest@1234')


        cursor = sql_conn.cursor()
        for index, row in dfTest.iterrows():
            #print(row)
            cursor.execute("INSERT INTO abshire_reconcile_test([Id],[dms_timestamp],[Op],[tablename],[tableprimarykey]) values(?,?,?,?,?)", row['Id'], row['dms_timestamp'], row['Op'], row['tablename'], row['tableprimarykey'])

            #insert into bgorders_reconcile(bgorderid,dms_timestamp)values(1234,getutcdate())
        
        sql_conn.commit()
        cursor.close()
        sql_conn.close()

    '''


