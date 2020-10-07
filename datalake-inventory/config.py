#---------------------------------------------------------------------
# To store default paths
#-----------------------------
import os
import re
import awswrangler as wr
# Bucket ---------
RAW_BUCKET = os.environ['RAW_BUCKET']
CURATED_BUCKET = os.environ['CURATED_BUCKET']

# Postgres DB ------
PG_DATABASE_HOST =  os.environ['PG_DATABASE_HOST']
#PG_DATABASE_NAME = os.environ['PG_DATABASE_NAME']
#PG_DATABASE_PORT = os.environ['PG_DATABASE_PORT']
PG_DATABASE_USER = os.environ['PG_DATABASE_USER']
PG_DATABASE_PASSWORD = os.environ['PG_DATABASE_PASSWORD']
KMS_KEY_ARN = os.environ['KMS_KEY_ARN']

# SQS --------------
SQS_QUEUE_NOTIFICATION_URL = os.environ['SQS_QUEUE_NOTIFICATION_URL']
SQS_QUEUE_INVENTORY_URL = os.environ['SQS_QUEUE_INVENTORY_URL']

# DynamoDB --------
DYNAMODB_CURATED_RECONCILE_TABLE_NAME = os.environ['DYNAMODB_CURATED_RECONCILE_TABLE_NAME']
DYNAMODB_RAW_RECONCILE_TABLE_NAME = os.environ['DYNAMODB_RAW_RECONCILE_TABLE_NAME']

#TABLE_NAME = 'curated_inventory_metadata'

PG_DATABASE_PORT = '5432'
PG_DATABASE_NAME = 'inventory'

TTL_DD = 2592000   #TTl of 30 days for DD

#--------------------------------------------------------
# To lower and remove special characters from string
#------------------------------------
def convertStringToLower(strName):
             
    strLowerNew = re.sub('[^a-zA-Z]+', '', strName) 
    strLowerNew = strLowerNew.lower()
    return strLowerNew

# --------------------------------------------------------
# Function to get primary key
#-------------------------------
'''
def getPrimaryKey(strDBName, strTableName):
    
    strDBName = str.lower(strDBName)
    strTableName = str.lower(strTableName)

    strPrimaryKeyColumn = ''

    if strDBName == "abshire":

        if strTableName == "bgorders":
            strPrimaryKeyColumn = 'BgOrderID'
        elif strTableName == "searchreq":
            strPrimaryKeyColumn = 'ReqID'
            

    return strPrimaryKeyColumn   

'''
# --------------------------------------------------------
# Function to get primary key
#-------------------------------
def getPrimaryKey(strDBName, strTableName):


    strTableName = strTableName.lower()

    try:
        table = wr.catalog.table(database=strDBName, table=strTableName)

        colPrimaryKey = table[table['Comment'] == 'primary-key']['Column Name'].values[0]
        #colRecordDateTime = table[table['Comment'] == 'record-create-time']['Column Name'].values[0]
        colRecordDateTime = 'null'    # Right now we are not adding 'created' column in every table.

    except:
        print("ERROR: Not able to get Primary Key of table", strTableName)
        return None, None


    #print("Config colPrimaryKey = ", colPrimaryKey)
    #print("Config colRecordDateTime = ", colRecordDateTime)

    return colPrimaryKey, colRecordDateTime


#-----------------------------------------------------------
# Match Glue Catalog table column names with Actual S3 Raw file columns
# Ex: reqid --> ReqID
#---------------------
def MatchColumns(lsColumns, strColName):
    
    #lsLowerColumns = map(str.lower, lsColumns)  # To lower column
    lsLowerColumns = [x.lower() for x in lsColumns]

    try:
        index = lsLowerColumns.index(strColName)
    except:
        print("Column didn't match", strColName)
        return None

    #print("Primary Collll = ", lsColumns[index])
    return lsColumns[index]

