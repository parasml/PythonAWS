
import json
import pandas as pd


DATABASE_NAME= "abshire"
SCHEMA_NAME = "dbo"
FILE_PATH = 'C:/Prashan/bitbucket/datalake/datalake-adhoc/DMS-Mappings/GeneratedExcel/Abshire_Replication_Table.xlsx'
#GROUP_TABLES = ['Abshire_Schools']
#GROUP_TABLES = ['Abshire_BgOrders', 'Abshire_SearchReq']

GROUP_TABLES = []   # For all tables in single json-------

def readData():
    pdData = pd.read_excel(FILE_PATH, sheet_name='Table Name', index_col=None, header=0)
    print("shape = ", pdData.shape)

    # To lower all the columns data ----------
    pdData = pdData.applymap(lambda s:s.lower() if type(s) == str else s)

    # To trim all column values --------------
    pdData.replace('(^\s+|\s+$)', '', regex=True, inplace=True)

    # To filter out particular Database tables only --------
    pdData=pdData[pdData['Database Name'] == DATABASE_NAME]
    print("shape = ", pdData.shape)

    # To filter with Group ------------
    GROUP_TABLES_LOWER = [v.lower() for v in GROUP_TABLES]
    print("GROUP_TABLES = ", GROUP_TABLES_LOWER)

    df = pd.DataFrame()
    for group in GROUP_TABLES_LOWER:
        #print("group = ", group)
        #df = pdData[pdData['PublicationName'] == group]
        df = df.append(pdData[pdData['PublicationName'] == group], ignore_index=True)

        #print("df = ", df)

    if df.shape[0] !=0:
        print("Iffff")
        return df

    return pdData


def getRuleObject(index):

    data = {}
    data["rule-type"] = "selection"
    data["rule-id"] = str(index)
    data["rule-name"] = str(index)
    data["object-locator"] = {}
    data["rule-action"] = "include"
    data["filters"]=[]

    return data
    
def getObjectLocator(strTableName, strSchemaName):

    data = {}
    data["schema-name"] = SCHEMA_NAME
    data["table-name"] = strTableName
    return data

def getExclusionTransformation(index):

    data = {}
    data["rule-type"] = "transformation"
    data["rule-id"] = str(index)
    data["rule-name"] = str(index)
    data["rule-target"] = "column"
    data["object-locator"] = {}
    data["rule-action"] = "remove-column"
    return data

def getObjectLocatorTransformation(strTableName, strSchemaName, strColumnName):

    data = {}
    data["schema-name"] = SCHEMA_NAME
    data["table-name"] = strTableName
    data["column-name"] = strColumnName
    return data

# To get complete Mapping --------------------------
def getCompleteMapping_JSON(pdData):

    #print("pdData = ", pdData.columns)
   
    #'SchemaName'

    oRule = {}
    oRule["rules"] = []

    for i in range(pdData.shape[0]):

        #print("info = ", str.lower(pdData['Publisher_Table'].iloc[i]))

        strTableName = pdData['Publisher_Table'].iloc[i]
        strSchemaName = pdData['SchemaName'].iloc[i]
        

        data = getRuleObject(i+1)
        data["object-locator"] = getObjectLocator(strTableName, strSchemaName)
        oRule["rules"].append(data)

    #print("data = ", data)
    #print(oRule)

    # Column Exclusion Transformation ------------------------

    dfBlob = pdData[pdData.BlobColumns.notnull()]

    nIndex = pdData.shape[0]

    for i in range(dfBlob.shape[0]):

        strTableName = dfBlob['Publisher_Table'].iloc[i]
        strSchemaName = dfBlob['SchemaName'].iloc[i]
        strColumnName = dfBlob['BlobColumns'].iloc[i]

        lsBlobColumns = strColumnName.split(',')
        #print("lsBlobColumns = ", lsBlobColumns)

        for colname in lsBlobColumns:
            nIndex = nIndex + 1
            data = getExclusionTransformation(nIndex)
            data["object-locator"] = getObjectLocatorTransformation(strTableName, strSchemaName, colname)
            oRule["rules"].append(data)


    # Write Json ------------------
    with open('data.json', 'w') as outfile:
        json.dump(oRule, outfile)

    #strJSON = json.dumps(oRule)
    #print("strJSON = ", strJSON)


# To be Deleted ---------------------------------------------------
def readDataInfo():
    pdData = pd.read_excel(FILE_PATH, index_col=None, header=0)  
    print("shape = ", pdData.shape)

    # To lower all the columns data ----------
    pdData = pdData.applymap(lambda s:s.lower() if type(s) == str else s)

    # To trim all column values --------------
    pdData.replace('(^\s+|\s+$)', '', regex=True, inplace=True)

    # To filter out particular Database tables only --------
    pdData=pdData[pdData['Database Name'] == DATABASE_NAME]
    print("shape = ", pdData.shape)
    print("unique groups = ", pdData['PublicationName'].nunique())


    pdGroupData = pdData.groupby(by=["PublicationName"]).count()

    print("pdGroupData = ", pdGroupData.columns)
    print("pdGroupData Shape= ", pdGroupData['Publisher_Table'])
    pdGroupData = pdGroupData['Publisher_Table']

    pdGroupData.to_csv('C:/Prashan/bitbucket/datalake/Kinesis-Stream/TableMapping/Groupinfo.csv')
   

#------------------------------------------------------------------
# Function Calls
#------------------
pdData = readData()
getCompleteMapping_JSON(pdData)

#readDataInfo()






