#-----------------------------------------------------------------------------
# Update Catalog table, skipping glue crawler
#------------------------------------------------
import json
import boto3

import config
import pandas as pd


client = boto3.client('glue') 
FILE_PATH = 'C:/Prashan/bitbucket/datalake/Kinesis-Stream/TableMapping/Abshire_Replication_Table.xlsx'
DATABASE_NAME = 'abshire'

def readData():
    pdData = pd.read_excel(FILE_PATH, index_col=None, header=0)  
    print("shape = ", pdData.shape)

    # To lower all the columns data ----------
    pdData = pdData.applymap(lambda s:s.lower() if type(s) == str else s)

    # To trim all column values --------------
    pdData.replace('(^\s+|\s+$)', '', regex=True, inplace=True)

    # To filter out particular Database tables only --------
    pdData=pdData[pdData['Database Name'] == DATABASE_NAME]


    pdData = pdData[['Database Name', 'Publisher_Table', 'Primary Key']]

    print("shape = ", pdData.shape)

    for index, row in pdData.iterrows():
        updatePrimaryKey(row['Database Name'], row['Publisher_Table'], row['Primary Key'])


    #return pdData



def updatePrimaryKey(DATABASE, TABLE, PRIMARY_KEY):

    try:
        # Read Catalog table-----------------------
        response = client.get_table(DatabaseName=DATABASE , Name=TABLE)
    except:
        print("Table Not found: ", TABLE)
        return



    old_table = response['Table']

    #print("old_table = ", old_table)

    # To update previous table with new columns------------------

    field_names = [
      "Name",
      "Description",
      "Owner",
      "LastAccessTime",
      "LastAnalyzedTime",
      "Retention",
      "StorageDescriptor",
      "PartitionKeys",
      "ViewOriginalText",
      "ViewExpandedText",
      "TableType",
      "Parameters"
    ]

    # dictionary creation to update existing table-----

    new_table = dict()
    for key in field_names:
        if key in old_table:
            #print("key = ", key)
            new_table[key] = old_table[key]

    
    #print("Columns = ", new_table['StorageDescriptor']['Columns'])

    lsColumns = new_table['StorageDescriptor']['Columns']

    for column in lsColumns:
    
        if PRIMARY_KEY == column['Name']:

            column['Comment'] = 'primary-key'



    response = client.update_table(DatabaseName = DATABASE, TableInput=new_table)

    


#-------------------------------------------------------------------
readData()
#updatePrimaryKey('abshire', 'staff', 'staffid')
