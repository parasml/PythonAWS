import boto3
import config

client = boto3.client('glue')


def getCurrentTableInfo():
    response = client.get_tables(
        DatabaseName='abshire',
        #NextToken='nextToken',
        MaxResults=120
    )

    lsTableList = response['TableList']

    for objTable in lsTableList:

        
        print("Table Name = ", objTable['Name'])
        lsColumnsList = objTable['StorageDescriptor']['Columns']
        #print("lsColumnsList = ", lsColumnsList)
        #print("Total Columns = ", len(lsColumnsList))

        for objColumn in lsColumnsList:

            try:
                if objColumn['Comment'] == 'primary-key':

                    print("Primary Key Column Name = ", objColumn['Name'])

            except:
                pass
            
        print("---------------------")

    #print("response = ", response)
    #print("response = ", response['TableList'][0])
    #print("response = ", len(response['TableList']))




#-----------------------------------------------------------------
# Function Calls
#------------------
getCurrentTableInfo()