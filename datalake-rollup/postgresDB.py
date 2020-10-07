#-----------------------------------------------------------------
# To read and write metadata info in Postgresql
#--------------------------------------------------

import psycopg2
import config
#import rdsSecretManager
from datetime import datetime, timedelta

class postgresDB:

    def __init__(self):

        #print("Inisde class metadataInfo")
        '''
        secret = rdsSecretManager.get_secret()
        print("secret = ", secret)

        oSecret = eval(secret)
        '''
        #self.rollupTime = rollupTime
        #self.LEVEL = LEVEL

        self.postgresConn = psycopg2.connect(dbname=config.PG_DATABASE_NAME, host=config.PG_DATABASE_HOST, port=config.PG_DATABASE_PORT, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD)
        #self.postgresConn = psycopg2.connect(dbname=config.PG_DB_NAME, host=config.PG_ENDPOINT, port=config.PG_PORT, user=oSecret['username'], password=oSecret['password'])
        #print("postgrescon = ", postgrescon)

        self.cursor = self.postgresConn.cursor()

        #print(self.postgresConn.get_dsn_parameters(),"\n")

    # To update Inventory metadata table----------------------------------------------------
    def insert_Rollup_Data(self, metaDataInfo):

        #print("Function insert_Rollup_Data")

        #print("metaDataInfo = ", metaDataInfo)

        postgres_insert_query = """ 
            INSERT INTO curated_consolidated_metadata
            (level, eventtime, s3key, tablename, dbname, filesadded, filesize, totalcolumns, totalrows)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

        record_to_insert = (
            metaDataInfo['level'],
            metaDataInfo['eventtime'],
            metaDataInfo['file'],
            #metaDataInfo['s3key'],
            metaDataInfo['table'],
            metaDataInfo['dbname'],
            metaDataInfo['totalCount'],
            metaDataInfo['filesize'],
            metaDataInfo['totalColumns'],
            metaDataInfo['totalRows'],
        )

        self.cursor.execute(postgres_insert_query, record_to_insert)

        self.postgresConn.commit()
        #count = self.cursor.rowcount

    # Closing the connecction ------------------------------------------------------------
    def closeConnection(self):

        try:
            if(self.postgresConn):
                self.cursor.close()
                self.postgresConn.close()
                print("PostgreSQL connection is closed")
        except:
            print("Error occured while closing postgres connections")






