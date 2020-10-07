#-----------------------------------------------------------------
# Called from: ECS_handler.py, splitparquet.py
# To read and write metadata info in Postgresql
#--------------------------------------------------

import psycopg2
import config
#import rdsSecretManager

class postgresDB:

    def __init__(self):

        self.postgresConn = psycopg2.connect(dbname=config.PG_DATABASE_NAME, host=config.PG_DATABASE_HOST, port=config.PG_DATABASE_PORT, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD)
        #print("postgrescon = ", postgrescon)

        self.cursor = self.postgresConn.cursor()
        #print(self.postgresConn.get_dsn_parameters(),"\n")


    # To update Inventory metadata table------------
    def insert_Raw_InventoryData(self, metaDataInfo):

        #print("Function getInventoryTable")

        postgres_insert_query = """ 
            INSERT INTO raw_inventory_metadata
            (eventtime, s3key, tablename, filesize, filetype, dbname)
            VALUES (%s,%s,%s,%s,%s,%s)
            """

        record_to_insert = (
            metaDataInfo['eventtime'],
            metaDataInfo['s3key'],
            metaDataInfo['tablename'],
            metaDataInfo['filesize'],
            metaDataInfo['filetype'],
            metaDataInfo['dbname']
        )

        self.cursor.execute(postgres_insert_query, record_to_insert)

        self.postgresConn.commit()
        #count = self.cursor.rowcount


    # To update Inventory metadata table----------------------------
    def insert_Curated_InventoryData(self, metaDataInfo):

        #print("Function getInventoryTable")

        postgres_insert_query = """ 
            INSERT INTO curated_inventory_metadata
            (eventtime, s3key, tablename, filesize, filetype, dbname)
            VALUES (%s,%s,%s,%s,%s,%s)
            """

        record_to_insert = (
            metaDataInfo['eventtime'],
            metaDataInfo['curated_s3key'],
            metaDataInfo['tablename'],
            metaDataInfo['filesize'],
            metaDataInfo['filetype'],
            metaDataInfo['dbname']
        )

        self.cursor.execute(postgres_insert_query, record_to_insert)

        self.postgresConn.commit()
        #count = self.cursor.rowcount


     # To check if the record already exists----------------------------
    def chekIfEntryExists(self, s3keyVal):

        from datetime import datetime, timedelta
        yesterdayDate = str(datetime.utcnow().date() - timedelta(days=1))

        #sql_select_query = """select * from raw_inventory_metadata where s3key = %s"""
        sql_select_query = """select * from raw_inventory_metadata where s3key = %s AND CAST(eventtime as date) >= %s"""
        self.cursor.execute(sql_select_query, (s3keyVal, yesterdayDate))
        record = self.cursor.fetchone()
        #print("record = ", record)
        #print("record = ", type(record))
        
        if record is not None:
            return True
        else:
            return False
    

    # Closing the connecction ----------------------------------------
    def closeConnection(self):

        try:
            if(self.postgresConn):
                self.cursor.close()
                self.postgresConn.close()
                print("PostgreSQL connection is closed")
        except:
            print("Error occured while closing postgres connections")





