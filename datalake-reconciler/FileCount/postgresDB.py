#-----------------------------------------------------------------
# To read and write metadata info in Postgresql
#--------------------------------------------------

from datetime import datetime, timedelta

import psycopg2
import config


class postgresDB:

    def __init__(self):

        #print("Inisde class postgresDB *************")

        self.postgresConn = psycopg2.connect(dbname=config.PG_DATABASE_NAME[0], host=config.PG_DATABASE_HOST, port=config.PG_DATABASE_PORT, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD)
        #print("postgrescon = ", postgrescon)

        self.cursor = self.postgresConn.cursor()

        #print(self.postgresConn.get_dsn_parameters(),"\n")


    # To get s3 files of particular hour --------------------------------------
    def getRawS3Key(self, metaDataInfo):
        

        strTablename = metaDataInfo['tablename']
        strDbName = metaDataInfo['dbname']
        currentDateTime = metaDataInfo['currentdatetime']

        strTablename  = config.convertStringToLower(strTablename)
        strDbName  = config.convertStringToLower(strDbName)

        startTime = currentDateTime - timedelta(hours=1)
        startTime = startTime.replace(minute=0, second=0, microsecond=0)
        endTime = currentDateTime.replace(minute=0, second=0, microsecond=0)

        #print("startTime = ", startTime)
        #print("endTime = ", endTime)

        postgreSQL_select_Query = """
            select distinct s3key from raw_inventory_metadata where tablename = %s AND dbname = %s AND CAST(eventtime as timestamp) BETWEEN %s AND %s
            """
     
        time_to_insert = (
            str(strTablename),
            str(strDbName),
            str(startTime),
            str(endTime)
        )


        self.cursor.execute(postgreSQL_select_Query, time_to_insert)

        postgresRawS3Key = self.cursor.fetchall()
        #print("postgresRawS3Key = ", type(postgresRawS3Key))
        #print("postgresRawS3Key = ", postgresRawS3Key)

        results = [r[0] for r in postgresRawS3Key]

        #print("Postgres Length = ", len(results))

        return results

    # Update Grafana table --------------------------------
    def insertGrafanaTable(self, metaDataInfo):

        postgresConn = psycopg2.connect(dbname=config.PG_DATABASE_NAME[1], host=config.PG_DATABASE_HOST, port=config.PG_DATABASE_PORT, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD)
        #print("postgrescon = ", postgrescon)

        cursor = postgresConn.cursor()

        postgres_insert_query = """ 
            INSERT INTO reconcile_file_count
            (tablename, dbname, source_count, destination_count, difference, created_date_time)
            VALUES (%s,%s,%s,%s,%s,%s)
            """

        record_to_insert = (
            metaDataInfo['tablename'],
            metaDataInfo['dbname'],
            metaDataInfo['source_count'],
            metaDataInfo['destination_count'],
            metaDataInfo['difference'],
            metaDataInfo['created_date_time']
        )

        cursor.execute(postgres_insert_query, record_to_insert)

        postgresConn.commit()
        #count = self.cursor.rowcount


    # Closing the connecction ---------
    def closeConnection(self):

        try:
            if(self.postgresConn):
                self.cursor.close()
                self.postgresConn.close()
                print("PostgreSQL connection is closed")
        except:
            print("Error occured while closing postgres connections")


#---------------------------------------------------
