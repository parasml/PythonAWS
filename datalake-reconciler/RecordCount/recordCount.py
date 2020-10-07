#!/usr/bin/env python
"""Reconciliation of source (sql server) and destination (dynamodb)
It gets the count from source sql server and dynamodb and inserts the
metadata in postgres db for grafana monitoring
"""
import boto3
from boto3.dynamodb.conditions import Key, Attr
import sqlalchemy
import config
import pyodbc
# from sqlalchemy.sql import text,and_
import pandas as pd
import config
from datetime import datetime, timedelta
import awswrangler as wr
import logging
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, text
import platform
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# print(startTime)
# print(endTime)

class recordCount:

    def __init__(self, currentDateTime):

        # Time -------
        self.currentDateTime = currentDateTime
        self.startTime = currentDateTime - timedelta(hours=1)
        self.startTime = self.startTime.replace(minute=0, second=0, microsecond=0)
        self.endTime = currentDateTime.replace(minute=0, second=0, microsecond=0)

        print("self.startTime = ", self.startTime)
        print("self.endTime = ", self.endTime)

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

        #s3 = session.client('s3', region_name='us-east-1')
        self.DD_table = dynamodb.Table(config.DYNAMODB_RAW_RECONCILE_TABLE_NAME)

        tables = wr.catalog.get_tables(database=config.DB_NAME)

        
        #for tablename in tables:

        tablename = {}
        tablename['Name'] = 'bgorders'

        created_datetime_column_name = self.get_created_datetime_column_name(tablename['Name'])

        print("created_datetime_column_name = ", created_datetime_column_name)

        #print("tablename = ", tablename['Name'])
        #self.source_count = self.getSourceCount(tablename['Name'], created_datetime_column_name)
        self.destination_count = self.getDestinationCount(tablename['Name'])
        #self.insertFileCountData(tablename['Name'])
    

    def get_created_datetime_column_name(self, tablename):

        table = wr.catalog.table(
            database=config.DB_NAME, table=tablename)
        #colPrimaryKey = table[table['Comment'] == 'primary-key']['Column Name'].values[0]
        colRecordDateTime = table[table['Comment'] ==
                                  'record-create-time']['Column Name'].values[0]
        return colRecordDateTime


    # Destination is our DynamoDB count (Inserts and Deletes)--------
    def getSourceCount(self, table_name, created_datetime_column_name):

        if platform.system() == "Linux":
            # for linux we use FreeTDS
            conn_str = "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=FreeTDS&port=1433&odbc_options='TDS_Version=8.0".format(
                user=config.MS_SQL_DATABASE_USER,
                password=config.MS_SQL_DATABASE_PASSWORD,
                host=config.MS_SQL_DATABASE_HOST,
                port=config.MS_SQL_PORT,
                database=config.MS_SQL_DB_NAME)
        elif platform.system() == "Windows":
            # for windows we use sql driver
            conn_str = "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=SQL+Server".format(
                user=config.MS_SQL_DATABASE_USER,
                password=config.MS_SQL_DATABASE_PASSWORD,
                host=config.MS_SQL_DATABASE_HOST,
                port=config.MS_SQL_PORT,
                database=config.MS_SQL_DB_NAME)

        print("conn_str = ", conn_str)
        engine = sqlalchemy.create_engine(conn_str, echo=False)
        # source_table = sqlalchemy.Table(
        #     table_name, sqlalchemy.MetaData(), autoload_with=engine)
        #    table_name = "dbo_"+table_name+"_CT"
        print("table_name = ", table_name)
        source_table = sqlalchemy.Table(
            table_name, sqlalchemy.MetaData(), autoload_with=engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        '''
        source_operation_column = [x for x in source_table.columns if x.key.upper(
        ) == "__$OPERATION"]
        str_source_operation_column = str(
            source_operation_column[0].name)
        source_start_lsn_column = [x for x in source_table.columns if x.key.upper(
        ) == "__$START_LSN"]
        str_source_start_lsn_column = str(
            source_start_lsn_column[0].name)

        query = session.query(sqlalchemy.func.count(getattr(source_table.columns, str_source_operation_column))). \
            with_hint(source_table, "with (nolock)"). \
            filter(text("DATEADD(second, DATEDIFF(second, GETDATE(), GETUTCDATE()), sys.fn_cdc_map_lsn_to_time(%s)) between '%s' and '%s'" % (
                str_source_start_lsn_column, self.startTime, self.endTime)))
            #filter(getattr(source_table.columns, str_source_operation_column) == 2). \
        '''
        # filter(func.sys.fn_cdc_map_lsn_to_time(getattr(source_table.columns, str_source_start_lsn_column)).between(
        #     startTime, endTime))

        source_created_datetime_column = [x for x in source_table.columns if x.key.upper(
        ) == created_datetime_column_name.upper()]
        str_source_created_datetime_column = str(
            source_created_datetime_column[0].name)
        
        query = session.query(sqlalchemy.func.count(getattr(source_table.columns, str_source_created_datetime_column))). \
            with_hint(source_table, "with (nolock)"). \
            filter(getattr(source_table.columns, str_source_created_datetime_column).between(
                str(self.startTime), str(self.endTime)))
        

        '''
        query = session.query(sqlalchemy.func.count(getattr(source_table.columns, str_source_created_datetime_column))). \
            with_hint(source_table, "with (nolock)"). \
            filter(getattr(source_table.columns, str_source_created_datetime_column).between('2020-09-30 08:00:00', '2020-09-30 10:00:00'))
        '''

        print("query = ", query)
        print("self.startTime = ", self.startTime)
        print("self.endTime = ", self.endTime)

        count = query.scalar()
        # print(query.statement)
        # print(startTime)
        # print(endTime)
        # query = sql_query.statement
        # mydf = pd.read_sql_query(mystring,engine)
        # query = text(
        #    "select count(*) from " + table_name+ "(nolock)"
        #     "where  created between :startTime AND :endTime"
        # )
        # query = source_table.count().with_hint(source_table, "with (nolock)")
        # result = engine.scalar(query,startTime=str(startTime),endTime=str(startTime))
        # results = engine.execute(stmt).fetchall()
        print(count)
        return count


    # Source is abshire SQL databse (Only Inserts)---------
    def getDestinationCount(self, table_name):

        print("===========")
        #print("self.DD_table = ", self.DD_table)
        dms_timestamp = (str(self.startTime), str(self.endTime))
        print("dms_timestamp = ", dms_timestamp)
        response = self.DD_table.query(
            IndexName='dbname-tablename-index',
            # need to include shema name in & condition
            FilterExpression=Key('dms_timestamp').between(*dms_timestamp),
            KeyConditionExpression=Key('dbname').eq(
                config.DB_NAME) & Key('tablename').eq(table_name)
        )
        data = response["Items"]

        while 'LastEvaluatedKey' in response:
            response = self.DD_table.query(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                IndexName='dbname-tablename-index',
                FilterExpression=Key(
                    'dms_timestamp').between(*dms_timestamp),
                KeyConditionExpression=Key('dbname').eq(
                    config.DB_NAME) & Key('tablename').eq(table_name))
            data.extend(response["Items"])

        #df = pd.DataFrame(data)
        # need to change below with primary key
        # print(df)
        #df.to_csv('dynamodata.csv', index=False)
        #print(df['primarykey_value'].count())
        #print(df['primarykey_value'].nunique())
        #return df['primarykey_value'].nunique()  #----PPP
        return response['Count']
        # response = table.query(
        #     IndexName='dbname-tablename-index',
        #     FilterExpression= Key('dms_timestamp').between(*dms_timestamp),
        #     KeyConditionExpression=Key('dbname').eq(db_name) & Key('tablename').eq(table_name) ,
        #     Select='COUNT'
        # )
        # return response['Count']

    def insertFileCountData(self, strTable):
        engine_grafana = wr.db.get_engine(db_type='postgresql', host=config.PG_DATABASE_HOST,
                                          port=config.PG_PORT, database=config.PG_DB_NAME, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD
                                          )
        dt = datetime.utcnow()
        df_metadata = pd.DataFrame({'dbname': 'abshire', 'tablename': strTable, 'source_count': self.source_count,
                                    'destination_count': self.destination_count,
                                    'created_date_time': str(dt)}, index=[0])
        df_metadata["difference"] = df_metadata['source_count'] - \
            df_metadata['destination_count']
        # print(df_metadata)
        # autogenerate key required in database for this insert to work
        wr.db.to_sql(df=df_metadata,
                     con=engine_grafana,
                     name=config.GRAFANA_TABLE_NAME,
                     schema="public",
                     if_exists='append',
                     index=False
                     )
        logger.info('insert_metadata_grafana succeeded')


