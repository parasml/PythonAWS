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

session = boto3.Session(region_name='us-east-1')
# session = boto3.Session(region_name='us-east-1',
#                         profile_name='909045093730_PowerUserAccess')
dynamodb = session.resource('dynamodb', region_name='us-east-1')
s3 = session.client('s3', region_name='us-east-1')
table = dynamodb.Table(config.DYNAMODB_RAW_RECONCILE_TABLE_NAME)
currentDateTime = datetime.utcnow()
startTime = currentDateTime - timedelta(hours=1)
startTime = startTime.replace(minute=0, second=0, microsecond=0)
endTime = currentDateTime.replace(minute=0, second=0, microsecond=0)
# print(startTime)
# print(endTime)

class Metadata:

    def get_databases(self):
        dbs = wr.catalog.get_databases(boto3_session=session)
        return dbs

    def get_tables(self, dbname):
        tables = wr.catalog.get_tables(database=dbname, boto3_session=session)
        return tables

    def get_created_datetime_column_name(self, dbname, tablename):
        table = wr.catalog.table(
            database=dbname, table=tablename, boto3_session=session)
        #colPrimaryKey = table[table['Comment'] == 'primary-key']['Column Name'].values[0]
        colRecordDateTime = table[table['Comment'] == 'record-create-time']['Column Name'].values[0]
        return colRecordDateTime
        #print("colPrimaryKey = ", colPrimaryKey)
        #print("colRecordDateTime = ", colRecordDateTime)

    # def GetTableFromCatalog(Self):

    # To read S3 table folder -------------------------------------

    # def ReadFolders(self):
    #     lsTablePaths = []
    #     result = s3.list_objects(
    #         Bucket=config.RAW_BUCKET, Prefix=config.KEY, Delimiter='/')
    #     # print("result = ", result)

    #     for tableName in result.get('CommonPrefixes'):
    #         # print('sub folder : ', tableName.get('Prefix'))
    #         # lsTablePaths.append(tableName.get('Prefix'))
    #         strPath = tableName.get('Prefix')
    #         lsStr = strPath.split("/")
    #         strTableName = lsStr[-2]
    #         lsTablePaths.append(strTableName)

    #     # print("lsTablePaths = ", lsTablePaths)
    #     return lsTablePaths

    def destination_count(self, db_name, table_name):
        dms_timestamp = (str(startTime), str(endTime))
        response = table.query(
            IndexName='dbname-tablename-index',
            # need to include shema name in & condition
            FilterExpression=Key('dms_timestamp').between(*dms_timestamp),
            KeyConditionExpression=Key('dbname').eq(
                db_name) & Key('tablename').eq(table_name)
        )
        data = response["Items"]

        while 'LastEvaluatedKey' in response:
            response = table.query(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                IndexName='dbname-tablename-index',
                FilterExpression=Key(
                    'dms_timestamp').between(*dms_timestamp),
                KeyConditionExpression=Key('dbname').eq(
                    db_name) & Key('tablename').eq(table_name))
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

    def source_count(self, table_name, created_datetime_column_name):

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

        engine = sqlalchemy.create_engine(conn_str, echo=False)
        # source_table = sqlalchemy.Table(
        #     table_name, sqlalchemy.MetaData(), autoload_with=engine)
        table_name = "dbo_"+table_name+"_CT"
        print("table_name = ", table_name)
        source_table = sqlalchemy.Table(
            table_name, sqlalchemy.MetaData(), autoload_with=engine, schema='cdc')

        Session = sessionmaker(bind=engine)
        session = Session()

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
                str_source_start_lsn_column, startTime, endTime)))
            #filter(getattr(source_table.columns, str_source_operation_column) == 2). \
           
        # filter(func.sys.fn_cdc_map_lsn_to_time(getattr(source_table.columns, str_source_start_lsn_column)).between(
        #     startTime, endTime))

        # source_created_datetime_column = [x for x in source_table.columns if x.key.upper(
        # ) == created_datetime_column_name.upper()]
        # str_source_created_datetime_column = str(
        #     source_created_datetime_column[0].name)
        # query = session.query(sqlalchemy.func.count(getattr(source_table.columns, str_source_created_datetime_column))). \
        #     with_hint(source_table, "with (nolock)"). \
        #     filter(getattr(source_table.columns, str_source_created_datetime_column).between(
        #         startTime, endTime))

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
        #print(count)
        return count

    def insert_metadata_grafana(self, strTable, created_datetime_column_name):
        engine_grafana = wr.db.get_engine(db_type='postgresql', host=config.PG_DATABASE_HOST,
                                          port=config.PG_PORT, database=config.PG_DB_NAME, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD
                                          )
        dt = datetime.utcnow()
        df_metadata = pd.DataFrame({'dbname': 'abshire', 'tablename': strTable, 'source_count': self.source_count(table_name=strTable, created_datetime_column_name=created_datetime_column_name),
                                    'destination_count': self.destination_count(db_name='abshire', table_name=strTable),
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


def reconcile_record():
    metadata = Metadata()
    dbs = metadata.get_databases()
    for dbname in dbs:

        print("dbname['Name'] = ", dbname['Name'])
        if dbname['Name'] == 'abshire':
            tables = metadata.get_tables(dbname['Name'])
            for tablename in tables:
                created_datetime_column_name = metadata.get_created_datetime_column_name(
                    dbname['Name'], tablename['Name'])
                metadata.insert_metadata_grafana(
                    tablename['Name'].lower(), created_datetime_column_name)

    # lsTablePaths=metadata.ReadFolders()
    # for strTable in lsTablePaths:
    #     metadata.insert_metadata_grafana(strTable.lower())

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == "__main__":
    print("------------------")
    reconcile_record()
