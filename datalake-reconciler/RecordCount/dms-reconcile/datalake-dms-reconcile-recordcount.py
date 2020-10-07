import boto3
import botocore
from botocore.exceptions import ClientError
import json
from datetime import datetime, timedelta
import awswrangler as wr
import config
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy.sql import text
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd
session = boto3.Session(region_name='us-east-1')
#session = boto3.Session(region_name='us-east-1',
#                        profile_name='909045093730_PowerUserAccess')
dms = session.client('dms', region_name='us-east-1')
dynamodb = session.resource('dynamodb', region_name='us-east-1')
s3 = session.client('s3', region_name='us-east-1')
table = dynamodb.Table(config.DYNAMODB_RAW_RECONCILE_TABLE_NAME)


class DatabaseMigrationService:
    def stop_task(self, task_arn):
        try:
            response = dms.describe_replication_tasks(Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                            task_arn,
                    ]
                }])
            if response['ReplicationTasks'][0]['Status'] == "running":
                waiter = dms.get_waiter('replication_task_stopped')
                response = dms.stop_replication_task(
                    ReplicationTaskArn=task_arn
                )
                ReplicationTaskIdentifier = response['ReplicationTask']['ReplicationTaskIdentifier']
                print("...waiting for task to be stopped...")
                waiter.wait(Filters=[
                    {
                        'Name': 'replication-task-id',
                        'Values': [
                            ReplicationTaskIdentifier,
                        ]
                    },
                ],
                    # this is default for wait settings in seconds
                    WaiterConfig={
                    'Delay': 15,
                    'MaxAttempts': 60
                })
                print("task stopped...")
            else:
                print("task is already stopped...")
        except botocore.exceptions.ClientError as ex:
            error_message = ex.response['Error']['Message']
            print(error_message)
            raise
        else:
            print(json.dumps(
                dms.describe_replication_tasks(Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [
                            task_arn,
                        ]
                    }]),
                indent=2,
                default=self.json_serial
            ))

    def json_serial(self, obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, datetime):
            serial = obj.isoformat()
            return serial
        raise TypeError("Type not serializable")

    def start_task(self, task_arn, cdc_start_time):
        try:
            response = dms.describe_replication_tasks(Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                            task_arn,
                    ]
                }])
            if response['ReplicationTasks'][0]['Status'] == "stopped":
                waiter = dms.get_waiter('replication_task_running')
                response = dms.start_replication_task(
                    ReplicationTaskArn=task_arn,
                    StartReplicationTaskType='resume-processing',
                    CdcStartTime=cdc_start_time,
                    #CdcStartPosition = cdc_start_time.strftime('%Y-%m-%dT%H::%M::%S.%f')
                )
                ReplicationTaskIdentifier = response['ReplicationTask']['ReplicationTaskIdentifier']
                print("...waiting for task to start...")
                waiter.wait(Filters=[
                    {
                        'Name': 'replication-task-id',
                        'Values': [
                            ReplicationTaskIdentifier,
                        ]
                    },
                ],
                    # this is default for wait settings in seconds
                    WaiterConfig={
                    'Delay': 15,
                    'MaxAttempts': 60
                })
                print("task started...")
            else:
                print("task is already running...")
        except botocore.exceptions.ClientError as ex:
            error_message = ex.response['Error']['Message']
            print(error_message)
            raise
        else:
            print(json.dumps(
                dms.describe_replication_tasks(Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [
                            task_arn,
                        ]
                    }]),
                indent=2,
                default=self.json_serial
            ))


class OnPrem:

    def check_cdc(self):
        # for linux we use FreeTDS
        # conn_str = "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=FreeTDS&port=1433&odbc_options='TDS_Version=8.0".format(
        #     user=config.MS_SQL_DATABASE_USER,
        #     password=config.MS_SQL_DATABASE_PASSWORD,
        #     host=config.MS_SQL_DATABASE_HOST,
        #     port=config.MS_SQL_PORT,
        #     database=config.MS_SQL_DB_NAME)
        # for windows we use sql driver
        conn_str = "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=SQL+Server".format(
            user=config.MS_SQL_DATABASE_USER,
            password=config.MS_SQL_DATABASE_PASSWORD,
            host=config.MS_SQL_DATABASE_HOST,
            port=config.MS_SQL_PORT,
            database=config.MS_SQL_DB_NAME)

        engine = sqlalchemy.create_engine(conn_str, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        query = text(
            "SELECT is_cdc_enabled FROM sys.databases(nolock)"
            "WHERE is_cdc_enabled = 1 and name = :dbname"
        )

        is_cds_enabled = session.execute(
            query, {"dbname": config.MS_SQL_DB_NAME}).scalar()
        return is_cds_enabled


class DynamoDb:

    def cdc_start_time(self, db_name, table_name):
        response = table.query(
            IndexName='dbname-tablename-index',
            KeyConditionExpression=Key('dbname').eq(
                db_name) & Key('tablename').eq(table_name)
        )
        data = response["Items"]

        while 'LastEvaluatedKey' in response:
               response = table.query(
                            ExclusiveStartKey = response["LastEvaluatedKey"],
                            IndexName='dbname-tablename-index',
                            KeyConditionExpression=Key('dbname').eq(
                                db_name) & Key('tablename').eq(table_name)
                           )
               data.extend(response["Items"])
        
        df = pd.DataFrame(data)
        if not df.empty:      
           df = df.sort_values("dms_timestamp",ascending = False).head(1)
           cdc_startTime =  datetime.strptime(df.iloc[0]["dms_timestamp"], '%Y-%m-%d %H:%M:%S.%f') 
           cdc_startTime = cdc_startTime.replace(second=0, microsecond=0)  

        print(cdc_startTime)     
        return cdc_startTime


def main():   
    on_prem = OnPrem()
    is_cds_enabled = on_prem.check_cdc()   
    if is_cds_enabled:
       dynamodb = DynamoDb()
       cdc_start_time =  dynamodb.cdc_start_time(config.GRAFANA_DB_NAME,config.GRAFANA_TABLE_NAME)
       dms = DatabaseMigrationService()
       dms.stop_task(config.DMS_TASK_ARN)
       dms.start_task(config.DMS_TASK_ARN, cdc_start_time)


if __name__ == '__main__':
    main()
