
import pandas as pd
import re
import boto3
import uuid
import time
from boto3.dynamodb.conditions import Key, Attr
import time
from datetime import datetime, timedelta
import os



os.environ['AWS_ACCESS_KEY_ID']='ASIA5HJ2PKVRLDBSNBNJ'
os.environ['AWS_SECRET_ACCESS_KEY']='vPUfRn0uSnxwJUuI1k98MgkpXascP99fIK7vmH/5'
os.environ['AWS_SESSION_TOKEN']='IQoJb3JpZ2luX2VjEFsaCXVzLWVhc3QtMSJHMEUCIH5hC+ykdOHNLewDb8QGKmn6R86KILJ5v1FeFuEZQ9TqAiEAx7Etn21YvqZZPJeYGN4G/NZFG5MkHZEzwfe7/eLYPLYq3wIIRBABGgw5MDkwNDUwOTM3MzAiDNh0umHtgao/4pJOHSq8AhNZpokP743bb+xeIl0HpcU7798Rmfos1VgyXJuuWaOhrdftp24d10DuuyD9Mq6kAoBINfj5HPCAHzYHhW+7zXx9W+XBfILUx+OSNIzlH+ijbohSoTUYOxxyEDCrqH+wlvc1EhjErdKv1Jeu5Xxf+bmgy1ojkamCheZu4mg4Js5GgbGf/cBySYlH20ijXrzVGdap8MRtW6E6bbmSxyo7LJayEGsjvA7Tni3TvczIXVy6Nq6V69J+1gMZx4FqiIyDi09xnNa/ybNMvsZsMpJmnCqU2Vl7C6DNXt7bnkfe99+ElwjXg51y+UyOEsp2v9UcY0zYzFXwfsktEVWgq8NztbuNrEByB+illGkA7YEDVyp7DCN3Nhi7up/m0nOX01HpirqVQ6m26bqULUxUkLVWkSoK251QdOd0cFt1Pd8wza75+QU6owGJNCQ7XoB04ATbLJFPf3v0wGJ2FRw9xYlG9OBQW1eeeybpQ9fHCzVfZ2x1BxnQWgPOnUwnMOYq4jJUvTliQiVM/Em8K5A59ega8Fk+AUnHOUVC0Mwl3xKoJRuBLDyZP+2W2O067VCmBg8qZ3RgQYGyDGesllXZpB+mpNbt0cYhC1bnk3abytJ4kfqQt5Ij2uphh87aCizmM9997RsWEm9evx0E'

# To remove records from DD (operation 'delete') --------------------------------
def query_DD(lsDeletRecords, table, objMetaData):

    pKey = 11892432
    arrRecordId = []

    currentDateTime = datetime.utcnow()
    startTime = currentDateTime - timedelta(hours=1)
    startTime = startTime.replace(minute=0, second=0, microsecond=0)
    endTime = currentDateTime.replace(minute=0, second=0, microsecond=0)

    #dms_timestamp = (str(startTime), str(endTime))

    dms_timestamp = ('2020-08-20 14:00:00', '2020-08-20 15:00:00')

    print("dms_timestamp = ", dms_timestamp)
    

    try:
        response = table.query(
            IndexName='dbname-tablename-index',
            # need to include shema name in & condition
            #FilterExpression=Key('dms_timestamp').between(*dms_timestamp) & Key('primarykey_value').eq(int(pKey)),
            FilterExpression=Key('primarykey_value').eq(int(pKey)),
            KeyConditionExpression=Key('dbname').eq(
                objMetaData['dbname']) & Key('tablename').eq(objMetaData['tablename'])
        )
        

        data = response["Items"]
        
        
        # If the results are on Pagination basis------
        while 'LastEvaluatedKey' in response:
            #print("Inside Whileeee")
            response = table.query(
                            ExclusiveStartKey = response["LastEvaluatedKey"],
                            IndexName='dbname-tablename-index',
                            #FilterExpression=Key('dms_timestamp').between(*dms_timestamp) & Key('primarykey_value').eq(int(pKey)),
                            FilterExpression=Key('primarykey_value').eq(int(pKey)),
                            KeyConditionExpression=Key('dbname').eq(
                                objMetaData['dbname']) & Key('tablename').eq(objMetaData['tablename'])
                        )
            data.extend(response["Items"])
    
        '''
        for item in data:
            #arrRecordId.append(item['id'])
            arrRecordId.append(primarykey_value['id'])

        print("Total Item = ", len(arrRecordId))
        print("Item = ", data[0])
        '''
    except:
        print("Error in finding the record = ")

    print("ooo Length arrRecord Id = ", len(data))
    #print("---QUERY TIME %s seconds ---" % (time.time() - start_time))
    # Delete Items now ------------------------------


def source_count(self, table_name, created_datetime_column_name):
        # for linux we use FreeTDS
        conn_str="mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=FreeTDS&port=1433&odbc_options='TDS_Version=8.0".format(
            user=config.MS_SQL_DATABASE_USER,
            password=config.MS_SQL_DATABASE_PASSWORD,
            host=config.MS_SQL_DATABASE_HOST,
            port=config.MS_SQL_PORT,
            database=config.MS_SQL_DB_NAME)
        # for windows we use sql driver
        # conn_str = "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=SQL+Server".format(
        #     user=config.MS_SQL_DATABASE_USER,
        #     password=config.MS_SQL_DATABASE_PASSWORD,
        #     host=config.MS_SQL_DATABASE_HOST,
        #     port=config.MS_SQL_PORT,
        #     database=config.MS_SQL_DB_NAME)

        engine = sqlalchemy.create_engine(conn_str, echo=False)
        source_table = sqlalchemy.Table(
            table_name, sqlalchemy.MetaData(), autoload_with=engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        source_created_datetime_column = [x for x in source_table.columns if x.key.upper(
        ) == created_datetime_column_name.upper()]
        str_source_created_datetime_column = str(
            source_created_datetime_column[0].name)
        query = session.query(sqlalchemy.func.count(getattr(source_table.columns, str_source_created_datetime_column))). \
            with_hint(source_table, "with (nolock)"). \
            filter(getattr(source_table.columns, str_source_created_datetime_column).between(
                startTime, endTime))

        count = query.scalar()
        # print(query.statement)
        # query = sql_query.statement
        # mydf = pd.read_sql_query(mystring,engine)
        # query = text(
        #    "select count(*) from " + table_name+ "(nolock)"
        #     "where  created between :startTime AND :endTime"
        # )
        # query = source_table.count().with_hint(source_table, "with (nolock)")
        # result = engine.scalar(query,startTime=str(startTime),endTime=str(startTime))
        # results = engine.execute(stmt).fetchall()
        return count


#--------------------------------------------------------------------------------------------
# Function Calls
#------------------
DYNAMODB_RAW_RECONCILE_TABLE_NAME = 'raw-reconcile-dataint'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
table = dynamodb.Table(DYNAMODB_RAW_RECONCILE_TABLE_NAME)

lsDeletRecords = [11799843, 11799844, 11799845, 11799846, 11799847, 11799848]
arrRecordId = ['0084123e-dc12-11ea-bd1b-02d737a65563', '018c6248-dc1d-11ea-bd1b-02d737a65563', '03688256-dbdf-11ea-bb5b-0265bcbac1b9', '03fac65e-dc8c-11ea-88de-0ef2938136b3', '06ed651a-dc8c-11ea-88de-0ef2938136b3']

#print("---QUERY TIME %s seconds ---" % (time.time() - start_time))

objMetaData = {}
objMetaData['dbname'] = 'abshire'
#objMetaData['tablename'] = 'searchreq'
objMetaData['tablename'] = 'bgorders'

query_DD(lsDeletRecords, table, objMetaData)