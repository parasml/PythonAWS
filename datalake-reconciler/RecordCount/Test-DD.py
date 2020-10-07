import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta

import config

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('raw-reconcile-dataint')

print("table = ", table)

table_name = 'bgorders'
dms_timestamp = ('2020-09-30 09:00:00.0000', '2020-09-30 10:00:00.0000')
print("dms_timestamp = ", dms_timestamp)
response = table.query(
    IndexName='dbname-tablename-index',
    # need to include shema name in & condition
    FilterExpression=Key('dms_timestamp').between(*dms_timestamp),
    KeyConditionExpression=Key('dbname').eq(
        config.DB_NAME) & Key('tablename').eq(table_name)
)

#print("datetime.utcnow() = ", datetime.utcnow())
print("response = ", response)