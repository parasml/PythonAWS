import awswrangler as wr
import pandas as pd
import urllib
import os
import boto3
import json
from datetime import datetime
from dateutil.parser import parse
from botocore.exceptions import ClientError
import base64
import config

session = boto3.Session(region_name="us-east-1")
# session = boto3.session.Session()
# session = boto3.session.Session(profile_name='909045093730_PowerUserAccess')
# session = boto3.Session(
#     aws_access_key_id='ASIA5HJ2PKVRIGEVT45R',
#     aws_secret_access_key='5z8QGjZbBYeHBevL2B97X4t8kJ/lp2pPwQMFPYuE',
#     aws_session_token='IQoJb3JpZ2luX2VjECwaCXVzLWVhc3QtMSJIMEYCIQCESm40mGL+NaAl4ykcNY6sWp8N0PVb8LkG7Lak1OE46gIhALFkOWArhIr3fy/ez9z/QdKtKjb9VzMzXzTgGsqLV6LTKpcCCHUQABoMOTA5MDQ1MDkzNzMwIgwWoGHKkp2ygo0VD3oq9AGKWW+YigcKmYx/Hub0CJ6WV/ushgAht0t6DJvKrp4si7Zo6a09V396SePswt1I8ySXs4kL+bxTNnuSZNzmDhK5nbC2JDjmnw9oaJ799v9h58n0MFdfbd29R0JrmVPHTAlDrT+Ppmywms2j/xQ2/D7aDGUguPfFnxc/El6E1slRmIZnB65HVepLLQ60wWSGkpW5/IgguVGwQLwQrb0gXrmr9euQGjJpoTTwyezXXF4IeMv/S72q3+7oLVvS64vBSHUk/te7oCaCi8yGYDr4gbNLI2fAMVxC4hAmdmwE1PRjJB5wsKUu8e7fIuIjIpuxTJCwgiAoMLHa9PUFOugBlN5ynwdCoT5b7f7N6pf/fpot7JQbgcQqhA2yZX3lN8yTIDuiRMf+XtEKTQxsTPrIQGmDoJ2kBWeo7kVgJzx5/TgfptwHwsoz+0Uh7X98dNidW/cXkI5OHtgo68hghJqllgCudswovq1uz2qa2ObKl4C0Fwg+VIAsLWOO/uckJRkgJl4VPx2ThM52WSNMpfjFLzgqsrQUaVBAKm4ZXWMVUmBztXLoKpaouab0srAp6nyFRkb57oVYl/58wLnGgsabZOpXfVvoSM3dqb+o2pbT+sSWTSMOvDIb9eH+oN+METdAlKjJYRMqGg=='
# )
sqs_client = session.client(service_name='sqs', region_name='us-east-1')
# sqs_client = boto3.client('sqs')


def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """

    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        try:
            yield from resp['Messages']
        except KeyError:
            return

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )


def main():

    while True:
        try:
            for message in get_messages_from_queue(config.SQS_QUEUE_URL):
                json_message = json.loads(message['Body'].replace("\'", "\""))
                source_bucket = json_message['s3']['bucket']['name']
                key = urllib.parse.unquote_plus(
                    json_message['s3']['object']['key'])
                df = wr.s3.read_json(
                    path='s3://' + source_bucket + '/'+key, lines=True, boto3_session=session)
                # print("df = ", df.shape)
                filename = os.path.basename(key)
                filenam_no_extension = os.path.splitext(filename)[0]
                foldername = os.path.dirname(key)
                date_time_obj = parse(json_message['eventTime'])
                dt_string = date_time_obj.strftime(
                    "year=%Y/month=%m/day=%d/hour=%H")

                if foldername:
                    path = 's3://' + config.CURATED_BUCKET+'/'+'curated' + '/' + \
                        foldername+'/'+dt_string + '/'+filenam_no_extension+'.parquet'
                else:
                    path = 's3://' + config.CURATED_BUCKET+'/'+'curated'+'/'+dt_string + \
                        '/'+filenam_no_extension+'.parquet'

                # Convert json to parquet--------
                wr.s3.to_parquet(
                    df=df,
                    path=path,
                    boto3_session=session,
                    s3_additional_kwargs={
                        "ServerSideEncryption": "aws:kms",
                        "SSEKMSKeyId": config.KMS_KEY_ARN
                        }
                )

                engine = wr.db.get_engine(db_type='postgresql', host=config.PG_DATABASE_HOST,
                                          port=config.PG_PORT, database=config.PG_DB_NAME, user=config.PG_DATABASE_USER, password=config.PG_DATABASE_PASSWORD
                                          )

                # engine = wr.db.get_engine(db_type='postgresql', host='parquet-to-json-01.c1mqhgbtntjh.us-east-1.rds.amazonaws.com',
                #                           port='5432', database='inventory', user='postgres', password='postgres')

                df_metadata = pd.DataFrame({'eventtime': json_message['eventTime'], 'filename': filename, 's3key': key,
                                            'tablename': foldername, 'filesize':  json_message['s3']['object']['size'], 'filetype': 'json'}, index=[0])
                wr.db.to_sql(df=df_metadata,
                             con=engine,
                             name=config.TABLE_NAME,
                             schema="public",
                             if_exists='append',
                             index=False
                             )

        except Exception as e:
            print(e)
            raise e


if __name__ == "__main__":
    main()
