
#---------------------------------------------------------------------
# To store default paths
#-----------------------------

import os
import re

'''
os.environ['AWS_ACCESS_KEY_ID'] = 'ASIA5HJ2PKVRJXO64WLO'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'XK96ckCTBiSKf690PsB/8lkCRrwawnaiokyBhREy'
os.environ['AWS_SESSION_TOKEN']="IQoJb3JpZ2luX2VjEIz//////////wEaCXVzLWVhc3QtMSJGMEQCICLWznWJfT7hEGnj6tJl06N/IDZD9LzZ3JZxLLHQnVTwAiAMpFLB/PLm16kE8abwKwMTjxun2v2ZqqxLuYp4M/c2qyqZAggUEAAaDDkwOTA0NTA5MzczMCIMUBacA6uaeeFcb/xMKvYBJKDbQ0mMhqVlSnWYcj0FAg8ejbBuY7l3B2y4+f9lvReZvJKHF+yWepkVYPQQAPFvzJOmjBHKyEb9YMrD8KbB3n3wi7Ryy0GhcrjQFMQomjMnw1WFJY1dGNHzHdT6s0Vh6jBowJ03zmGH72WB+lEu7R4yimy9vVEAMHRqY9KhmXgWAn8wNK7IXKUP1yCIJux2V1mHhUB192S21IeNMjJcw/MQYrsGzaKTEe8y6yu5UuqIhDBZX3k8TNgHL/JSZdYTMXTmtGHsid1egoyRTPSoJjGoYTPHafKM2IhfZU9w5D4uyFwXLSmRKkRhJzYuFG0/C3tw6PSgMIy6svcFOuoBDiTgjmaY2811wYPVp6dAAwWE2EfpZIjZirHyH7tS6dDg9WpYfcBL4gKAWXlxnNd/M55NDxo3gGflIoPRvabZIK+QZCg4VZD+HpOhrqXucsRiuidznFyBf6pCqYLFR2EIS4rQdg1n9XbJTDhrwtUq3n0o7tCG+qg6i3d0fdubHZwMPeSOG8YBY5yJHRrJ54E9FaZW8eo9lojLU5soYKaDLfkvI7+I5zViLVDDoj55qSg5DeJyjUPSeEFkT7YFCUTVLnsAoGEKAS2459NsarQ0z6D4IArO/8NTBMsLoXL5+/dxi3t0s8zp4Hvk"
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

SQS_QUEUE_INVENTORY_URL = 'https://sqs.us-east-1.amazonaws.com/909045093730/inventory-raw-incoming'
SQS_QUEUE_DEAD_INVENTORY_URL = 'https://sqs.us-east-1.amazonaws.com/909045093730/inventory-raw-incoming-dlq'

GRAFANA_SCHEMA_NAME = os.environ['GRAFANA_SCHEMA_NAME']
GRAFANA_DB_NAME = os.environ['GRAFANA_DB_NAME']
GRAFANA_TABLE_NAME = os.environ['GRAFANA_TABLE_NAME']
'''
# Lower Environment ---------------------------

RAW_BUCKET = os.environ['RAW_BUCKET']
CURATED_BUCKET = os.environ['CURATED_BUCKET']

PG_DATABASE_HOST =  os.environ['PG_DATABASE_HOST']
#PG_DATABASE_NAME = os.environ['PG_DATABASE_NAME']
#PG_DATABASE_PORT = os.environ['PG_DATABASE_PORT']
TABLE_NAME = 'curated_inventory_metadata'
SQS_QUEUE_INVENTORY_URL = os.environ['SQS_QUEUE_INVENTORY_URL']
SQS_QUEUE_DEAD_INVENTORY_URL = os.environ['SQS_QUEUE_DEAD_INVENTORY_URL']

PG_DATABASE_USER = os.environ['PG_DATABASE_USER']
PG_DATABASE_PASSWORD = os.environ['PG_DATABASE_PASSWORD']
KMS_KEY_ARN = os.environ['KMS_KEY_ARN']



# Grafana Environment variables -------------------------
GRAFANA_SCHEMA_NAME = 'dbo'  # Hardcoded right now------
#GRAFANA_SCHEMA_NAME = os.environ['GRAFANA_SCHEMA_NAME']
GRAFANA_DB_NAME = os.environ['GRAFANA_DB_NAME']
GRAFANA_TABLE_NAME = os.environ['GRAFANA_TABLE_NAME']

#---------------------------------------------------------------------
SNS_GRAPHANA= 'arn:aws:sns:us-east-1:909045093730:grafana-notification'

KEY = 'abshire/dbo/'
PG_DATABASE_PORT = '5432'
PG_DATABASE_NAME = ['inventory', 'grafana']


#--------------------------------------------------------
# To lower and remove special characters from string
#------------------------------------
def convertStringToLower(strName):
             
    strLowerNew = re.sub('[^a-zA-Z]+', '', strName) 
    strLowerNew = strLowerNew.lower()
    return strLowerNew



