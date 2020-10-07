import os
# Lower---------


RAW_BUCKET = os.environ['RAW_BUCKET']
CURATED_BUCKET = os.environ['CURATED_BUCKET']
PG_DATABASE_HOST =  os.environ['PG_DATABASE_HOST']
PG_DATABASE_USER = os.environ['PG_DATABASE_USER']
PG_DATABASE_PASSWORD = os.environ['PG_DATABASE_PASSWORD']
KMS_KEY_ARN = os.environ['KMS_KEY_ARN']

SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
PG_DB_NAME = 'inventory'
PG_PORT = '5432'
TABLE_NAME = 'curated_inventory_metadata'


#PG_DATABASE_HOST = 'inventory-db.data.int.backgroundcheck.com'

