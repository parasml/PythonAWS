import os
# Lower---------

RAW_BUCKET = os.environ['RAW_BUCKET']
KEY = 'abshire/dbo/'
MS_SQL_DATABASE_HOST = "10.25.12.158"
MS_SQL_DATABASE_USER = "dmstest"
MS_SQL_DATABASE_PASSWORD = "dmstest@1234"

MS_SQL_DB_NAME = 'Abshire_test_datalake'
MS_SQL_PORT = '1433'

PG_DATABASE_HOST =  os.environ['PG_DATABASE_HOST']
#print(PG_DATABASE_HOST)
PG_DATABASE_USER = os.environ['PG_DATABASE_USER']
#print(PG_DATABASE_USER)
PG_DATABASE_PASSWORD = os.environ['PG_DATABASE_PASSWORD']
GRAFANA_TABLE_NAME = 'reconcile_record_count'
PG_DB_NAME = 'grafana'
PG_PORT = '5432'

DYNAMODB_RAW_RECONCILE_TABLE_NAME = os.environ['DYNAMODB_RAW_RECONCILE_TABLE_NAME']

GRAFANA_DB_NAME =  os.environ['GRAFANA_DB_NAME']
#print(GRAFANA_DB_NAME)
GRAFANA_TABLE_NAME = os.environ['GRAFANA_TABLE_NAME']
#print(GRAFANA_TABLE_NAME)
DMS_TASK_ARN = os.environ['DMS_TASK_ARN'] 
#print(DMS_TASK_ARN)
# PG_DATABASE_HOST =  'parquet-to-json-01.c1mqhgbtntjh.us-east-1.rds.amazonaws.com'
# PG_DATABASE_USER = 'postgres'
# PG_DATABASE_PASSWORD = 'postgres'


# PG_DB_NAME = 'grafana'
# PG_PORT = '5432'
#GRAFANA_TABLE_NAME = 'bgorders'


