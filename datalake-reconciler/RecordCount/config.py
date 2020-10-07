# Lower---------
import os

RAW_BUCKET = os.environ['RAW_BUCKET']
DYNAMODB_RAW_RECONCILE_TABLE_NAME = os.environ['DYNAMODB_RAW_RECONCILE_TABLE_NAME']
KEY = 'abshire/dbo/'

# On Premise Database ----------------
'''
MS_SQL_DATABASE_HOST = "10.25.12.158"
MS_SQL_DATABASE_USER = "dmstest"
MS_SQL_DATABASE_PASSWORD = "dmstest@1234"

MS_SQL_DB_NAME = 'Abshire_test_datalake'
MS_SQL_PORT = '1433'
'''

# SD Init Database -------------------
MS_SQL_DATABASE_HOST = "qa.absodb.st.com"
MS_SQL_DATABASE_USER = "redshift-ro-user"
MS_SQL_DATABASE_PASSWORD = "fHWc45s!adC3q"

MS_SQL_DB_NAME = 'Abshire'
MS_SQL_PORT = '2410'


PG_DATABASE_HOST =  os.environ['PG_DATABASE_HOST']
PG_DATABASE_USER = os.environ['PG_DATABASE_USER']
PG_DATABASE_PASSWORD = os.environ['PG_DATABASE_PASSWORD']

PG_DB_NAME = 'grafana'
PG_PORT = '5432'

# TABLE_NAME = 'curated_inventory_metadata'


# PG_DATABASE_HOST =  'parquet-to-json-01.c1mqhgbtntjh.us-east-1.rds.amazonaws.com'
# PG_DATABASE_USER = 'postgres'
# PG_DATABASE_PASSWORD = 'postgres'


#PG_DB_NAME = 'grafana'
# PG_DB_NAME = 'inventory'
# PG_PORT = '5432'
#GRAFANA_TABLE_NAME = 'bgorders'
GRAFANA_TABLE_NAME = 'reconcile_record_count'

DB_NAME = 'abshire'

