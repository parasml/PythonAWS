#---------------------------------------------------------------------
# To store default paths
#-----------------------------

import os

'''
os.environ['AWS_ACCESS_KEY_ID'] = 'ASIA5HJ2PKVRCDWDJWEP'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'CNPn5H00URetHEBe/VxIORm/xtjKALsoZL3q0fpt'
os.environ['AWS_SESSION_TOKEN']="IQoJb3JpZ2luX2VjEIz//////////wEaCXVzLWVhc3QtMSJHMEUCIQDZI4Dd1GTVZg6/xlw9mBiThd9UUWQ3HluCYuYXmi1J+AIgZtIJinnpOuhqf0EBjLzgijPGtqbl5KLBQLMpThdQkuMqogII1f//////////ARAAGgw5MDkwNDUwOTM3MzAiDAciwHMyhhBCWBi3qyr2Afs2nZ/8MdUPv3P3apdetFl9vBNqrpRunrTo358EiTAC1Z6jdHnYT7NPINbs9yZNlRct+LoVW6LWBJUPy5OonLJjmmmtwkgEG10nnKh39OeYIfQrj6MNvAl2Cd9UUncuHZr0udMTEmmIj7CVzGNUtunbow9m5ukXCtSdmiXfaqOly7rjjlXXy+j5RUDskzuc1t7ogFsSYK07bTtKLc6mryRgXxdMuf44uO9WdnbuesHJQPlKWPJM7QsWdTtAbGCZ/dC6ZEhtAVM3KfYnZQwfTEpCGWF3jMcEbKhxWZWb8z5XGT3mFGJ3mDHucG1+y7gwwql40xCY9jCY5on2BTrpATgGRPzTbecPYLrRxRwqjKEWkQUhjgdYAiMKOuZyB4bUMEYq6qkCjVzhV6iAoNzAPQRois9COTqVqSPIYUfJpjxidhn+4b9n9WR/fHogp5G+5QET1t7ZEq/vDYtOileiyZ9P1Uv1mxAbfF4WSyas68kbipCPrLgAMcio9YM2LVAyG5UHjH7cIxhgevdZKgq+I7hY66msZPWdfunCMovFE+lBiPUpmRmKO3xymUPep3+/68aFDtl40D4Bf8N2U2sTUMRLgEwQc5qKvt22iB6EFbxz7eTH7Pd6WOgua7urBi2AAyIS7gInJkMc"
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
'''

# Lower Environment ---------------------------

RAW_BUCKET = os.environ['RAW_BUCKET']
CURATED_BUCKET = os.environ['CURATED_BUCKET']

PG_DATABASE_HOST =  os.environ['PG_DATABASE_HOST']
#PG_DATABASE_NAME = os.environ['PG_DATABASE_NAME']
#PG_DATABASE_PORT = os.environ['PG_DATABASE_PORT']
TABLE_NAME = 'curated_inventory_metadata'
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

PG_DATABASE_USER = os.environ['PG_DATABASE_USER']
PG_DATABASE_PASSWORD = os.environ['PG_DATABASE_PASSWORD']
KMS_KEY_ARN = os.environ['KMS_KEY_ARN']

#---------------------------------------------------------------------

# Lower---------
#BUCKET = 'sterling-datalake-curated-data-lower-us-east-1'
KEY = 'abshire/dbo/'

'''
# Postgres SQL DB details -------------------
PG_ENDPOINT = 'inventory-db.data.int.backgroundcheck.com'
PG_DB_NAME = 'inventory'
PG_PORT = '5432'
'''

PG_DATABASE_PORT = '5432'
PG_DATABASE_NAME = 'inventory'

#---------------------------------------------
MAX_SIZE = int('256') - 10       # MB
#MAX_SIZE = int('5')               # MB

# This is used while calculating the time at which rollup needs to be done--------------
BUFFER_HOUR = 30  # value in minutes
BUFFER_DAY = 20  # Value in hours
BUFFER_MONTH = 20   # Value in days

#RollUP_LEVEL = "hour"  #Default Value (Need to commect)------