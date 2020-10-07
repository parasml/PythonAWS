
#---------------------------------------------------------------------
# To store default paths
#-----------------------------

import os
import re

'''
os.environ['AWS_ACCESS_KEY_ID'] = 'ASIA5HJ2PKVRPABKBXH5'
os.environ['AWS_SECRET_ACCESS_KEY'] = '48UyHdZQaFW03OuVKz0v6yWfVTE6Bu+pUEzpeHD3'
os.environ['AWS_SESSION_TOKEN']="IQoJb3JpZ2luX2VjEGAaCXVzLWVhc3QtMSJIMEYCIQDDaEM7JkzazXufCOOSC6XsPhnKIdsb7ZB+MFWILUTHjQIhAIncZrPl6afpK+A1Y8EEYW3ZucXdgv6gsZ0S/NXQLcWMKqICCNn//////////wEQABoMOTA5MDQ1MDkzNzMwIgwsbFgRY0sjldQOb0Aq9gHSONtPmf4Huw4OdsarfpJO6kjK5uZ2GlIHtQkIzWxdXNh6mpdQ3BlYq2XvICfR8RWD8ZwzgNEfV8ImC3qnaatm0h62WUtfY2ZYUapLmPaz9B96fl/Sm5c8hPOxUEeAT+RgE6dXfE6TGMaqz2Do+EfFJLLpX7wDIl6GLTdV/6KVoKVWVyoeL4DfNKY098rgxlX0SQPK6cs6PbS7oiLSvi2yO0vG8IwK9bps6HPXvOZ3fHqboVpLYGXuj1yvZr98xy9rHEymZF3Bx6G4hNYZiXl6us56RyjOFcz1LAXM2RWC6+UUC7Agrtl84E10DKX4K95dQeSoPAEw5/So9wU66AEWK918KB12sp7EbCmTCSZQNFxdHBZeFMnQcSeIycgXEXDgR3jVypsyOroHPk6R/GMjEkfj7gy3qyg9M2qa2AryDaU0DoHjaEqj4cRYCKcuXvwHmV6Y/iliOqDT4V2yDTyMLKIuqVMBx2DIINacp93DkCq0Ck91WhElVC8RRaORtyMaO9QvZ4vn6yndli1coovVprFOa9HgxQhB+eMt9CBwgjCC2i17BGqxyorMb2EnF+JPVqOEXiYhG9ibGHPke/k3scXRShyPtJiHJpIfB0aba6d+pl6ofMj8Acyz5UpcT1/E/FB56s5M"
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
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
