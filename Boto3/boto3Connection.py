#----------------------------------------------------------
# To connect various AWS Services from python Boto3
#---------------------------------------------------
import boto3
import pandas as pd


#################################################################
# S3
##############
#Connect S3 and load csv-------
def ConnectS3():
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket= config.BUCKET, Key= config.INPUT_FILE) 
    pdData = pd.read_csv(obj['Body']) # 'Body' is a key word
    print("pdData = ", pdData.shape)


#-------------------------------------------------------------------
# To convert pandas to parquet format
#--------------------------------------
def convertParquetFormat():

    s3 = boto3.client('s3')
    BUCKET_NAME = "srini-ml-sagemaker"
    CSV_PATH = "Glue/US_StateCounty.csv"
    TARGET_FOLDER = "Glue/glue-pythonshell/"

    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket= BUCKET_NAME, Key= CSV_PATH) 
    pdData = pd.read_csv(obj['Body']) # 'Body' is a key word
    print("pdData  = ", pdData.shape)

    '''
    # Read in some example text, as unicode
    with open("utext.txt") as fi:
        text_body = fi.read().decode("utf-8")
    '''

    # A GzipFile must wrap a real file or a file-like object. We do not want to
    # write to disk, so we use a BytesIO as a buffer.
    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 9, gz_body)
    #gz.write(pdData.encode('utf-8'))  # convert unicode strings to bytes!
    rec = pdData.to_records(index=False)
    s = rec.tostring()
    gz.write(s)  # convert unicode strings to bytes!
    gz.close()
    # GzipFile has written the compressed bytes into our gz_body
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key= TARGET_FOLDER + 'dfParquet1.gzip',  # Note: NO .gz extension!
        #ContentType='text/plain',  # the original type
        ContentEncoding='gzip',  # MUST have or browsers will error
        Body=gz_body.getvalue()
    )

##################################################################
# Glue
###########
def connectGlue():
    client = boto3.client('glue', 'us-east-1')

    try:
        response = client.start_crawler(
        Name='glue-hive-metadata'
        )
    except:
        response = 'null'

#----------------------------------------------------------------- 
# # To list the crawlers in associated glue
# #-----------------------------------------   
def listCrawlers():

    client = boto3.client('glue', 'us-east-1')

    print("client = ", client)

    response = client.list_crawlers(MaxResults=10)
    print("response = ", response)
    
def GlueJob():
    
    glue = boto3.client(service_name='glue', region_name='us-east-1')
    
    myJob = glue.create_job(Name='pras-cli-glue', Role='Glue_DefaultRole',Command={'Name': 'pras-cli-glue','ScriptLocation': 's3://srini-ml-sagemaker/Glue/glue-hive/my_etl_script.py'})




###################################################################
# DMS
###########

#-------------------------------------------------
# To connect DMS task and add tags
#---------------------------

def connectDMSTask():
    client = boto3.client('dms', 'us-east-1')

    response = client.add_tags_to_resource(
        ResourceArn='arn:aws:dms:us-east-1:882038671278:task:OOVANVY4BKRE2CFG7LOXVQKRSQ',
        Tags=[
            {
                'Key': 'python tag',
                'Value': 'edited from python'
            },
        ]
        
    )
    
#--------------------------------------------------------
# To cnnect Replication Instance
#---------------------------------
def connectReplicationInstance():

    client = boto3.client('dms', 'us-east-1')

    response = client.describe_replication_instances(
    #ResourceArn='arn:aws:dms:us-east-1:882038671278:rep:VOWWEDYNCQCSQ55SBITKX3PZQI',

    Filters=[
        {
            'Name': 'replication-instance-arn',
            'Values': [
                'arn:aws:dms:us-east-1:882038671278:rep:VOWWEDYNCQCSQ55SBITKX3PZQI',
            ]
        },
    ],
    MaxRecords=23
    
    )

    print("response = ", response)
    

#--------------------------------------------------------
# To cnnect Replication Instance
#---------------------------------
def modifyReplicationInstance():

    client = boto3.client('dms', 'us-east-1')

    print("client = ", client)
    replication_instance_arn = 'arn:aws:dms:us-east-1:882038671278:rep:BWKCRZ2N6ONTJFYMZL24OPXLIM'
    next_instance_type = 'dms.t2.micro'

    client.modify_replication_instance(
        ReplicationInstanceArn=replication_instance_arn,
        ApplyImmediately=True,
        ReplicationInstanceClass=next_instance_type
    )

    instance_details = get_replication_instance_details(replication_instance_name)

#########################################################

