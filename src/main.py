import os
import boto3
from dotenv import load_dotenv
from Utils import Utils
from PreProcessing import PreProcessing
from SparkEnvironment import SparkEnvironment
import documents as dc


# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Utils(spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# S3 connection
s3 = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# Data extraction for raw layer
dataType = ['registration','itr']
ex.extraction_cvm(dataType=dataType, years_list=dc.years_list)
ex.unzippded_files(dataType=dataType)
ex.saving_raw_data(dataType='itr_cia_aberta_DRE_con', filename='itr_dre', schema=dc.itr_dre)
ex.load_bucket(s3env=s3, bucket='deepfi-raw', PATH=ex.PATH_RAW)

# Pre-processing data
ex.check_auxiliary_files(bucket='deepfi-auxiliary-data')
pp.pre_process_itr_dre(dataType='itr_dre', years_list=dc.years_list)
ex.load_bucket(s3env=s3, bucket='deepfi-pre-processed', PATH=ex.PATH_PRE_PROCESSED)

# Loading
# ex.load_datalake(bucket='datalake-mercado-financeiro-raw', type='extraction')
