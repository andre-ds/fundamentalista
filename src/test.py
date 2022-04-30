
from fileinput import filename
import os
import boto3
import botocore
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

dataset = sk.spark_environment.read.parquet(os.path.join(ex.PATH_PRE_PROCESSED, 'pp_itr_dre_2021.parquet'))

dataset.show()
dataset.toPandas().to_csv(os.path.join(ex.PATH_PRE_PROCESSED, 'test.csv'), index=False)