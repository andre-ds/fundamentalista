
from fileinput import filename
import os
import boto3
import botocore
from dotenv import load_dotenv
from Utils import Extraction
from PreProcessing import PreProcessing
from SparkEnvironment import SparkEnvironment
import documents as dc


# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Extraction(spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# S3 connection
s3 = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)


bucket = 'deepfi-auxiliary-data'
filename = 'pp_code_dre_2022_01_04.csv'









s3.Bucket().download_file(Fileobj=s3_file, os.path.join(ex.PATH_AUXILIARY, 'teste.csv'))



s3.Bucket('deepfi-auxiliary-data').download_file(s3_file, os.path.join(ex.PATH_AUXILIARY, 'teste.csv'))


def load_bucket(bucket, PATH, s3):

    for folder in os.listdir(PATH):
        files_foder = [file for file in os.listdir(os.path.join(PATH, folder))]
        for file in files_foder:
            path = os.path.join(PATH, folder)
            s3.upload_file(Filename=f'{path}/{file}', Bucket=bucket, Key=f'{folder}/{file}')


print("loading")
load_bucket(bucket='deepfi-raw', PATH=ex.PATH_RAW, s3=s3)