from msilib import schema
import boto3
import os
import re
import zipfile
from datetime import date
import pandas as pd
import documents as dc
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from SparkEnvironment import SparkEnvironment
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, quarter, to_date, year, when, to_date, asc, months_between, round, lit, concat
from Extraction import Extraction
from PreProcessing import PreProcessing 
from dotenv import load_dotenv
import documents as dc

# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Extraction(s3env = dc.s3, spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# path
PATH_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.join(os.path.realpath('__file__')))), 'data')
PATH_ZIPFILES = os.path.join(PATH_DIR, os.path.join('temporary-files', 'zipfiles'))
PATH_UNZIPPEDFILES = os.path.join(PATH_DIR, os.path.join('temporary-files', 'unzippedfiles'))
PATH_RAW = os.path.join(os.path.join(PATH_DIR, 'datalake-mercado-financeiro-raw'))
PATH_PRE_PROCESSED = os.path.join(os.path.join(PATH_DIR, 'pre-processed'))
PATH_AUXILIARY = os.path.join(os.path.join(PATH_DIR, 'auxiliary'))

file = 'pp_itr_dre_2021.parquet'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_PRE_PROCESSED, file))
dataset.toPandas().to_csv(os.path.join(PATH_PRE_PROCESSED, 'teste2.csv'))

dataset.show()




s3 = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)


bucket = 'datalake-mercado-financeiro-pre-processed-itr-dre'
for folder in os.listdir(PATH_PRE_PROCESSED):
    files_foder = [file for file in os.listdir(os.path.join(PATH_PRE_PROCESSED, folder))]
    for file in files_foder:
        path = os.path.join(PATH_PRE_PROCESSED, folder)
        s3.upload_file(Filename=f'{path}/{file}', Bucket=bucket, Key=f'{folder}/{file}')








bucket = 'datalake-mercado-financeiro-pre-processed'
filename = 'teste.parquet'

for file in path:
    s3.meta.client.upload_file(Filename=os.path.join(path, file), Bucket = bucket, Key=filename)



dataset = sk.spark_environment.read.parquet(os.path.join(PATH_PRE_PROCESSED, 'teste2.parquet'))
dataset.show()




dataType = 'itr_dre'
dateType = ['2020', '2021']

file = 'extracted_2022_01_25_itr_dre_2020.parquet'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW, file))
dataset.show()

dataset_aux = sk.spark_environment.read.csv(os.path.join(PATH_AUXILIARY, 'pp_code_dre_2022_01_04.csv'), header=True)

dataset_aux.show()

dataset = pp._pre_processing_itr_dre(dataset=dataset, dataset_aux=dataset_aux)
dataset.toPandas().to_csv(os.path.join(PATH_RAW, 'teste2.csv'))


file = 'pp_itr_dre_2019.parquet'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_PRE_PROCESSED, file))

dataset = dataset.withColumn('date', lit(concat(col('ANO_REFERENCIA'), lit('-'), col('TRIMESTRE'))))




dataset.groupBy(['TRIMESTRE', 'date']).count().show()
dataset.groupBy('date').count().show()
dataset.printSchema()
dataset.show()


dataset.toPandas().to_csv(os.path.join(PATH_RAW, 'teste.csv'))



class teste(Extraction):

    def __init__(self, spark_environment):
        self._environment()
        self._run()
        self.spark_environment = spark_environment

    def _environment(self):
        load_dotenv()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

 
    def patterning_text_var(self, dataset, variable):

        dataset[variable] = dataset[variable].str.lower().str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        dataset[variable] = dataset[variable].replace(r'\s+', '_', regex=True)

        return dataset


    def _pre_processing_itr_dre(self, dataset, dataset_aux):
    
        # Pre-processing
        dataset = dataset.filter(col('ORDEM_EXERC') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['DT_REFER', 'DT_FIM_EXERC', 'DT_INI_EXERC']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('period', round(months_between(col('DT_FIM_EXERC'), col('DT_INI_EXERC'))))
        dataset = dataset.withColumn('ano_referencia', year('DT_REFER'))
        dataset = dataset.withColumn('trimestre', quarter('DT_REFER'))
        dataset = dataset.filter(col('period') == 3)
        ## Standarting ESCALA_MOEDA
        dataset = dataset.withColumn('VL_CONTA', when(col('ESCALA_MOEDA') == 'MIL', col('VL_CONTA')*1000).otherwise(col('VL_CONTA')))
        dataset = dataset.withColumn('VL_CONTA', col('VL_CONTA').cast(IntegerType()))
        ## Standarting CD_CONTA
        dataset = dataset.join(dataset_aux, on=dataset.CD_CONTA == dataset_aux.codigo, how='inner')
        # Pivot dataset
        varlist = ['CD_CVM', 'CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DT_FIM_EXERC', 'DT_INI_EXERC', 'ano_referencia', 'trimestre']
        dataset = dataset.orderBy(asc('CD_CONTA'))
        dataset = dataset.groupBy(varlist).pivot('tipo').max('VL_CONTA').na.fill(0)
        # Rename all variables to upercase
        for variable in dataset.columns:
            dataset = dataset.withColumnRenamed(variable, variable.upper())

        return dataset


    def pre_process_itr_dre(self, dataType, years_list):

        list_files = [file for file in os.listdir(self.PATH_RAW) if (file.endswith('.parquet')) and re.findall(dataType, file)]
        for file in list_files:
            for year in years_list:
                if year == file[-12:-8]:
                    # Oppen Datasets
                    dataset_aux = self.spark_environment.read.csv(os.path.join(self.PATH_AUXILIARY, 'pp_code_dre_2022_01_04.csv'), header = True, sep=',', encoding='ISO-8859-1')
                    dataset = self.spark_environment.read.parquet(os.path.join(self.PATH_RAW , file))
                    dataset = self._pre_processing_itr_dre(dataset = dataset, dataset_aux = dataset_aux)
                    # Saving
                    saveFilename = f'pp_itr_dre_{file[-12:-8]}.parquet'
                    dataset.write.format('parquet').mode('overwrite').save(os.path.join(self.PATH_PRE_PROCESSED, saveFilename))