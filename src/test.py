from msilib import schema
from operator import index
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
from Utils import Utils
from PreProcessing import PreProcessing 
from dotenv import load_dotenv
import documents as dc

# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Utils(s3env = dc.s3, spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# path
PATH_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.join(os.path.realpath('__file__')))), 'data')
PATH_ZIPFILES = os.path.join(PATH_DIR, os.path.join('temporary-files', 'zipfiles'))
PATH_UNZIPPEDFILES = os.path.join(PATH_DIR, os.path.join('temporary-files', 'unzippedfiles'))
PATH_RAW = os.path.join(os.path.join(PATH_DIR, 'raw'))
PATH_PRE_PROCESSED = os.path.join(os.path.join(PATH_DIR, 'pre-processed'))
PATH_AUXILIARY = os.path.join(os.path.join(PATH_DIR, 'auxiliary'))

'''
# Pre-processado
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_PRE_PROCESSED, 'pp_itr_dre_2011.parquet'))

for file in os.listdir(PATH_PRE_PROCESSED):
    if file != 'pp_itr_dre_2011.parquet':
        df = sk.spark_environment.read.parquet(os.path.join(PATH_PRE_PROCESSED, file))
        dataset = dataset.union(df)
    else:
        'não'
dataset.toPandas().to_csv(os.path.join(PATH_PRE_PROCESSED, 'dataset_report.csv'), index=False)
'''
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW, 'extracted_2022_06_04_itr_dre_2011.parquet'))
dataset_aux = sk.spark_environment.read.option('header',True).csv(os.path.join(PATH_AUXILIARY, 'pp_code_dre_2022_01_04.csv'))

for file in os.listdir(PATH_RAW):
    if file != 'extracted_2022_04_30_itr_dre_2011.parquet':
        df = sk.spark_environment.read.parquet(os.path.join(PATH_RAW, file))
        dataset = dataset.union(df)
    else:
        'não'

#dataset.toPandas().to_csv(os.path.join(PATH_DIR, 'dataset_raw_test.csv'), index=False)

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

### A VARIAVEL DATA ESTAVA ERRADA, FAZER A CORRECAO A PARTIR DISSO; VERIFICAR O TRIMESTRE DE OUTRAS EMPRESAS
## Standarting CD_CONTA

dataset.columns
dataset_aux.columns

dataset = dataset.join(dataset_aux, on=dataset.CD_CONTA == dataset_aux.codigo, how='inner')
# Pivot dataset
varlist = ['CD_CVM', 'CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DATA', 'DT_FIM_EXERC', 'DT_INI_EXERC', 'ano_referencia', 'trimestre']
dataset = dataset.orderBy(asc('CD_CONTA'))
dataset = dataset.groupBy(varlist).pivot('tipo').max('VL_CONTA').na.fill(0)
# Rename all variables to upercase
for variable in dataset.columns:
    dataset = dataset.withColumnRenamed(variable, variable.upper())


dataset.show()
dataset.columns 

dataset.groupBy('DATA').count().show()
dataset.groupBy('DT_INI_EXERC').count().show()
dataset.groupBy('DT_FIM_EXERC').count().show()

dataset.toPandas().to_csv(os.path.join(PATH_PRE_PROCESSED, 'dataset_report.csv'), index=False)
dataset.toPandas().to_csv(os.path.join(PATH_PRE_PROCESSED, 'dataset_report_test_1.csv'), index=False)