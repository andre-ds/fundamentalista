from calendar import month
from fileinput import filename
from msilib import schema
from operator import index
import string
import boto3
import os
import re
import zipfile
from datetime import date
from numpy import isin
import pandas as pd
import documents as dc
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from SparkEnvironment import SparkEnvironment
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, quarter, month, to_date, year, when, to_date, asc, months_between, round, lit, concat, regexp_replace
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
PATH_ZIPFILES = os.path.join(PATH_DIR, 'zipfiles')
PATH_UNZIPPEDFILES = os.path.join(PATH_DIR, 'unzippedfiles')
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

dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW, 'extracted_2022_06_04_itr_dre_2011.parquet'))
dataset_aux = sk.spark_environment.read.option('header',True).csv(os.path.join(PATH_AUXILIARY, 'pp_code_dre_2022_01_04.csv'))

for file in os.listdir(PATH_RAW):
    if file != 'extracted_2022_04_30_itr_dre_2011.parquet':
        df = sk.spark_environment.read.parquet(os.path.join(PATH_RAW, file))
        dataset = dataset.union(df)
    else:
        'não'

dataset.toPandas().to_csv(os.path.join(PATH_DIR, 'dataset_raw_test.csv'), index=False)

'''

'''
file = 'itr_cia_aberta_BPP_con_2021.csv'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW , file), schema=dc.itr_bp)
'''

### Refatorização DRE
file = 'extracted_2022_06_28_itr_dre_2020.parquet'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW , file), schema=dc.itr_bp)

# Pre-processing
for var in dataset.columns:
    dataset = dataset.withColumnRenamed(var, var.lower())
# Keeping last registers
dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
## Getting year and quarter
for v in ['dt_refer', 'dt_fim_exerc', 'dt_ini_exerc']:
    dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
dataset = dataset.withColumn('dt_period', round(months_between(col('dt_fim_exerc'), col('dt_ini_exerc'))))
dataset = dataset.withColumn('dt_year', year('dt_refer'))
dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
dataset = dataset.filter(col('dt_period') == 3)
dataset = dataset.withColumn('dt_data', to_date(lit(concat(col('dt_year'), lit('-'), col('dt_quarter'), lit('-'), lit('01'))),'yyyy-M-dd'))
## Standarting escala_moeda
dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
## Keeping only relevants cd_conta
listKey = list(dc.dre_account.keys())
dataset = dataset.filter(col('cd_conta').isin(listKey))
## Standarting CD_CONTA
for k in listKey:
    dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', dc.dre_account.get(k)))
# Pivot dataset
varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_data', 'dt_fim_exerc', 'dt_ini_exerc', 'dt_year', 'dt_quarter']
dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
# Rename all variables to upercase
variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
for v in variablesRename:
    dataset = dataset.withColumnRenamed(v[0], v[1])


### BPP
file = 'extracted_2022_06_20_itr_bpp_2021.parquet'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW , file), schema=dc.itr_bp)
dataset_aux = sk.spark_environment.read.csv(os.path.join(PATH_AUXILIARY, 'pp_code_bpp_2022_05_20.csv'), header = True, sep=',', encoding='ISO-8859-1')

# Pre-processing
for var in dataset.columns:
    dataset = dataset.withColumnRenamed(var, var.lower())
# Keeping las registers
dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
## Getting year and quarter
for v in ['dt_refer', 'dt_fim_exerc']:
    dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
dataset = dataset.withColumn('dt_year', year('dt_refer'))
dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
dataset = dataset.withColumn('dt_data', to_date(lit(concat(col('dt_year'), lit('-'), month(col('dt_refer')), lit('-'), lit('01'))),'yyyy-M-dd'))
## Standarting ESCALA_MOEDA
dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
## Keeping only relevants cd_conta
listKey = list(dc.bpp_account.keys())
dataset = dataset.filter(col('cd_conta').isin(listKey))
for k in listKey:
    print(k)
    dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', dc.bpp_account.get(k)))
# Pivot dataset
varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_data', 'dt_year', 'dt_quarter']
dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
# Rename all variables to upercase
variablesRename = [['cd_cvm','id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
for v in variablesRename:
    dataset = dataset.withColumnRenamed(v[0], v[1])


### BPA
# dataset = sk.spark_environment.read.csv(os.path.join(PATH_UNZIPPEDFILES, 'itr_cia_aberta_BPA_con_2021.csv'), header = True, sep=';', encoding='ISO-8859-1')
# dataset.toPandas().to_csv(os.path.join(PATH_UNZIPPEDFILES, 'dataset.csv'))

file = 'extracted_2022_06_29_itr_bpa_2021.parquet'
dataset = sk.spark_environment.read.parquet(os.path.join(PATH_RAW , file), schema=dc.itr_bp_ba)

# Pre-processing
for var in dataset.columns:
    dataset = dataset.withColumnRenamed(var, var.lower())
# Keeping las registers
dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
## Getting year and quarter
for v in ['dt_refer', 'dt_fim_exerc']:
    dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
dataset = dataset.withColumn('dt_year', year('dt_refer'))
dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
dataset = dataset.withColumn('dt_data', to_date(lit(concat(col('dt_year'), lit('-'), month(col('dt_refer')), lit('-'), lit('01'))),'yyyy-M-dd'))
## Standarting ESCALA_MOEDA
dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
## Keeping only relevants cd_conta
listKey = list(dc.bpa_account.keys())
dataset = dataset.filter(col('cd_conta').isin(listKey))
for k in listKey:
    dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', dc.bpa_account.get(k)))
# Pivot dataset
varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_data', 'dt_year', 'dt_quarter']
dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
# Rename all variables to upercase
variablesRename = [['cd_cvm','id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
for v in variablesRename:
    dataset = dataset.withColumnRenamed(v[0], v[1])

dataset.show()
