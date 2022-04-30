from operator import index
import os
import re
import pandas as pd
import documents as dc
from dotenv import load_dotenv
from Utils import Utils
from PreProcessing import PreProcessing
from SparkEnvironment import SparkEnvironment
from pyspark.sql.functions import col, quarter, to_date, year, when, to_date, asc, months_between, round, lit, concat,regexp_replace, udf, lower, lag, sum, upper, avg
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Utils(s3env = dc.s3, spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# Oppen and append all datasets
dataType = 'itr_dre'
list_files = [file for file in os.listdir(ex.PATH_PRE_PROCESSED) if (file.endswith('.parquet')) and re.findall(dataType, file)]
dataset_list = []
for file in list_files:
    df = sk.spark_environment.read.parquet(os.path.join(ex.PATH_PRE_PROCESSED , file))
    dataset_list.append(df)
dataset = reduce(DataFrame.unionAll, dataset_list)

# Oppen cadastral information
dataset_cad = sk.spark_environment.read.option('encoding', 'ISO-8859-1').csv(os.path.join(ex.PATH_UNZIPPEDFILES, 'cad_cia_aberta.csv'), header=True, sep=';')
# Selecting Variables
varlist = ['CNPJ_CIA', 'SETOR_ATIV', 'CONTROLE_ACIONARIO']
dataset_cad = dataset_cad[varlist]
dataset_cad = dataset_cad.dropDuplicates(subset = ['CNPJ_CIA'])

# Pre-processing
## Dataset cadastral
dataset_cad = dataset_cad.withColumn('SETOR_ATIV', upper(col('SETOR_ATIV'))).withColumn('CONTROLE_ACIONARIO', upper(col('CONTROLE_ACIONARIO')))
dataset_cad = dataset_cad.withColumn('SETOR_ATIV', regexp_replace(col('SETOR_ATIV'), '[_():;,.!?\\-]', ''))
dataset_cad = dataset_cad.withColumn('SETOR_ATIV', regexp_replace(col('SETOR_ATIV'), '\s+', '_'))
dataset_cad = dataset_cad.withColumn('CONTROLE_ACIONARIO', regexp_replace(col('CONTROLE_ACIONARIO'), '\s+', '_'))

## Integration
dataset = dataset.join(dataset_cad, on='CNPJ_CIA', how='left')
varlist = ['CNPJ_CIA', 'DENOM_CIA', 'SETOR_ATIV', 'CONTROLE_ACIONARIO', 'DATA', 'ANO_REFERENCIA', 'TRIMESTRE', 'RESULTADO_BRUTO']
dataset = dataset[varlist]

## Featuring Engineering
# Calculating Lags
windowSpec  = Window.partitionBy('CNPJ_CIA').orderBy('DATA')
dataset = dataset.withColumn('RESULTADO_BRUTO_LAG_1',lag('RESULTADO_BRUTO', 1).over(windowSpec))
dataset = dataset.withColumn('RESULTADO_BRUTO_LAG_3',lag('RESULTADO_BRUTO', 3).over(windowSpec))
dataset = dataset.withColumn('RESULTADO_BRUTO_LAG_12',lag('RESULTADO_BRUTO', 12).over(windowSpec))

# Aggregating by sector
dataset = dataset.fillna({'SETOR_ATIV':'missing_value'})
dataset_sector = dataset.groupBy('SETOR_ATIV').agg(sum('RESULTADO_BRUTO').alias('SECTOR_RESULTADO_BRUTO'),
                                                sum('RESULTADO_BRUTO_LAG_1').alias('SECTOR_RESULTADO_BRUTO_LAG_1'),
                                                sum('RESULTADO_BRUTO_LAG_3').alias('SECTOR_RESULTADO_BRUTO_LAG_3'),
                                                sum('RESULTADO_BRUTO_LAG_12').alias('SECTOR_RESULTADO_BRUTO_LAG_12'))
# Integration
dataset = dataset.join(dataset_sector, on='SETOR_ATIV', how='left')

# Calculate return
dataset = dataset.withColumn('RETURN_RESULTADO_BRUTO_LAG_1', round(((col('RESULTADO_BRUTO')/col('RESULTADO_BRUTO_LAG_1'))-1)*100, 2))
dataset = dataset.withColumn('RETURN_RESULTADO_BRUTO_LAG_3', round(((col('RESULTADO_BRUTO')/col('RESULTADO_BRUTO_LAG_3'))-1)*100, 2))
dataset = dataset.withColumn('RETURN_RESULTADO_BRUTO_LAG_12', round(((col('RESULTADO_BRUTO')/col('RESULTADO_BRUTO_LAG_12'))-1)*100, 2))
dataset = dataset.withColumn('RETURN_SECTOR_RESULTADO_BRUTO_LAG_1', round(((col('SECTOR_RESULTADO_BRUTO')/col('SECTOR_RESULTADO_BRUTO_LAG_1'))-1)*100, 2))
dataset = dataset.withColumn('RETURN_SECTOR_RESULTADO_BRUTO_LAG_3', round(((col('SECTOR_RESULTADO_BRUTO')/col('SECTOR_RESULTADO_BRUTO_LAG_3'))-1)*100, 2))
dataset = dataset.withColumn('RETURN_SECTOR_RESULTADO_BRUTO_LAG_12', round(((col('SECTOR_RESULTADO_BRUTO')/col('SECTOR_RESULTADO_BRUTO_LAG_12'))-1)*100, 2))

# Creating Dummies
dataset_controle_acionario = dataset.groupBy('CNPJ_CIA').pivot('CONTROLE_ACIONARIO').agg(lit(1)).na.fill(0)
dataset_controle_acionario = dataset_controle_acionario.drop('null')

# Integration
dataset = dataset.join(dataset_controle_acionario, on='CNPJ_CIA', how='left')

# Replacing all Nan by 0
dataset = dataset.fillna(value=0)

# Selecting Variables
variable = ['CNPJ_CIA', 'RESULTADO_BRUTO', 'RESULTADO_BRUTO_LAG_1', 'RESULTADO_BRUTO_LAG_3', 'RESULTADO_BRUTO_LAG_12',
            'SECTOR_RESULTADO_BRUTO', 'SECTOR_RESULTADO_BRUTO_LAG_1', 'SECTOR_RESULTADO_BRUTO_LAG_3', 'SECTOR_RESULTADO_BRUTO_LAG_12',
            'RETURN_RESULTADO_BRUTO_LAG_1', 'RETURN_RESULTADO_BRUTO_LAG_3', 'RETURN_RESULTADO_BRUTO_LAG_12',
            'RETURN_SECTOR_RESULTADO_BRUTO_LAG_1', 'RETURN_SECTOR_RESULTADO_BRUTO_LAG_3', 'RETURN_SECTOR_RESULTADO_BRUTO_LAG_12',
            'ESTATAL', 'ESTATAL_HOLDING', 'ESTRANGEIRO', 'ESTRANGEIRO_HOLDING', 'PRIVADO', 'PRIVADO_HOLDING']
dataset = dataset[variable]

# Saving Dataset
dataset.write.format('parquet').mode('overwrite').save(os.path.join(ex.PATH_PRE_PROCESSED, 'pp_revenue_quarter.parquet'))
dataset = dataset.toPandas()
dataset.to_csv(os.path.join(ex.PATH_PRE_PROCESSED, 'pp_revenue_quarter.csv'), index=False)
