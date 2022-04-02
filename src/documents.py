import os
import boto3
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType

# Companies Registration
repository_registration = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'

# DFP
repository_DFP = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'

# ITR
repository_ITR = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'


# Demonstração de Resultado Abrangente (DRA)
DTYPE_DFP_1 = {'CNPJ_CIA': 'object', 'VERSAO': 'int64', 'DENOM_CIA': 'object', 'CD_CVM': 'object', 'GRUPO_DFP': 'object',
               'MOEDA': 'object', 'ESCALA_MOEDA': 'object', 'ORDEM_EXERC': 'object', 'CD_CONTA': 'object', 'DS_CONTA': 'object', 'VL_CONTA': 'float64', 'ST_CONTA_FIXA': 'object'}
DATES_DFP_1 = ['DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC']

# S3 connection
s3 = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)


years_list = ['2011', '2012', '2013', '2014', '2015',
              '2016', '2017', '2018', '2019', '2020', '2021']

itr_dre = StructType([
    StructField('CNPJ_CIA', StringType(), True),
    StructField('DT_REFER', StringType(), True),
    StructField('VERSAO', IntegerType(), True),
    StructField('DENOM_CIA', StringType(), True),
    StructField('CD_CVM', StringType(), True),
    StructField('GRUPO_DFP', StringType(), True),
    StructField('MOEDA', StringType(), True),
    StructField('ESCALA_MOEDA', StringType(), True),
    StructField('ORDEM_EXERC', StringType(), True),
    StructField('DT_INI_EXERC', StringType(), True),
    StructField('DT_FIM_EXERC', StringType(), True),
    StructField('CD_CONTA', StringType(), True),
    StructField('DS_CONTA', StringType(), True),
    StructField('VL_CONTA', FloatType(), True),
    StructField('ST_CONTA_FIXA', StringType(), True)
])

dtype = {'CNPJ_CIA':'object', 'VERSAO':'int', 'DENOM_CIA':'object', 'CD_CVM':'object', 'GRUPO_DFP':'object',
                            'MOEDA':'object', 'ESCALA_MOEDA':'object', 'ORDEM_EXERC':'object',
                            'CD_CONTA':'object', 'DS_CONTA':'object', 'VL_CONTA':'float', 'ST_CONTA_FIXA':'object',
                            'DT_REFER':'object', 'DT_INI_EXERC':'object', 'DT_FIM_EXERC':'object'}

modelRevenueVariables = ['RESULTADO_BRUTO_LAG_1', 'RESULTADO_BRUTO_LAG_3',
       'RESULTADO_BRUTO_LAG_12', 'SECTOR_RESULTADO_BRUTO',
       'SECTOR_RESULTADO_BRUTO_LAG_1', 'SECTOR_RESULTADO_BRUTO_LAG_3',
       'SECTOR_RESULTADO_BRUTO_LAG_12', 'RETURN_RESULTADO_BRUTO_LAG_1',
       'RETURN_RESULTADO_BRUTO_LAG_3', 'RETURN_RESULTADO_BRUTO_LAG_12',
       'RETURN_SECTOR_RESULTADO_BRUTO_LAG_1',
       'RETURN_SECTOR_RESULTADO_BRUTO_LAG_3',
       'RETURN_SECTOR_RESULTADO_BRUTO_LAG_12', 'ESTATAL', 'ESTATAL_HOLDING',
       'ESTRANGEIRO', 'ESTRANGEIRO_HOLDING', 'PRIVADO', 'PRIVADO_HOLDING']