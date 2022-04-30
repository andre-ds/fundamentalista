import os
import pandas as pd
from dotenv import load_dotenv
from Utils import PreProcessing, Companies
import documents as dc
import boto3

# Enviroment
load_dotenv()
pp = PreProcessing()
ex = Companies(s3env = dc.s3, years_list=dc.years_list)

## pre-processing codes DRE
PATH_DIR = os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath('__file__'))), 'data'), 'auxiliary')
filename = 'raw_code_dre.csv'
dataset = pd.read_csv(os.path.join(PATH_DIR, filename))
dataset = pp.patterning_text_var(dataset=dataset, variable='tipo')
dataset['codigo'] = dataset['codigo'].astype(str)
dataset.drop(columns=['sigla'], inplace=True)
dataset.to_csv(os.path.join(PATH_DIR, f'pp_code_dre_{ex.todaystr}.csv'), index=False)
# Loading in S3bucket
ex.load_datalake(bucket='datalake-mercado-financeiro-auxiliary-data', type='code_dre')

