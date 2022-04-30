import os
import re
import boto3
import botocore
import pandas as pd
import urllib.request
import zipfile
import documents as dc
from datetime import date
from dotenv import load_dotenv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, quarter, to_date, year, when, to_date, asc, months_between, round


class Utils:


    def __init__(self, spark_environment):
        self._run()
        self._path_environment()
        self.spark_environment = spark_environment

    def _run(self):
        load_dotenv()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

    def _path_environment(self):

        # Getting Date
        self.todaystr = re.sub('-', '_', str((date.today())))
        # Defining Path
        self.PATH = os.path.dirname(os.path.dirname(os.path.realpath('__file__')))
        # Defining Environment
        list_folders = os.listdir(self.PATH)
        if 'data' not in list_folders:
            os.mkdir(os.path.join(self.PATH, 'data'))
        PATH_DATA = os.path.join(os.path.join(self.PATH, 'data'))
        list_folders = os.listdir(PATH_DATA)
        if 'raw' not in list_folders:
            os.mkdir(os.path.join(PATH_DATA, 'raw'))
        if 'pre-processed' not in list_folders:
            os.mkdir(os.path.join(PATH_DATA, 'pre-processed'))
        if 'zipfiles' not in list_folders:
            os.mkdir(os.path.join(PATH_DATA, 'zipfiles'))
        if 'unzippedfiles' not in list_folders:
            os.mkdir(os.path.join(PATH_DATA, 'unzippedfiles'))
        if 'auxiliary' not in list_folders:
            os.mkdir(os.path.join(PATH_DATA, 'auxiliary'))
        # Getting paths
        self.PATH_ZIPFILES = os.path.join(PATH_DATA, 'zipfiles')
        self.PATH_UNZIPPEDFILES = os.path.join(PATH_DATA, 'unzippedfiles')
        self.PATH_RAW = os.path.join(PATH_DATA, 'raw')
        self.PATH_PRE_PROCESSED = os.path.join(PATH_DATA, 'pre-processed')
        self.PATH_AUXILIARY = os.path.join(PATH_DATA, 'auxiliary')

        print('[Environment Cheked.]')


    def check_auxiliary_files(self, bucket:str):
        
        def func(filename:str, bucket:str):
            if filename not in os.listdir(self.PATH_AUXILIARY):
                try:
                    s3resource = boto3.resource('s3')
                    s3resource.Bucket(bucket).download_file(filename, os.path.join(self.PATH_AUXILIARY, filename))
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
        
        auxiliary_files = ['pp_code_dre_2022_01_04.csv']
        for file in auxiliary_files:
            func(filename=file, bucket=bucket)



    def extraction_cvm(self, dataType:str, years_list:list):

        if 'registration' in dataType:
            urllib.request.urlretrieve(dc.repository_registration, os.path.join(self.PATH_UNZIPPEDFILES, 'cad_cia_aberta.csv'))

        for year in years_list:
            # Year (yearly)
            if 'dfp' in dataType:
                urllib.request.urlretrieve(dc.repository_DFP+f'dfp_cia_aberta_{year}.zip', os.path.join(self.PATH_ZIPFILES, f'dfp_cia_aberta_{year}.zip'))
            # Quarter (quarterly) 
            if 'itr' in dataType:
                urllib.request.urlretrieve(dc.repository_ITR+f'itr_cia_aberta_{year}.zip', os.path.join(self.PATH_ZIPFILES, f'itr_cia_aberta_{year}.zip'))
        print('[Extracted.]')


    def unzippded_files(self, dataType):

        self.list_files = [file.lower() for file in os.listdir(self.PATH_ZIPFILES) if (file.endswith('.zip')) and re.findall('|'.join(dataType), file)]
        for file in self.list_files:
            with zipfile.ZipFile(os.path.join(self.PATH_ZIPFILES, file), 'r') as zip_ref:
                zip_ref.extractall(self.PATH_UNZIPPEDFILES)
        print('[Unzipped.]') 


    def saving_raw_data(self, dataType:str, filename:str, schema):

        list_files = [file for file in os.listdir(self.PATH_UNZIPPEDFILES) if (file.endswith('.csv')) and re.findall(dataType, file)]
        for file in list_files:
            dataset = self.spark_environment.read.csv(os.path.join(self.PATH_UNZIPPEDFILES, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
            saveFilename= f'extracted_{self.todaystr}_{filename}_{file[-8:-4]}.parquet'
            dataset.write.format('parquet').mode('overwrite').save(os.path.join(self.PATH_RAW, saveFilename))


    def load_bucket(self, s3env, bucket, PATH):

        for folder in os.listdir(PATH):
            files_foder = [file for file in os.listdir(os.path.join(PATH, folder))]
            for file in files_foder:
                path = os.path.join(PATH, folder)
                s3env.upload_file(Filename=f'{path}/{file}', Bucket=bucket, Key=f'{folder}/{file}')

        print('[Files Uploaded.]')

