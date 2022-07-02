import os
import re
import pandas as pd
from pyspark.sql import DataFrame
from Utils import Utils
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, quarter, to_date, month, year, when, to_date, asc, months_between, round, concat, lit, regexp_replace


class PreProcessing(Utils):

    def __init__(self, spark_environment):
        self._run()
        self._path_environment()
        self.spark_environment = spark_environment

    
    def remove_missing_values(self, dataset:DataFrame) -> DataFrame:

        missing_variables = {col: dataset.filter(dataset[col].isNull()).count() for col in dataset.columns}

        return missing_variables


    def text_normalization(row):

        return row.encode('ascii', 'ignore').decode('ascii')


    def patterning_text_var(self, dataset:DataFrame, variable:str) -> DataFrame:

        dataset[variable] = dataset[variable].str.lower().str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        dataset[variable] = dataset[variable].replace(r'\s+', '_', regex=True)

        return dataset


    def _pre_processing_itr_dre(self, dataset:DataFrame) -> DataFrame:
    
        from documents import dre_account

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
        listKey = list(dre_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        ## Standarting CD_CONTA
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', dre_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_data', 'dt_fim_exerc', 'dt_ini_exerc', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset


    def _pre_processing_itr_bpp(self, dataset:DataFrame) -> DataFrame:
        
        from documents import bpp_account

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
        listKey = list(bpp_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', bpp_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_data', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm','id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset
        

    def _pre_processing_itr_bpa(self, dataset:DataFrame) -> DataFrame:

        from documents import bpa_account

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
        listKey = list(bpa_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', bpa_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_data', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm','id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset


    def pre_process_itr(self, dataType:str, years_list:list):

        list_files = [file for file in os.listdir(self.PATH_RAW) if (file.endswith('.parquet')) and re.findall(dataType, file)]
        for file in list_files:
            for year in years_list:
                if year == file[-12:-8]:
                    # Oppen Datasets
                    dataset = self.spark_environment.read.parquet(os.path.join(self.PATH_RAW , file))
                    if dataType == 'itr_dre':
                        dataset = self._pre_processing_itr_dre(dataset = dataset)
                    elif dataType == 'itr_bpp':
                        dataset = self._pre_processing_itr_bpp(dataset = dataset)
                    elif dataType == 'itr_bpa':
                        dataset = self._pre_processing_itr_bpa(dataset = dataset)
                    # Saving
                    saveFilename = f'pp_{dataType}_{file[-12:-8]}.parquet'
                    dataset.write.format('parquet').mode('overwrite').save(os.path.join(self.PATH_PRE_PROCESSED, saveFilename))

