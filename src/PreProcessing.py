import os
import re
import pandas as pd
from Extraction import Extraction
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, quarter, to_date, year, when, to_date, asc, months_between, round, concat, lit, udf


class PreProcessing(Extraction):

    def __init__(self, spark_environment):
        self._run()
        self._path_environment()
        self.spark_environment = spark_environment

    
    def remove_missing_values(self, dataset):

        missing_variables = {col: dataset.filter(dataset[col].isNull()).count() for col in dataset.columns}

        return missing_variables


    def text_normalization(row):

        return row.encode('ascii', 'ignore').decode('ascii')


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
        dataset = dataset.withColumn('DATA', to_date(lit(concat(col('ANO_REFERENCIA'), lit('-'), col('TRIMESTRE'), lit('-'), lit('01'))),'yyyy-M-dd'))
        ## Standarting ESCALA_MOEDA
        dataset = dataset.withColumn('VL_CONTA', when(col('ESCALA_MOEDA') == 'MIL', col('VL_CONTA')*1000).otherwise(col('VL_CONTA')))
        dataset = dataset.withColumn('VL_CONTA', col('VL_CONTA').cast(IntegerType()))
        ## Standarting CD_CONTA
        dataset = dataset.join(dataset_aux, on=dataset.CD_CONTA == dataset_aux.codigo, how='inner')
        # Pivot dataset
        varlist = ['CD_CVM', 'CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DATA', 'DT_FIM_EXERC', 'DT_INI_EXERC', 'ano_referencia', 'trimestre']
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