from dotenv import load_dotenv
from Extraction import Extraction
from PreProcessing import PreProcessing
from SparkEnvironment import SparkEnvironment
import documents as dc


# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Extraction(s3env = dc.s3, spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# Extracting Data
# alldataType = ['registration', 'dfp', 'itr']
dataType = ['registration','itr']
ex.extraction_cvm(dataType=dataType, years_list=dc.years_list)
ex.unzippded_files(dataType=dataType)
ex.saving_raw_data(dataType='itr_cia_aberta_DRE_con', filename='itr_dre', schema=dc.itr_dre)
ex.load_bucket(bucket='deepfi-raw', PATH=ex.PATH_RAW)
# Pre-processing data
pp.pre_process_itr_dre(dataType='itr_dre', years_list=dc.years_list)
ex.load_bucket(bucket='deepfi-pre-processed', PATH=ex.PATH_PRE_PROCESSED)

# Loading
# ex.load_datalake(bucket='datalake-mercado-financeiro-raw', type='extraction')
