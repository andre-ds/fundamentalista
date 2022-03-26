
import warnings
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class SparkEnvironment:

    def __init__(self, session_type):
        self.session_type = session_type
        self.environment()
        self.__run()

    def __run(self):
        self.spark_environment.conf.set("spark.sql.debug.maxToStringFields", 1000)
        warnings.simplefilter(action='ignore', category=DeprecationWarning)

    def environment(self):

        if self.session_type == 'local':

            # Building Session
            
            config = SparkConf().setAll([('spark.executor.memory', '40g'), ('spark.executor.cores', '16'), ('spark.cores.max', '16'), ('spark.driver.memory', '30g')])
            sc = SparkContext(conf=config)
            self.spark_environment = SparkSession(sc)
         
            return self.spark_environment

        elif self.session_type == 'emr':

            # Building Session
            self.spark_environment = SparkSession.builder.config(
                'fs.s3a.connection.maximum', 1000).config('fs.s3a.threads.max', 2000).getOrCreate()

            return self.spark_environment
