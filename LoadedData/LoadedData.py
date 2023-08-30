from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import logging

class LoadedData():

    Data_Frame = None

    def __init__(self, path :str, expected_columns: list):
        '''
        
        
        '''
        spark = SparkSession \
            .builder \
            .getOrCreate() \
        
        self.Data_Frame = spark.read.format("csv")\
            .option("header", "true")\
            .option("sep",",")\
            .load(path) 
        
        if expected_columns != list(self.Data_Frame.columns):
            logging.error('Wrong column names inside of file')

    def filter_countires(self, countries :list) -> 'LoadedData': # applicable only for client data
        '''
        
        '''
        self.Data_Frame = self.Data_Frame.filter(self.Data_Frame.country.isin(countries))
        return self

    def rename_columns(self, old_names :list, new_names :list) -> 'LoadedData':
        '''
        
        
        '''
        iteration_num = 0
        for col_name in old_names:
            self.Data_Frame = self.Data_Frame.withColumnRenamed(col_name, new_names[iteration_num])
            iteration_num +=1
        return self