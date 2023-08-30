from pyspark.sql import SparkSession
from pyspark.context import SparkContext

class LoadedData():

    Data_Frame = None

    def __init__(self, path, expected_columns):
        spark = SparkSession \
            .builder \
            .getOrCreate() 
        
        self.Customer_Data_Frame = spark.read.format("csv")\
            .option("header", "true")\
            .option("sep",",")\
            .load(path) 
        
        if expected_columns == list(self.Data_Frame.columns):
            print ("loaded proper columns")
        else:
            print ("source file is not like expected")
            # tu dodac jeszcze cos moze, jakiego breaka czy cos

        print(type(self.Data_Frame))

    def wyswietl(self, count):
        self.Data_Frame.show(count)

    def filter_countires(self, countries):
        self.Data_Frame = self.Data_Frame.filter(self.Data_Frame.country.isin(countries))
        return self

    def rename_columns(self, old_names, new_names):
        iteration_num = 0
        for col_name in old_names:
            self.Data_Frame = self.Data_Frame.withColumnRenamed(col_name, new_names[iteration_num])
            iteration_num +=1
        return self