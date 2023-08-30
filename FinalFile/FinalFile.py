from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql

class FinalFile():

    Data_Frame = None

    def __init__ (self,customer_data :pyspark.sql.dataframe.DataFrame, financial_data :pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        '''
        
        
        
        '''
        spark = SparkSession \
            .builder \
            .getOrCreate() \
        
        customer_data.createOrReplaceTempView("customer_data_view")
        financial_data.createOrReplaceTempView("financial_data_view")
        self.Data_Frame = spark.sql("""SELECT cdv.client_identifier, email, country, bitcoin_address, credit_card_type 
                                    FROM customer_data_view cdv
                                    LEFT JOIN financial_data_view fdv on cdv.client_identifier = fdv.client_identifier""") 

