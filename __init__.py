from LoadedData.LoadedData import LoadedData
from FinalFile.FinalFile import FinalFile
from pyspark.context import SparkContext
import pandas as pd
import logging
import argparse

#sc = SparkContext()
#sc.setSystemProperty("hadoop.home.dir", "C:\winutils")

def load_raw_data(data_name: str, expected_columns: list) -> LoadedData:
    '''
    load_raw_data function is used to load raw data files

    - **parameters**, **types** and **return**::

          :param data_name: path and name of raw_data file
          :param expected_columns: list with expected columns
          :type data_name: string
          :type expected_columns: list
          :return: object of LoadedData class
    '''
    raw_data = LoadedData(data_name, expected_columns)
    return raw_data

def data_transformation(customer_raw_data: LoadedData, financial_raw_data: LoadedData, countries_to_filter: list):
    '''
    
    
    '''
    customer_raw_data = customer_raw_data.filter_countires(countries_to_filter)
    customer_raw_data = customer_raw_data.rename_columns(['id'],['client_identifier'])
    financial_raw_data = financial_raw_data.rename_columns(['id','btc_a','cc_t'],['client_identifier','bitcoin_address','credit_card_type'])
    final_data = FinalFile(customer_raw_data.Data_Frame, financial_raw_data.Data_Frame)
    final_data.Data_Frame.toPandas().to_csv("client_data\client_data.csv", header=True) 

parser = argparse.ArgumentParser()
parser.add_argument("--cdn", required=False, default="raw_data_files\dataset_one.csv")
parser.add_argument("--fdn", required=False, default="raw_data_files\dataset_two.csv")
parser.add_argument("--cl", required=False, default="Netherlands, United Kingdom")
args = parser.parse_args()

logging.basicConfig(filename='logs_file.log', format='%(asctime)s %(levelname)s %(message)s')

expected_Customer_columns = ['id', 'first_name', 'last_name', 'email', 'country']
expected_Financial_columns = ['id', 'btc_a', 'cc_t', 'cc_n'] 

customer_raw_data = load_raw_data(args.cdn,expected_Customer_columns)

financial_raw_data = load_raw_data(args.fdn,expected_Financial_columns)

countries_to_filter = list(args.cl.split(", "))
data_transformation(customer_raw_data,financial_raw_data,countries_to_filter)

help(load_raw_data)

exit()

