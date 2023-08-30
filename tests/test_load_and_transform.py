import sys
import pandas as pd

sys.path.append('C:\\Users\\kkarwat\\Desktop\\Wazne\\nauka python\\POC0\\load_and_transform')
from load_and_transform import *

def test_load_and_transform(url):
    assert type(load_and_transform(url)) is not int, "Wrong URL or file does not contain proper data!"
    assert isinstance(load_and_transform(url), pd.DataFrame), "Expected to return pandas DataFrame"
    
test_load_and_transform('http://api.nbp.pl/api/exchangerates/tables/a?format=json')
print ("All tests passed")