from Classes.extracts import extracts
from Classes.transforms import transforms
from Classes.loads import loads

import pandas as pd
import configparser

# Load Config
config = configparser.ConfigParser()
config.read('Config/config.ini')


extract_path = 'File_Storage/in/breweries.csv'
statecode_path = 'File_Storage/in/State_List.csv'
output_path = 'File_Storage/out/'

extract_variables = extracts(extract_path)
df = extract_variables.read_csv_file()

transform_variables = transforms(df, 'key', '/')
df2 = transform_variables.split_columns()
df2.rename(columns = {0:'Country_Code', 1:'State_Nbr', 2:'City', 3:'Street_Address'}, inplace = True)

results = pd.merge(df, df2, left_index=True, right_index=True)
results['State_Nbr'] = pd.to_numeric(results['State_Nbr'])

extract_variables = extracts(statecode_path)
state_codes = extract_variables.read_csv_file()

transform_variables = transforms(results, state_codes, 'State_Nbr')
final = transform_variables.transform_state()

load_variables = loads(final, output_path, 'test.csv')
load_variables.send_to_csv()

