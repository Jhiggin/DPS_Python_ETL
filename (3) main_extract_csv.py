from Queries.adw_queries import *
from Classes.loads import loads
from Classes.SQL import SQL

import pandas as pd 
import sys 
import configparser

# extract_name = sys.argv[1] # Passed in through SQL Agent Job
extract_name = 'test.csv'

# Load Config
config = configparser.ConfigParser()
config.read('Config/config.ini')

# Initialize Variables
eng_conn = config['Dev']['conn_string']
extract_path = config['Dev']['extract_path']

conn_variables = SQL('',eng_conn)
conn = conn_variables.create_connection()

sql_variables = SQL(factsales_query, conn)
salesDF = sql_variables.read_sql()

ETL_variables = loads(salesDF, extract_path, extract_name)
ETL_variables.send_to_csv()