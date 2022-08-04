import requests
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import warnings
import sqlalchemy
from sqlalchemy import create_engine
import os
import argparse
from config import *
from warnings import filterwarnings


warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', None)


df = pd.read_sql_query(sql='SELECT * FROM public."linkedinJobs"', con=engine)
df.head()
# df.to_excel("data.xlsx")
df.info()

cols_dtype = sqlcol(df)
df.to_sql(name='linkedinJobs', con=mysql_conn, index=False, if_exists='replace',  dtype=cols_dtype)
df.to_sql(name='linkedinJobs', con=mysql_conn, index=False, if_exists='append',  dtype=cols_dtype, chunksize=1000)

##################################################
# 1. Text Preprocessing
##################################################
###############################
# Normalizing Case Folding
###############################

df['description_text'] = df['description_text'].str.lower()

###############################
# Punctuations
###############################

df['description_text'] = df['description_text'].str.replace('[^\w\s]', '')

###############################
# Numbers
###############################

df['description_text'] = df['description_text'].str.replace('\d', '')

###############################
# Stopwords
###############################

database_connection = sqlalchemy.create_engine(
    'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(
        database_username,
        database_password,
        database_ip, database_port,
        database_name
    ), pool_recycle=3600, pool_size=5).connect()

df.to_sql(
    con=database_connection,
    name='linkedinJobs',
    index=False,
    if_exists='append',
    dtype=cols_dtype,
    chunksize=1000
)

pd.read_sql_query(sql='SHOW TABLES', con=database_connection)

df['description_text'][0:1].to_excel("aaa.xlsx")
