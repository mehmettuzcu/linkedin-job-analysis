##################################################
# Text Mining and Natural Language Processing
##################################################

from warnings import filterwarnings
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image
from nltk.corpus import stopwords
from config import *
from sqlalchemy import create_engine
import sqlalchemy


filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

##############################################
# 1. Text Preprocessing
##################################################

sql = """select * from "linkedinJobs";"""
df = pd.read_sql(sql,con=engine)
df.drop_duplicates(keep='first', subset=['jobPostingId'], inplace=True)
df.head()
df.info()

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
#import nltk
#nltk.download('stopwords')
sw = stopwords.words('english')
df['description_text'] = df['description_text'].apply(lambda x: " ".join(x for x in str(x).split() if x not in sw))
df.head()
###############################
# Rarewords
###############################

drops = pd.Series(' '.join(df['description_text']).split()).value_counts()[-20:]
df['description_text'] = df['description_text'].apply(lambda x: " ".join(x for x in x.split() if x not in drops))

###############################
# Terim Frekanslarının Hesaplanması
###############################

tf = df["description_text"].apply(lambda x: pd.value_counts(x.split(" "))).sum(axis=0).reset_index()

tf.columns = ["words", "tf"]
tf.head()
tf.shape
tf["words"].nunique()
tf["tf"].describe([0.05, 0.10, 0.25, 0.50, 0.75, 0.80, 0.90, 0.95, 0.99]).T
tf.head()
tf.sort_values(by='tf', ascending=False).head(300)
tf[tf['tf'] > 50].sort_values(by='tf', ascending=False)
tf[tf['words'] == 'gcp']
