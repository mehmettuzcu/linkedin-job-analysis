##################################################
# Text Mining
##################################################

from warnings import filterwarnings
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image
from nltk.corpus import stopwords
#from config import *
from sqlalchemy import create_engine
import sqlalchemy
import numpy as np
import matplotlib.pyplot as plt


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
df['title'] = df['title'].str.lower()
###############################
# Punctuations
###############################

df['description_text'] = df['description_text'].str.replace('[^\w\s]', '')
df['title'] = df['title'].str.replace('[^\w\s]', '')

###############################
# Numbers
###############################

df['description_text'] = df['description_text'].str.replace('\d', '')
df['title'] = df['title'].str.replace('\d', '')

###############################
# Stopwords
###############################
#import nltk
#nltk.download('stopwords')
sw = stopwords.words('english')
df['description_text'] = df['description_text'].apply(lambda x: " ".join(x for x in str(x).split() if x not in sw))
df['title'] = df['title'].apply(lambda x: " ".join(x for x in str(x).split() if x not in sw))

df['description_text'] = df['description_text'].apply(lambda x: " ".join(set(x.split())))
###############################
# Rarewords
###############################

drops = pd.Series(' '.join(df['description_text']).split()).value_counts()[-20:]
df['description_text'] = df['description_text'].apply(lambda x: " ".join(x for x in x.split() if x not in drops))

df = df[df['title'].str.contains('data engineer')]
df.head(30)
df.info()
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
len(tf[tf['tf'] > 50].sort_values(by='tf', ascending=False))
tf[tf['words'] == 'amazon']

# tf[tf['tf'] > 50].sort_values(by='tf', ascending=False).to_excel('words.xlsx', index=False)

words = pd.read_excel("words.xlsx")
df_merge = words.merge(tf, how = "left",
                               on=['words'])

df_merge['tf'] = df_merge['tf'].apply(np.int64)
df_merge[df_merge['tf'] > 50].sort_values(by='tf', ascending=False)
