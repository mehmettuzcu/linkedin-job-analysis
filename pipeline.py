import requests
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import warnings
import sqlalchemy
from sqlalchemy import create_engine
import os, re
import argparse
from config import *

warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', None)


################## jobPostingId #######################

def scrap_jobPostingId(loop=25):
    jobPostingId = []
    company_name= []
    loop_number = loop
    ctr_name = []
    job_category = []
    pattern = r'\(([^,]*)\,'
    for item in country.items():
        for i in range(0, loop_number, 25):
            response = requests.get(
                f'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-192&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_KEYWORD_HISTORY,keywords:data%20engineer,locationUnion:(geoId:{item[1]}),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start={i}&skip={i}',
                cookies=cookies, headers=headers)
            data = response.json()
            #print(response.status_code)
            for i in range(0, 25):
                try:
                    jobPostingId.append(
                        re.findall(pattern, data['elements'][i]['jobCardUnion']['jobPostingCard']['entityUrn'])[0])
                except:
                    jobPostingId.append(np.NaN)
                try:
                    ctr_name.append(item[0])
                except:
                    ctr_name.append(np.NaN)
                try:
                    job_category.append(f'Data Engineer')
                except:
                    job_category.append(np.NaN)
        #print('finished')

    dataframe = pd.DataFrame({"jobPostingId":jobPostingId,
                       "country":ctr_name,
                       "jobCategory": job_category})

    dataframe = dataframe.drop_duplicates('jobPostingId', keep='first')
    dataframe = dataframe.loc[dataframe['jobPostingId'].notnull()]
    dataframe['jobPostingId'] = dataframe['jobPostingId'].apply(np.int64)

    sql = """select "jobPostingId" from "linkedinJobs";"""
    sqldf = pd.read_sql(sql,con=engine)
    sqldf.drop_duplicates(keep='first', subset=['jobPostingId'], inplace=True)
    ids = sqldf["jobPostingId"].to_list()
    dataframe = dataframe[~dataframe['jobPostingId'].isin(ids)]
    return dataframe

#jobId = scrap_jobPostingId(25)

################## Jobs Details #######################


def job_details(dataframe):
    detail_data = pd.DataFrame()
    for i in dataframe['jobPostingId']:
        try:
            response = requests.get(f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{i}', cookies=cookies2,
                                    headers=headers2)
            data2 = response.json()
        except:
            print("Request Error")
        try:
            df_data = pd.json_normalize(data2['data'])
            detail_data = detail_data._append(
                df_data[['jobPostingId', 'title', 'localizedCostPerApplicantChargeableRegion',
                         'originalListedAt', 'expireAt', 'createdAt', 'listedAt', 'views',
                         'applies', 'formattedLocation', 'jobPostingUrl', 'jobState',
                         'formattedEmploymentStatus', 'description.text']], ignore_index=True)
        except:
            pass

    detail_data['applies'] = detail_data['applies'].apply(lambda x : str(x))
    detail_data = detail_data[detail_data['applies'] != 'None']
    detail_data["applies"] = detail_data["applies"].astype(int)
    df_details = pd.merge(dataframe, detail_data, how="left", on="jobPostingId")
    df_details.columns = df_details.columns.str.replace('.', '_')

    return df_details

#df_detail = job_details(jobId)

################## Timestamp Convert #######################

def timestamp_convert(dataframe):

  dataframe = dataframe.loc[dataframe['createdAt'].notnull()]
  at_date = [i for i in dataframe.columns if 'At' in i]
  for col in at_date:
    dataframe[col] = dataframe[col].apply(lambda d: datetime.utcfromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    dataframe[col]= pd.to_datetime(dataframe[col])

  return dataframe
#df_timestamp = timestamp_convert(df_detail)
