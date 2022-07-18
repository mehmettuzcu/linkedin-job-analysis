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

    for ctr in country:
      for j in jobs:
        for i in range(0, loop_number, 25):
          response = requests.get(f'https://www.linkedin.com/voyager/api/search/hits?decorationId=com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-25&count=25&filters=List(timePostedRange-%3Er86400,distance-%3E25.0,sortBy-%3ER,{ctr[0]}, resultType-%3EJOBS)&keywords=data%20{j}&origin=JOB_SEARCH_PAGE_OTHER_ENTRY&q=jserpFilters&queryContext=List(primaryHitType-%3EJOBS,spellCorrectionEnabled-%3Etrue)&start={i}&skip={i}&topNRequestedFlavors=List(HIDDEN_GEM,IN_NETWORK,SCHOOL_RECRUIT,COMPANY_RECRUIT,SALARY,JOB_SEEKER_QUALIFIED,PRE_SCREENING_QUESTIONS,SKILL_ASSESSMENTS,ACTIVELY_HIRING_COMPANY,TOP_APPLICANT)', cookies=cookies, headers=headers)
          data = response.json()
          # print(response.status_code)
    
          for i in range(0,25):
            try:
              jobPostingId.append(data['elements'][i]['hitInfo']['com.linkedin.voyager.deco.jserp.WebSearchJobJserpWithSalary']['jobPostingResolutionResult']['jobPostingId'])
            except:
              jobPostingId.append(np.NaN)
    
            try:
              company_name.append(data['elements'][i]['hitInfo']['com.linkedin.voyager.deco.jserp.WebSearchJobJserpWithSalary']['jobPostingResolutionResult']['companyDetails']['com.linkedin.voyager.deco.jserp.WebJobPostingWithCompanyName']['companyResolutionResult']['name'])
            except:
              company_name.append(np.NaN)
    
            try:
              ctr_name.append(ctr[1])
            except:
              ctr_name.append(np.NaN)
    
            try:
              job_category.append(f'Data {j.capitalize()}')
            except:
              job_category.append(np.NaN)


    dataframe = pd.DataFrame({"jobPostingId":jobPostingId,
                      "companyName": company_name,
                       "country":ctr_name,
                       "jobCategory": job_category})
    
    
    dataframe = dataframe.loc[dataframe['jobPostingId'].notnull()]
    dataframe['jobPostingId'] = dataframe['jobPostingId'].apply(np.int64)

    return dataframe

# jobId = scrap_jobPostingId()

################## Jobs Details #######################


def job_details(dataframe):
    detail_data = pd.DataFrame()

    for i in dataframe['jobPostingId']:
      try:
        response = requests.get(f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{i}', cookies=cookies, headers=headers2)
      except:
        print("error")
      data = response.json()
      # print(data)
      # print(response.status_code)

      try:
        dataframe_data = pd.json_normalize(data['data'])
        detail_data = detail_data.append(dataframe_data[['jobPostingId', 'title',  'localizedCostPerApplicantChargeableRegion',  'originalListedAt', 'expireAt', 'createdAt', 'listedAt', 'views', 'applies', 'formattedLocation', 'jobPostingUrl', 'jobState', 'formattedEmploymentStatus',  'description.text' ]])
      except:
        print("Error on:", i)

    detail_data["applies"] = detail_data["applies"].astype(int)
    df_details = pd.merge(dataframe, detail_data, how="left", on="jobPostingId")
    df_details.columns = df_details.columns.str.replace('.', '_')

    return df_details

# df_detail = job_details(jobId)

################## Timestamp Convert #######################

def timestamp_convert(dataframe):

  dataframe = dataframe.loc[dataframe['createdAt'].notnull()]
  at_date = [i for i in dataframe.columns if 'At' in i]
  for col in at_date:
    dataframe[col] = dataframe[col].apply(lambda d: datetime.utcfromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    dataframe[col]= pd.to_datetime(dataframe[col])

  return dataframe


