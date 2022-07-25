from sqlalchemy import create_engine
import sqlalchemy

import requests
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import warnings
import sqlalchemy
from sqlalchemy import create_engine
import os
import argparse


from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', None)


def sqlcol(dfparam):
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):

        if i == "description_text":
            dtypedict.update({i: sqlalchemy.types.Text()})

        if i != "description_text" and "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.VARCHAR(length=255)})

        if i == "jobPostingUrl":
            dtypedict.update({i: sqlalchemy.types.Text()})

        if i == "companyName":
            dtypedict.update({i: sqlalchemy.types.Text()})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DATE()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.BIGINT()})

    return dtypedict

################## Jobs and Country Informations ##################
jobs = ['engineer', 'scientist']

country = [['geoUrn-%3Eurn%3Ali%3Afs_geo%3A102105699,locationFallback-%3ETurkey', 'Turkey'],
           ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A103644278,locationFallback-%3EUnited%20States', 'USA']]

# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A101282230,locationFallback-%3EGermany', 'Germany']
# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A105117694,locationFallback-%3ESweden', 'Sweden']
# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A102890719,locationFallback-%3ENetherlands', 'Netherlands']
################## Cookies and Headers ##################



cookies = {
    'li_at': 'AQEDASq5JWoDZthhAAABghGGS7IAAAGCNZLPsk0AmWg1wWE_f1PbDajY_LKsZbyH8xanpKX57Vz1kuCyG6ySVrtI71noBjx7cSCcQ8MzmjUxUv83ubv_qKsnej4Nar7UZpCPn1-lqUYOQ5GVIZBXNr1x',
    'JSESSIONID': '"ajax:8174042621654042084"'
}

headers = {
    'csrf-token': 'ajax:8174042621654042084',
    'x-restli-protocol-version': '2.0.0',
}

cookies2 = {
    'li_at': 'AQEDASq5JWoDZthhAAABghGGS7IAAAGCNZLPsk0AmWg1wWE_f1PbDajY_LKsZbyH8xanpKX57Vz1kuCyG6ySVrtI71noBjx7cSCcQ8MzmjUxUv83ubv_qKsnej4Nar7UZpCPn1-lqUYOQ5GVIZBXNr1x',
    'JSESSIONID': '"ajax:8174042621654042084"'
}

headers2 = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'csrf-token': 'ajax:8174042621654042084'
}


################## jobPostingId #######################


def scrap_jobPostingId(loop=5):

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
        response2 = requests.get(f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{i}', cookies=cookies2, headers=headers2)
      except:
        print("error")
      data2 = response2.json()
      # print(data)
      # print(response.status_code)

      try:
        dataframe_data = pd.json_normalize(data2['data'])
        detail_data = detail_data.append(dataframe_data[['jobPostingId', 'title',  'localizedCostPerApplicantChargeableRegion',  'originalListedAt', 'expireAt', 'createdAt', 'listedAt', 'views', 'applies', 'formattedLocation', 'jobPostingUrl', 'jobState', 'formattedEmploymentStatus',  'description.text' ]])
      except:
        print("Request Error")

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


def main_job(loop=5):
    try:
        jobId = scrap_jobPostingId(loop)
        print("Jobs Posting Id Scraping is Succesfully")
    except Exception as e:
        print("Jobs Posting Id Scraping is Error")
        print(e)

    try:
        df_detail = job_details(jobId)
        print("Jobs Detail Scraping is Succesfully")
    except Exception as e:
        print("Jobs Detail Scraping is Error")
        print(e)

    try:
        df_timestamp = timestamp_convert(df_detail)
        # df_timestamp.columns = df_timestamp.columns.str.replace('.', '_')
        print(df_timestamp.head())
        print("Timestamp converting is Succesfully")
    except Exception as e:
        print("Timestamp converting is Error")




with DAG("main_job", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        main_job = PythonOperator(
            task_id="main_job",
            python_callable=main_job
        )

        [main_job]
