import requests
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
from config import *

warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', None)


################## jobPostingId #######################
jobPostingId = []
company_name = []
loop_number = 25
jobCategory = []
ctr_name = []


for item in country.items():
  for i in range(0, loop_number, 25):
    response = requests.get(
        f'https://www.linkedin.com/voyager/api/search/hits?decorationId=com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-25&count=25&filters=List({item[1]},resultType-%3EJOBS)&keywords=data%20engineer&origin=JOB_SEARCH_PAGE_OTHER_ENTRY&q=jserpFilters&queryContext=List(primaryHitType-%3EJOBS,spellCorrectionEnabled-%3Etrue)&start={i}&skip={i}&topNRequestedFlavors=List(HIDDEN_GEM,IN_NETWORK,SCHOOL_RECRUIT,COMPANY_RECRUIT,SALARY,JOB_SEEKER_QUALIFIED,PRE_SCREENING_QUESTIONS,SKILL_ASSESSMENTS,ACTIVELY_HIRING_COMPANY,TOP_APPLICANT)'
        , cookies=cookies, headers=headers)
    data = response.json()
    print(response.status_code)
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
        ctr_name.append(item[0])
      except:
        ctr_name.append(np.NaN)

      try:
        jobCategory.append(f'Data {j.capitalize()}')
      except:
        jobCategory.append(np.NaN)
  print('finished')


df = pd.DataFrame({"jobPostingId":jobPostingId,
                  "companyName": company_name,
                   "ctr_name":ctr_name,
                   "jobCategory": jobCategory})


print(len(jobPostingId), len(company_name),len(ctr_name),len(jobCategory))

df = df.loc[df['jobPostingId'].notnull()]
df['jobPostingId'] = df['jobPostingId'].apply(np.int64)

df.head()

df[df['jobPostingId'] == '3545532447']

#######################################


detail_data = pd.DataFrame()

for i in df['jobPostingId']:
  try:
    response = requests.get(f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{i}', cookies=cookies2, headers=headers2)
  except:
    print("Request Error")
  data2 = response.json()
  # print(data)
  # print(response.status_code)

  try:
    df_data = pd.json_normalize(data2['data'])
    detail_data = detail_data.append(df_data[['jobPostingId', 'title',  'localizedCostPerApplicantChargeableRegion',  'originalListedAt', 'expireAt', 'createdAt', 'listedAt', 'views', 'applies', 'formattedLocation', 'jobPostingUrl', 'jobState', 'formattedEmploymentStatus',  'description.text' ]])
  except:
    pass

detail_data["applies"] = detail_data["applies"].astype(int)
df_details = pd.merge(df, detail_data, how="left", on="jobPostingId")
df_details.columns = df_details.columns.str.replace('.', '_')

df_details.head()

def timestamp_convert(dataframe):

  dataframe = dataframe.loc[dataframe['createdAt'].notnull()]
  at_date = [i for i in dataframe.columns if 'At' in i]
  for col in at_date:
    dataframe[col] = dataframe[col].apply(lambda d: datetime.utcfromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    dataframe[col]= pd.to_datetime(dataframe[col])

  return dataframe


jobs_details2 = df_details.loc[df_details['createdAt'].notnull()]


jobs_details3 = timestamp_convert(jobs_details2)

jobs_details3.head()


cols_dtype = sqlcol(jobs_details3)
jobs_details3.head(n=0).to_sql(name='linkedinJobs', con=engine, if_exists='replace', index=False, dtype=cols_dtype)
jobs_details3.to_sql(name='linkedinJobs', con=engine, index=False, if_exists='append',  dtype=cols_dtype)
print("Dataframe Sent to Datab se Succesfully")


import pandas as pd
sql = """select * from "linkedinJobs";"""
linkedinJobs_database = pd.read_sql(sql,con=engine)
