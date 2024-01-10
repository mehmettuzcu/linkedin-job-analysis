import requests
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
from config import *
import re

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


f'https://www.linkedin.com/voyager/api/search/hits?decorationId=com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-25&count=25&filters=List({item[1]},resultType-%3EJOBS)&keywords=data%20engineer&origin=JOB_SEARCH_PAGE_OTHER_ENTRY&q=jserpFilters&queryContext=List(primaryHitType-%3EJOBS,spellCorrectionEnabled-%3Etrue)&start={i}&skip={i}&topNRequestedFlavors=List(HIDDEN_GEM,IN_NETWORK,SCHOOL_RECRUIT,COMPANY_RECRUIT,SALARY,JOB_SEEKER_QUALIFIED,PRE_SCREENING_QUESTIONS,SKILL_ASSESSMENTS,ACTIVELY_HIRING_COMPANY,TOP_APPLICANT)'
f'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-192&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_KEYWORD_HISTORY,keywords:junior%20data%20engineer,locationUnion:(geoId:91000000),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start={i}&skip={i}',

response = requests.get(
    'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-192&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_KEYWORD_HISTORY,keywords:junior%20data%20scientist,locationUnion:(geoId:91000000),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start=50',
    cookies=cookies,
    headers=headers,
)
data = response.json()
print(response.status_code)

pattern = r'\(([^,]*)\,'
# Use re.findall to find all matches
matches = re.findall(pattern, data['elements'][0]['jobCardUnion']['jobPostingCard']['entityUrn'])
matches[0]
data['elements'][0]['jobCardUnion']['jobPostingCard']['entityUrn']

####################################### Start
pattern = r'\(([^,]*)\,'
for item in country.items():
  for i in range(0, loop_number, 25):
    response = requests.get(f'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-192&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_KEYWORD_HISTORY,keywords:data%20engineer,locationUnion:(geoId:{item[1]}),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start={i}&skip={i}',
                            cookies=cookies, headers=headers)
    data = response.json()
    print(response.status_code)
    for i in range(0, 25):
      try:
        jobPostingId.append(re.findall(pattern, data['elements'][i]['jobCardUnion']['jobPostingCard']['entityUrn'])[0])
      except:
        jobPostingId.append(np.NaN)
      try:
        ctr_name.append(item[0])
      except:
        ctr_name.append(np.NaN)
      try:
        jobCategory.append(f'Data Engineer')
      except:
        jobCategory.append(np.NaN)
  print('finished')

df = pd.DataFrame({"jobPostingId":jobPostingId,
                   "country":ctr_name,
                   "jobCategory": jobCategory})
print(len(jobPostingId), len(ctr_name), len(jobCategory))

df = df.drop_duplicates('jobPostingId', keep='first')
df = df.loc[df['jobPostingId'].notnull()]
df['jobPostingId'] = df['jobPostingId'].apply(np.int64)
#df[df['jobPostingId'] == '3545532447']
#sql = """select "jobPostingId" from "linkedinJobs";"""
#sqldf = pd.read_sql(sql,con=engine)
#sqldf.drop_duplicates(keep='first', subset=['jobPostingId'], inplace=True)
#ids = sqldf["jobPostingId"].to_list()
#df = df[~df['jobPostingId'].isin(ids)]
len(df)
df.info()
df['jobPostingId'].nunique()


detail_data = pd.DataFrame()
for i in df['jobPostingId']:
  try:
    response = requests.get(f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{i}',cookies=cookies2, headers=headers2)
    data2 = response.json()
  except:
    print("Request Error")
  try:
    df_data = pd.json_normalize(data2['data'])
    detail_data = detail_data._append(df_data[['jobPostingId', 'title',  'localizedCostPerApplicantChargeableRegion',
                                              'originalListedAt', 'expireAt', 'createdAt', 'listedAt', 'views',
                                              'applies', 'formattedLocation', 'jobPostingUrl', 'jobState',
                                              'formattedEmploymentStatus',  'description.text']], ignore_index=True)
  except:
    pass

detail_data

####################################################### End

detail_data['applies'] = detail_data['applies'].apply(lambda x : str(x))
detail_data = detail_data[detail_data['applies'] != 'None']
detail_data["applies"] = detail_data["applies"].astype(int)
df_details = pd.merge(df, detail_data, how="left", on="jobPostingId")
df_details.columns = df_details.columns.str.replace('.', '_')

len(df_details)
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
#jobs_details3[['jobPostingId', 'title', 'localizedCostPerApplicantChargeableRegion', 'views', 'applies', 'formattedLocation', 'jobPostingUrl']].to_excel('dejobs.xlsx', index= False)

cols_dtype = sqlcol(jobs_details3)
#jobs_details3.head(n=0).to_sql(name='linkedinJobs', con=engine, if_exists='replace', index=False, dtype=cols_dtype)
jobs_details3.to_sql(name='linkedinJobs', con=engine, index=False, if_exists='append',  dtype=cols_dtype)
print("Dataframe Sent to Database Succesfully")


import pandas as pd
sql = """select * from "linkedinJobs";"""
linkedinJobs_database = pd.read_sql(sql,con=engine)
linkedinJobs_database.head()