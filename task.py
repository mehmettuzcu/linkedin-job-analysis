from pipeline import *

try:
    jobId = scrap_jobPostingId(loop=25)
    print("Jobs Posting Id Scraping is Succesfully")
except Exception as e:
    print("Jobs Posting Id Scraping is Error")

try:
    df_detail = job_details(jobId)
    print("Jobs Detail Scraping is Succesfully")
except Exception as e:
    print("Jobs Detail Scraping is Error")

try:
    df_timestamp = timestamp_convert(df_detail)
    print("Timestamp converting is Succesfully")
except Exception as e:
    print("Timestamp converting is Error")

try:
    engine.connect()
    df_timestamp.head(n=0).to_sql(name='linkedinJobs', con=engine, if_exists='replace')
    df_timestamp.to_sql(name='linkedinJobs', con=engine, if_exists='append')
    print("Timestamp converting is Succesfully")
except Exception as e:
    print("Timestamp converting is Error")



df_timestamp.head()
df_detail.info()
