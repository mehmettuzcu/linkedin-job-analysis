from pipeline import *

try:
    jobId = scrap_jobPostingId(loop=25)
    "Jobs Posting Id Scraping is Succesfully"
except Exception as e:
    "Jobs Posting Id Scraping is Error"


try:
    df_detail = job_details(jobId)
    "Jobs Detail Scraping is Succesfully"
except Exception as e:
    "Jobs Detail Scraping is Error"

try:
    df_timestamp = timestamp_convert(df_detail)
    "Timestamp converting is Succesfully"
except Exception as e:
    "Timestamp converting is Error"

df_timestamp.head()
