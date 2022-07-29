from pipeline import *

begin = datetime.now()
today = begin.strftime("%Y-%m-%d")

print(f"""
##############################################
Start Time: {begin}
""")

def daily(loop=500):
    try:
        jobId = scrap_jobPostingId(loop)
        print("Jobs Posting Id Scraping is Succesfully")
    except Exception as e:
        print("Jobs Posting Id Scraping is Error")

    try:
        df_detail = job_details(jobId)
        print("Jobs Detail Scraping is Succesfully")
    except Exception as e:
        print("Jobs Detail Scraping is Error")
        print(e)

    try:
        df_timestamp = timestamp_convert(df_detail)
        print( f'data lenght {len(df_timestamp)}')
        # df_timestamp.columns = df_timestamp.columns.str.replace('.', '_')
        print("Timestamp converting is Succesfully")
    except Exception as e:
        print("Timestamp converting is Error")

    try:
        engine.connect()
        cols_dtype = sqlcol(df_timestamp)
        df_timestamp.head(n=0).to_sql(name='linkedinJobs', con=engine, if_exists='replace', index=False, dtype=cols_dtype)
        df_timestamp.to_sql(name='linkedinJobs', con=engine, index=False, if_exists='append',  dtype=cols_dtype)

        print("Dataframe Sent to Datab se Succesfully")
    except Exception as e:
        print("Dataframe Sent to Database Error!")

    return df_timestamp

if __name__ == "__main__":

        daily()
        end = datetime.now()
        print(f"""
Finished Time: {end}
Total Time: {end - begin}    
##################################################    
    """)
