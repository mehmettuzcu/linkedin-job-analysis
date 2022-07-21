
FROM python:3.9

LABEL maintainer ="Mehmet Tuzcu @mehmet123" version ="1.0" name ="linkedin_analaysis"

# RUN apt-get update -y


RUN apt-get update -y && \
    pip install numpy && \
    pip install argparse && \
    pip install pandas && \
    pip install urllib3 && \
    pip install pymysql && \
    pip install pytest-warnings && \
    pip install openpyxl && \
    pip install sqlalchemy && \
    pip install requests==2.7.0 && \
    pip install psycopg2-binary

WORKDIR /app

COPY . /app

# HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD  -f ["python3", "main.py"] || exit 1

ENTRYPOINT ["python3", "deneme.py"]
