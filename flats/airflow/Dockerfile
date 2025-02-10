FROM apache/airflow:2.7.3-python3.10 
 
COPY requirements.txt ./tmp/requirements.txt 
RUN pip install -U pip
RUN pip install -r ./tmp/requirements.txt 