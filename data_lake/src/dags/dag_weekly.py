import airflow
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2022, 1, 1),
    'end_date': datetime(2022, 6, 21),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("update_data_weekly",
          schedule_interval='0 0 * * 1',
          default_args=default_args
         )


#Обновляем статистику по городам
get_city_info = SparkSubmitOperator(
                                    task_id='get_city_info',
                                    task_concurrency = 1,
                                    dag=dag,
                                    application ='/lessons/dags/scripts/get_city_info.py' ,
                                    conn_id= 'yarn_spark',
                                    application_args = ['{{ds}}', 
                                                        '/user/yaroslavkn/data/geo/events', 
                                                        '/user/yaroslavkn/data/catalog/geo.csv', 
                                                        '/user/yaroslavkn/data/user_registrated',
                                                        '/user/yaroslavkn/cdm/city_info']
                                    )


get_city_info