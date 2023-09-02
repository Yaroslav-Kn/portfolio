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
dag = DAG("update_data_daily",
          schedule_interval='0 0 * * *',
          default_args=default_args
         )


#Обновляем события с учётом координат
update_geo_events = SparkSubmitOperator(
                                    task_id='update_geo_events',
                                    task_concurrency = 1,
                                    dag=dag,
                                    application ='/lessons/dags/scripts/partition.py' ,
                                    conn_id= 'yarn_spark',
                                    application_args = ['{{ds}}', 
                                                        '/user/master/data/geo/events', 
                                                        '/user/yaroslavkn/data/geo/events', 
                                                        'parquet'],
                                    )
#Обновляем список актуальных городов
actualize_city = SparkSubmitOperator(
                                    task_id='actualize_city',
                                    task_concurrency = 1,
                                    dag=dag,
                                    application ='/lessons/dags/scripts/actualize_city.py' ,
                                    conn_id= 'yarn_spark',
                                    application_args = ['{{ds}}', 
                                                        '/user/yaroslavkn/data/geo/events', 
                                                        '/user/yaroslavkn/data/catalog/geo.csv', 
                                                        '/user/yaroslavkn/data/actual_city'],
                                    )

#Обновляем список путешествий пользователей
get_user_travel = SparkSubmitOperator(
                                    task_id='get_user_travel',
                                    task_concurrency = 1,
                                    dag=dag,
                                    application ='/lessons/dags/scripts/get_user_travel.py' ,
                                    conn_id= 'yarn_spark',
                                    application_args = ['{{ds}}', 
                                                        '/user/yaroslavkn/data/actual_city',  
                                                        '/user/yaroslavkn/cdm/user_travel'],
                                    )

#Обновляем список регистраций пользователей
get_registratration = SparkSubmitOperator(
                                    task_id='get_registratration',
                                    task_concurrency = 1,
                                    dag=dag,
                                    application ='/lessons/dags/scripts/get_registratration.py' ,
                                    conn_id= 'yarn_spark',
                                    application_args = ['{{ds}}', 
                                                        '/user/yaroslavkn/data/geo/events',  
                                                        '/user/yaroslavkn/data/user_registrated'],
                                    )

#обновляем список рекомендаций для пользователей
rec_user = SparkSubmitOperator(
                                    task_id='rec_user',
                                    task_concurrency = 1,
                                    dag=dag,
                                    application ='/lessons/dags/scripts/rec_user.py',
                                    conn_id= 'yarn_spark',
                                    application_args = ['{{ds}}', 
                                                        '/user/yaroslavkn/data/geo/events',
                                                        '/user/yaroslavkn/data/catalog/geo.csv',
                                                        '/user/yaroslavkn/cdm/rec_user']
                                    )

update_geo_events >> actualize_city >> get_user_travel >> get_registratration >> rec_user
