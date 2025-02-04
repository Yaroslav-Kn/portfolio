from airflow.decorators import dag, task
from telegram_sender.messeges import send_telegram_success_message, send_telegram_failure_message 
import pendulum
from utils import (
    Loader, 
    Preprocessor, 
    Recommender_top, 
    Recommender_als, 
    Recommender_similar, 
    Rancking_model)
DATA_FOLDER = './dags/data'
RECOMENDATION_FOLDER = './dags/recommendations'

@dag(
    dag_id='update_recomendetion_ec',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 28, tz="UTC"),
    catchup=False,
    tags=['rec_sys', 'ec'],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def update_recomendation():
    @task()
    def download_raw_data():
        loader = Loader(DATA_FOLDER, RECOMENDATION_FOLDER)
        loader.download_files()
        
    @task()
    def preprocess_data():
        preprocessor = Preprocessor(DATA_FOLDER)
        preprocessor.preprocess_files()
        return preprocessor.new_data
        
    @task()
    def create_top_rec(new_data):
        recomender = Recommender_top(new_data, DATA_FOLDER, RECOMENDATION_FOLDER)
        recomender.get_recommendations()

    @task()
    def create_als_rec(new_data):
        recomender = Recommender_als(new_data, DATA_FOLDER, RECOMENDATION_FOLDER)
        recomender.get_recommendations()

    @task()
    def create_similar_rec(new_data):
        recomender = Recommender_similar(new_data, DATA_FOLDER, RECOMENDATION_FOLDER)
        recomender.get_recommendations()

    @task()
    def rank_rec(new_data):
        ranker = Rancking_model(new_data, DATA_FOLDER, RECOMENDATION_FOLDER)
        ranker.get_ranking()

    @task()
    def upload_rec(new_data):
        loader = Loader(DATA_FOLDER, RECOMENDATION_FOLDER)
        loader.upload_files(new_data)
    
    new_data = download_raw_data() >> preprocess_data()
    [create_top_rec(new_data), create_als_rec(new_data), create_similar_rec(new_data)] >> rank_rec(new_data) >> upload_rec(new_data)

update_recomendation()
