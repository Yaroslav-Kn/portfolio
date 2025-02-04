import boto3
from airflow.models import Variable
import os
import logging

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = Variable.get('S3_BUCKET_NAME')
S3_SERVICE_NAME = 's3'
S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'

class Loader:
    def __init__(self, path_data_floder, path_recomendation_folder):
        self.path_data_floder = path_data_floder
        self.path_recomendation_folder = path_recomendation_folder
        os.makedirs(f'{path_data_floder}/raw', exist_ok=True)

    @staticmethod
    def get_session():
        session = boto3.session.Session()

        return session.client(
            service_name=S3_SERVICE_NAME,
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

    def download_files(self) -> None:
        logging.info('Начата загрузка сырых данных данных')
        s3 = self.get_session()
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        list_data_key = [content['Key'] for content in  response['Contents'] if 'recsys_ec/data/raw/' in content['Key'] and '.csv' in content['Key']]
        for key in list_data_key:            
            file_name = key.split('/')[-1]      
            s3.download_file(S3_BUCKET_NAME, key, f'{self.path_data_floder}/raw/{file_name}')

    def upload_files(self, new_data) -> None:
        if new_data:
            logging.info('Начата загрузка рекомендаций в S3')
            s3 = self.get_session()
            for file in os.listdir(self.path_recomendation_folder):
                s3.upload_file(f'{self.path_recomendation_folder}/{file}', S3_BUCKET_NAME, f'recsys_ec/recommendations/{file}')