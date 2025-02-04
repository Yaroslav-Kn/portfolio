from dotenv import load_dotenv
import boto3
import os

load_dotenv()
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

S3_SERVICE_NAME = 's3'
S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'

def get_session():
    session = boto3.session.Session()

    return session.client(
        service_name=S3_SERVICE_NAME,
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def download_file_from_s3(file_name_s3: str, file_name_local: str):
    s3 = get_session()
    s3.download_file(S3_BUCKET_NAME, file_name_s3, file_name_local)