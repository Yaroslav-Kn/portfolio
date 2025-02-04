import requests
import logging
from tqdm import tqdm
import time
import random
import pandas as pd
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

S3_SERVICE_NAME = 's3'
S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'
DATA_FOLDER = 'data'

EVENTS_HOST = os.environ.get('EVENTS_HOST')
EVENTS_PORT = os.environ.get('EVENTS_PORT')
events_store_url = f"{EVENTS_HOST}:{EVENTS_PORT}"
RECOMMENDATIONS_HOST = os.environ.get('RECOMMENDATIONS_HOST')
RECOMMENDATIONS_PORT = os.environ.get('RECOMMENDATIONS_PORT')
recommendations_url = f"{RECOMMENDATIONS_HOST}:{RECOMMENDATIONS_PORT}"

os.makedirs(DATA_FOLDER, exist_ok=True)


def get_session():
    session = boto3.session.Session()

    return session.client(
        service_name=S3_SERVICE_NAME,
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def get_users_and_items() -> tuple[list[int]]:
    print('Загрузка актуальных данных')
    s3 = get_session()
    s3.download_file( S3_BUCKET_NAME, 'recsys_ec/data/raw/events.csv', f'{DATA_FOLDER}/events.csv')
    s3.download_file(S3_BUCKET_NAME, 'recsys_ec/data/raw/item_properties_part1.csv', f'{DATA_FOLDER}/item_properties_part1.csv')
    s3.download_file(S3_BUCKET_NAME, 'recsys_ec/data/raw/item_properties_part2.csv', f'{DATA_FOLDER}/item_properties_part2.csv')

    print('Получение списков уникальных пользователей и товаров')
    events = pd.read_csv(f'{DATA_FOLDER}/events.csv')
    users_list = events['visitorid'].unique().tolist()

    items = pd.concat([pd.read_csv(f'{DATA_FOLDER}/item_properties_part1.csv'),
                    pd.read_csv(f'{DATA_FOLDER}/item_properties_part2.csv')
                    ], axis=0)
    items_list = items['itemid'].unique().tolist()
    del events, items 
    return users_list, items_list

def do_action(users_list: list[int], items_list: list[int]):
    new_user_id = max(users_list) + 1
    # Эмитируем действие 40 случайных пользователей
    for i in tqdm(range(40), desc='Действие пользователей и получение прогнозов'):
        # каждый 10 пользователь будет новым 
        if i % 10 == 0:
            user_id = new_user_id
            new_user_id += 1
        else:
            user_id = users_list[random.randint(0, len(users_list))]

        # каждый пользователь добавит в корзину от 1 до 5 товаров
        count_items = random.randint(1, 5)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        for i in range(count_items):
            item_id = items_list[random.randint(0, len(items_list))]
            requests.post(events_store_url + "/put", 
                        headers=headers, 
                        params={"user_id": user_id, "item_id": item_id})
            time.sleep(max([random.normalvariate(2, 0.75), 1]))
            logging.info(f"user {user_id} added item {item_id}")

        # Получаем рекомендации
        params = {"user_id": user_id, 'k': 10}
        recommendation = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)
        logging.info(f"user {user_id} get recomendation: {recommendation}")
        
        if i == 30:
            time.sleep(20) # После 30 прерываем запросы на 20 секунд
        time.sleep(max([random.normalvariate(2, 0.5), 1]))

users_list, items_list = get_users_and_items()
do_action(users_list, items_list)