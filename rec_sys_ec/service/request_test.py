import requests
import os
import logging

logging.basicConfig(filename='test_service.log', level=logging.INFO,  encoding = 'utf-8')

EVENTS_HOST = os.environ.get('EVENTS_HOST')
EVENTS_PORT = os.environ.get('EVENTS_PORT')
events_store_url = f"{EVENTS_HOST}:{EVENTS_PORT}"
RECOMMENDATIONS_HOST = os.environ.get('RECOMMENDATIONS_HOST')
RECOMMENDATIONS_PORT = os.environ.get('RECOMMENDATIONS_PORT')
recommendations_url = f"{RECOMMENDATIONS_HOST}:{RECOMMENDATIONS_PORT}"

event_item_ids =  [283218, 171335, 105864]
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

def get_recs(user_id, event_item_ids, headers):
    for event_item_id in event_item_ids:
        resp = requests.post(events_store_url + "/put", 
                            headers=headers, 
                            params={"user_id": user_id, "item_id": event_item_id})
                            

    params = {"user_id": user_id, 'k': 10}
    resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
    resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
    resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

    recs_offline = resp_offline.json()["recs"]
    recs_online = resp_online.json()["recs"]
    recs_blended = resp_blended.json()["recs"]

    dict_recs = {
        'ofline': recs_offline,
        'online': recs_online,
        'ofline_and_ofline': recs_blended,
    }

    return dict_recs

user_id = 419841 # Пользователь, по которому есть история добавления в покупки
logging.info(f'Рекомендации для пользователя с историей добавления товара в корзину {get_recs(user_id, event_item_ids, headers)}')

user_id = 155 # Новый пользователь
logging.info(f'Рекомендации для нового пользователя {get_recs(user_id, event_item_ids, headers)}')


