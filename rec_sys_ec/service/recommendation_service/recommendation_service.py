import logging
import requests

import os

from fastapi import FastAPI
from contextlib import asynccontextmanager

from Recommendations import Recommendations
from SimilarItems import SimilarItems
from utilits_s3 import download_file_from_s3

from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter

logger = logging.getLogger("uvicorn.error")
rec_store = Recommendations()
sim_items_store = SimilarItems()
RECOMENDATION_FOLDER = '../recommendations'
EVENTS_HOST = os.environ.get('EVENTS_HOST')
EVENTS_PORT = os.environ.get('EVENTS_PORT')
events_store_url = f"{EVENTS_HOST}:{EVENTS_PORT}"

@asynccontextmanager
async def lifespan(app: FastAPI):
    #часть кода, которая отрабатывает при запуске
    logger.info("Starting")    

    os.makedirs(RECOMENDATION_FOLDER, exist_ok=True)
    try:
        
        download_file_from_s3('recsys_ec/recommendations/recommendations.parquet', f'{RECOMENDATION_FOLDER}/recommendations.parquet')
        download_file_from_s3('recsys_ec/recommendations/top_popular.parquet', f'{RECOMENDATION_FOLDER}/top_popular.parquet')
        download_file_from_s3('recsys_ec/recommendations/similar.parquet', f'{RECOMENDATION_FOLDER}/similar.parquet')
        logger.info("Last versions recommendaions downloded")
    except Exception as e:
        logger.info(f"Downloading recommendations failed with error {e}")

    rec_store.load(
        "personal",
        f'{RECOMENDATION_FOLDER}/recommendations.parquet',
        columns=["user_id", "item_id", "score"],
    )
    rec_store.load(
        "default",
        f'{RECOMENDATION_FOLDER}/top_popular.parquet',
        columns=["item_id", "score"],
    )

    sim_items_store.load(
        f'{RECOMENDATION_FOLDER}/similar.parquet',
        columns=["item_id_1", "item_id_2", "score"],
    )

    yield
    #часть кода отрабатывает при остановке
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

# инициализируем и запускаем экпортёр метрик
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)
# Инициализируем метрики
recommendation_service_counter_pred = Counter("recommendation_service_counter_pred", "Count of predicts")
recommendation_service_counter_unique_items = Counter("recommendation_service_counter_unique_items", "Count of unique items in predicts")
app.set_of_rec_items = set([]) # множество рекомендаций данных системой (для мониторинга)

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список офлайн-рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id=user_id, k=k)

    return {"recs": recs} 

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100, k_last_events: int=5):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем список последних событий пользователя
    params = {"user_id": user_id, "k": k_last_events}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()["events"]

    # получаем список айтемов, похожих на последние, с которыми взаимодействовал пользователь
    items = []
    scores = []
    for item_id in events:
        # для каждого item_id получаем список похожих в item_similar_items
        item_similar_items = sim_items_store.get(item_id, k)
        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)

    return {"recs": recs[:k]} 

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_offline[i])
        recs_blended.append(recs_online[i])

    # добавляем оставшиеся элементы в конец
    recs_blended.extend(recs_offline[min_length:])
    recs_blended.extend(recs_online[min_length:])

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    recommendation_service_counter_pred.inc()

    new_set_predict = set(recs_blended)
    recommendation_service_counter_unique_items.inc(len(new_set_predict - app.set_of_rec_items))
    app.set_of_rec_items = app.set_of_rec_items | new_set_predict

    return {"recs": recs_blended} 