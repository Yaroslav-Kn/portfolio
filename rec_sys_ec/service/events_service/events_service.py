from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Histogram
from prometheus_client import Counter

class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """
        if user_id not in self.events:
            self.events[user_id] = []
        user_events = self.events[user_id]
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events[user_id][:k]

        return user_events

events_store = EventStore()

app = FastAPI(title="events")

# инициализируем и запускаем экпортёр метрик
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)
# Инициализируем метрики
events_service_counter_put = Counter("events_service_counter_put", "Count of put actions of users")
events_service_counter_unique_users = Counter("events_service_counter_unique_users", "Count of unique user in events")
app.set_of_user = set([]) # множество рекомендаций данных системой (для мониторинга)

@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """
    events_store.put(user_id, item_id)

    events_service_counter_put.inc()
    new_set_user = set([user_id])
    events_service_counter_unique_users.inc(len(new_set_user - app.set_of_user))
    app.set_of_user = app.set_of_user | new_set_user

    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """
    events = events_store.get(user_id, k)

    return {"events": events} 