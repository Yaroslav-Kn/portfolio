import pandas as pd
import os
from implicit.als import AlternatingLeastSquares
from sklearn.preprocessing import LabelEncoder
import scipy
import numpy as np

class Recommender_similar:
    def __init__(self, new_data, path_data_floder, path_recomendation_folder):
        self.new_data = new_data
        self.path_data_floder = path_data_floder
        self.path_recomendation_folder = path_recomendation_folder
        os.makedirs(path_recomendation_folder, exist_ok=True)

    def get_recommendations(self, max_similar_items: int=100):
        # обновляем данные, только если были новые события
        if self.new_data:
            items = pd.read_csv(f'{self.path_data_floder}/preprocessed/items.csv')
            events_train = pd.read_csv(f'{self.path_data_floder}/preprocessed/events_train.csv')

            events_train, items = self.encode_id(events_train, items)
            similar_items = self.fit_model_and_get_rec(events_train, max_similar_items)   
            similar_items = self.create_rec_df( similar_items, events_train, max_similar_items)

            # сохраняем результат
            similar_items.to_parquet(f'{self.path_recomendation_folder}/similar.parquet') 

    def encode_id(self, events_train: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
        # Фильтруем нужные события и кодируем id
        events_train = events_train[events_train['event'] == 'addtocart'].reset_index()
        events_train = events_train[events_train['item_id'].isin(items['item_id'].values.tolist())]

        self.user_encoder = LabelEncoder()
        events_train["user_id_enc"] = self.user_encoder.fit_transform(events_train["user_id"])
        self.item_encoder = LabelEncoder()
        items["item_id_enc"] = self.item_encoder.fit_transform(items["item_id"])
        events_train["item_id_enc"] = self.item_encoder.transform(events_train["item_id"])
        return events_train, items

    def fit_model_and_get_rec(self, events_train: pd.DataFrame, max_similar_items: int) -> tuple:
        # Обучаем als модель
        user_item_matrix_train = scipy.sparse.csr_matrix((
            [1] * events_train.shape[0],
            (events_train['user_id_enc'], events_train['item_id_enc'])),
            dtype=np.int8)    
        
        als_model = AlternatingLeastSquares(factors=150, iterations=10, regularization=0.05, random_state=0)
        als_model.fit(user_item_matrix_train) 
        # получаем похожие объекты
        train_item_ids_enc = events_train['item_id_enc'].unique()
        
        # преобразуем в датафрейм
        similar_items = als_model.similar_items(train_item_ids_enc, N=max_similar_items+1)
        return similar_items
    
    def create_rec_df(self, similar_items: tuple, 
                      events_train: pd.DataFrame, 
                      max_similar_items: int) -> pd.DataFrame:
        
        train_item_ids_enc = events_train['item_id_enc'].unique()
        sim_item_item_ids_enc = similar_items[0]
        sim_item_scores = similar_items[1]

        similar_items = pd.DataFrame({
            "item_id_enc": np.array([[ind for _ in range(max_similar_items + 1)] for ind in train_item_ids_enc]).flatten(),
            "sim_item_id_enc": np.array(sim_item_item_ids_enc.tolist()).flatten(), 
            "score": np.array(sim_item_scores.tolist()).flatten()})
        
        similar_items["sim_item_id_enc"] = similar_items["sim_item_id_enc"].astype("int")
        similar_items["score"] = similar_items["score"].astype("float")

        # получаем изначальные идентификаторы
        item_decod = {row[0]: row[1] for row in zip(events_train['item_id_enc'].tolist(), events_train['item_id'].tolist())}
        similar_items["item_id_1"] = similar_items['item_id_enc'].map(item_decod)
        similar_items["item_id_2"] = similar_items['sim_item_id_enc'].map(item_decod)
        similar_items = similar_items.drop(columns=["item_id_enc", "sim_item_id_enc"])

        # убираем пары с одинаковыми объектами
        similar_items = similar_items.query("item_id_1 != item_id_2") 
        return similar_items