import pandas as pd
import os
from implicit.als import AlternatingLeastSquares
from sklearn.preprocessing import LabelEncoder
import scipy
import numpy as np

class Recommender_als:
    def __init__(self, new_data, path_data_floder, path_recomendation_folder):
        self.new_data = new_data
        self.path_data_floder = path_data_floder
        self.path_recomendation_folder = path_recomendation_folder
        os.makedirs(path_recomendation_folder, exist_ok=True)

    def get_recommendations(self, N: int=100):
        # обновляем данные, только если были новые события
        if self.new_data:
            items = pd.read_csv(f'{self.path_data_floder}/preprocessed/items.csv')
            events_train = pd.read_csv(f'{self.path_data_floder}/preprocessed/events_train.csv')

            events_train, items = self.encode_id(events_train, items)
            als_recommendations = self.fit_model_and_get_rec(events_train, N)            
            als_recommendations = self.create_rec_df(als_recommendations)

            #Сохраняем рекомендации
            als_recommendations = als_recommendations[["user_id", "item_id", "score"]] 
            als_recommendations.to_parquet(f'{self.path_recomendation_folder}/personal_als.parquet')

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

    def fit_model_and_get_rec(self, events_train: pd.DataFrame, N: int) -> tuple:
        # Обучаем als модель
        user_item_matrix_train = scipy.sparse.csr_matrix((
            [1] * events_train.shape[0],
            (events_train['user_id_enc'], events_train['item_id_enc'])),
            dtype=np.int8)    
        
        als_model = AlternatingLeastSquares(factors=150, iterations=10, regularization=0.05, random_state=0)
        als_model.fit(user_item_matrix_train) 

        # получаем список всех возможных user_id (перекодированных)
        user_ids_encoded = range(len(self.user_encoder.classes_))

        # получаем рекомендации для всех пользователей
        als_recommendations = als_model.recommend(
            user_ids_encoded, 
            user_item_matrix_train[user_ids_encoded], 
            filter_already_liked_items=False, N=N)         
        return als_recommendations
    
    def create_rec_df(self, als_recommendations: tuple) -> pd.DataFrame:
        # получаем рекомендации и скоры
        item_ids_enc = als_recommendations[0]
        als_scores = als_recommendations[1]

        # преобразуем в датафрейм
        als_recommendations = pd.DataFrame({
            "user_id_enc": self.user_ids_encoded,
            "item_id_enc": item_ids_enc.tolist(), 
            "score": als_scores.tolist()})
        als_recommendations = als_recommendations.explode(["item_id_enc", "score"], ignore_index=True)

        # приводим типы данных
        als_recommendations["item_id_enc"] = als_recommendations["item_id_enc"].astype("int")
        als_recommendations["score"] = als_recommendations["score"].astype("float")

        # получаем изначальные идентификаторы
        als_recommendations["user_id"] = self.user_encoder.inverse_transform(als_recommendations["user_id_enc"])
        als_recommendations["item_id"] = self.item_encoder.inverse_transform(als_recommendations["item_id_enc"])
        als_recommendations = als_recommendations.drop(columns=["user_id_enc", "item_id_enc"])
        return als_recommendations
