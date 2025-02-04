import pandas as pd
import os
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics.pairwise import cosine_similarity
import scipy
import numpy as np
from catboost import CatBoostClassifier, Pool

class Rancking_model:
    def __init__(self, new_data, path_data_floder, path_recomendation_folder):
        self.new_data = new_data
        self.path_data_floder = path_data_floder
        self.path_recomendation_folder = path_recomendation_folder
        os.makedirs(path_recomendation_folder, exist_ok=True)

    @staticmethod
    def get_item2porp_matrix(items: pd.DataFrame, column_prop: str) -> scipy.sparse.csr_matrix:
        '''Функция для получения разряженной матрицы свойств товаров'''
        prop_names_to_id = {k: v for v, k in enumerate(items[column_prop].unique())}
        csr_data = []
        csr_row_idx = []
        csr_col_idx = []
        
        for item_idx, (k, v) in enumerate(items.iterrows()):
            if np.isnan(v['parentid']):
                continue
            prop_idx = prop_names_to_id[v[column_prop]]
            csr_data.append(int(1))
            csr_row_idx.append(item_idx)
            csr_col_idx.append(prop_idx)
        prop_csr = scipy.sparse.csr_matrix((csr_data, (csr_row_idx, csr_col_idx)), shape=(len(items), len(items[column_prop].unique())))
        return prop_csr 
    
    def get_all_prop_matrix(self, items: pd.DataFrame, list_columns: list[str]) -> scipy.sparse.csr_matrix:
        res = None
        for col in list_columns:
            if res is None:
                res = self.get_item2porp_matrix(items, col)
            else:
                res = scipy.sparse.hstack((res, self.get_item2porp_matrix(items, col)))
        return res
    
    def get_content_rec(self, df: pd.DataFrame, 
                        type_events: str,
                        df_user_als_item_list: pd.DataFrame,
                        df_top_item: pd.DataFrame,
                        all_items_prop_csr: scipy.sparse._csr.csr_matrix) -> pd.DataFrame:
        '''Функция вовзращает косинусное расстояние от среднего вектора пользователя до товара'''
        df = df.loc[df['event'] == type_events].reset_index(drop=True)
        # получаем все уникальные айтемы для конкретного пользователя
        df_user_item_list = df[['user_id', 'item_id_enc']].groupby('user_id').agg({'item_id_enc': 'unique'})

        user_ids = df['user_id'].unique()
        user_id_list = []
        item_id_enc_list = []
        score_list = []
        
        for user_id in user_ids:
            # Получаем лист рекомендованных эйтемов (если есть персональные, то добавляем их, плюс всем топ популяреных)
            list_id_rec = df_top_item['item_id_enc'].tolist()
            if user_id in df_user_als_item_list['user_id'].to_list():
                list_id_rec.extend(df_user_als_item_list.loc[df_user_als_item_list['user_id'] == user_id, 'item_id_enc'].tolist())

            list_id_rec = list(set(list_id_rec))
            k = len(list_id_rec)
            # для каждого пользователя получаем средний вектор товаров
            user_prop = all_items_prop_csr[df_user_item_list.loc[user_id, 'item_id_enc']].mean(axis=0).tolist()[0]        
            user_prop = np.array(user_prop).reshape(1, -1)    
            # вычисляем сходство между вектором пользователя и всеми товарами
            similarity_scores = cosine_similarity(all_items_prop_csr[list_id_rec], user_prop)
            # преобразуем в одномерный массив
            similarity_scores = similarity_scores.flatten()
            
            user_id_list.extend([user_id] * k)
            item_id_enc_list.extend(list_id_rec)
            score_list.extend(similarity_scores.tolist())
        df_rec = pd.DataFrame({
            'user_id': np.array(user_id_list),
            'item_id': self.item_encoder.inverse_transform(item_id_enc_list),
            'score': np.array(score_list)
        })

        return df_rec

    def get_data(self):
        items = pd.read_csv(f'{self.path_data_floder}/preprocessed/items.csv')
        events_train = pd.read_csv(f'{self.path_data_floder}/preprocessed/events_train.csv')

        # Фильтруем нужные события и кодируем id
        events_train = events_train[events_train['item_id'].isin(items['item_id'].values.tolist())]

        self.user_encoder = LabelEncoder()
        events_train["user_id_enc"] = self.user_encoder.fit_transform(events_train["user_id"])
        self.item_encoder = LabelEncoder()
        items["item_id_enc"] = self.item_encoder.fit_transform(items["item_id"])
        events_train["item_id_enc"] = self.item_encoder.transform(events_train["item_id"])

        als_recommendations = pd.read_parquet(f'{self.path_recomendation_folder}/personal_als.parquet')
        df_top_recs = pd.read_parquet(f'{self.path_recomendation_folder}/top_popular.parquet')
        als_recommendations['item_id_enc'] = self.item_encoder.transform(als_recommendations['item_id'])
        df_top_recs['item_id_enc'] = self.item_encoder.transform(df_top_recs['item_id'])
        
        return events_train, items, als_recommendations, df_top_recs

    def get_cosin_sim_df(self) -> pd.DataFrame:    
        # получаем датасет из косинусного расстояния
        events_train, items, als_recommendations, df_top_recs = self.get_data()
        items = items.sort_values(by="item_id_enc")
        all_items_prop_csr = self.get_all_prop_matrix(items, ['6', '678', '839', 'categoryid', 'parentid'])

        df_addtocart = self.get_content_rec(events_train, 'addtocart', als_recommendations, df_top_recs, all_items_prop_csr)
        df_view = self.get_content_rec(events_train, 'view', als_recommendations, df_top_recs, all_items_prop_csr)
        df_transaction = self.get_content_rec(events_train, 'transaction', als_recommendations, df_top_recs, all_items_prop_csr)

        df_addtocart = df_addtocart.rename(columns={'score': 'cosin_sim_addtocart'})
        df_view = df_view.rename(columns={'score': 'cosin_sim_view'})
        df_transaction = df_transaction.rename(columns={'score': 'cosin_sim_transaction'})

        df_cosin_sim = df_addtocart.merge(df_view, on = ['user_id', 'item_id'], how='outer')
        df_cosin_sim = df_cosin_sim.merge(df_transaction, on = ['user_id', 'item_id'], how='outer')
        df_cosin_sim = df_cosin_sim.fillna(0)
        return df_cosin_sim
    
    def get_candidates_df(self) -> pd.DataFrame:
        als_recommendations = pd.read_parquet(f'{self.path_recomendation_folder}/personal_als.parquet')
        df_top_recs = pd.read_parquet(f'{self.path_recomendation_folder}/top_popular.parquet')
        candidates = pd.merge(
            als_recommendations[["user_id", "item_id", "score"]].rename(columns={"score": "als_score"}),
            df_top_recs[["user_id", "item_id", "score"]].rename(columns={"score": "top_score"}),
            on=["user_id", "item_id"],
            how="outer") 

        events_train = pd.read_csv(f'{self.path_data_floder}/preprocessed/events_train.csv')
        events_last = events_train.drop_duplicates(subset=['user_id', 'item_id'], keep='last')
        events_last = events_last.drop(['timestamp', 'event', 'date'], axis=1)
        candidates = candidates.merge(events_last, 
            on=["user_id", "item_id"],
            how="outer") 
        
        df_cosin_sim = self.get_cosin_sim_df()
        candidates = candidates.merge(df_cosin_sim, 
            on=["user_id", "item_id"],
            how="outer") 
        
        candidates['not_rec'] = 0
        candidates.loc[candidates[['als_score', 'top_score']].isna().any(axis=1), 'not_rec'] = 1
        candidates = candidates.fillna(0)
        return candidates
    
    def get_df_for_train(self, candidates, negatives_per_user: int = 4) -> pd.DataFrame:
        # на основе отложенной выборки осуществляем разметку таргета
        events_test = pd.read_csv(f'{self.path_data_floder}/preprocessed/events_test.csv')
        events_test = events_test[events_test['event'] == 'addtocart'].reset_index(drop=True)
        events_test["target"] = 1        
        candidates = candidates.merge(events_test[["user_id", "item_id", "target"]], 
                                    on=["user_id", "item_id"],
                                    how='left')
        candidates["target"] = candidates["target"].fillna(0).astype("int")
        
        # в кандидатах оставляем только тех пользователей, у которых есть хотя бы один положительный таргет
        candidates_to_sample = candidates.groupby("user_id").filter(lambda x: x["target"].sum() > 0)

        # для каждого пользователя оставляем только 4 негативных примера
        candidates_for_train = pd.concat([
            candidates_to_sample.query("target == 1"),
            candidates_to_sample.query("target == 0") \
                .groupby("user_id") \
                .apply(lambda x: x.sample(negatives_per_user, random_state=0))
            ]) 
        return candidates_for_train
    
    def create_and_fit_model(self, candidates_for_train: pd.DataFrame) -> CatBoostClassifier:
        # задаём имена колонок признаков и таргета
        list_remove = ['user_id',  'item_id', 'target']
        features = [c for c in candidates_for_train.columns.to_list() if c not in list_remove]
        target = 'target'

        train_data = Pool(
            data=candidates_for_train[features], 
            label=candidates_for_train[target])     
        
        cb_model = CatBoostClassifier(
            iterations=1000,
            learning_rate=0.1,
            depth=6,
            loss_function='Logloss',
            verbose=100,
            random_seed=0
        )

        cb_model.fit(train_data) 
        return cb_model

    def get_ranking(self, max_recommendations_per_user: int = 100):        
        # обновляем данные, только если были новые события
        if self.new_data:
            candidates = self.get_candidates_df()    
            candidates_for_train = self.get_df_for_train(candidates)
            model = self.create_and_fit_model(candidates_for_train)

            # ранжируем рекомендации для каждого пользователя
            list_remove = ['user_id',  'item_id', 'target']
            features = [c for c in candidates_for_train.columns.to_list() if c not in list_remove]
            inference_data = Pool(data=candidates[features])
            predictions = model.predict_proba(inference_data)
            candidates["cb_score"] = predictions[:, 1]
            # для каждого пользователя проставим rank, начиная с 1 — это максимальный cb_score
            candidates = candidates.sort_values(["user_id", "cb_score"], ascending=[True, False])
            candidates["rank"] = candidates.groupby("user_id").cumcount().add(1)
            
            final_recommendations = candidates.query("rank <= @max_recommendations_per_user") 
            final_recommendations = final_recommendations[['user_id', 'item_id', 'cb_score', 'rank']]
            final_recommendations = final_recommendations.rename(columns={'cb_score': 'score'})

            final_recommendations.to_parquet(f'{self.path_recomendation_folder}/recommendations.parquet') 