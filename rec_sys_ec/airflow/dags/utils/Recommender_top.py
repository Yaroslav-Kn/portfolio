import pandas as pd
import os

class Recommender_top:
    def __init__(self, new_data, path_data_floder, path_recomendation_folder):
        self.new_data = new_data
        self.path_data_floder = path_data_floder
        self.path_recomendation_folder = path_recomendation_folder
        os.makedirs(path_recomendation_folder, exist_ok=True)

    def get_recommendations(self, k_top: int=100):
        # обновляем данные, только если были новые события
        if self.new_data:
            items = pd.read_csv(f'{self.path_data_floder}/preprocessed/items.csv')
            events_train = pd.read_csv(f'{self.path_data_floder}/preprocessed/events_train.csv')
            
            top_k_pop_items = self.get_top_popular(events_train, items, k_top)
            df_top_recs = self.create_rec_df(top_k_pop_items, events_train)  

            df_top_recs.to_parquet(f'{self.path_recomendation_folder}/top_popular.parquet')

    def get_top_popular(self, 
                        events_train: pd.DataFrame, 
                        items: pd.DataFrame,
                        k_top: int) -> pd.DataFrame:
        # Получаем рекомендации
        events_train = events_train[events_train['event'] == 'addtocart'].reset_index()
        events_train = events_train[events_train['item_id'].isin(items['item_id'].values.tolist())]
        item_popularity = events_train.groupby(["item_id"]).agg(count_users=("user_id", "nunique")).reset_index()

        item_popularity = item_popularity.sort_values('count_users', ascending=False).reset_index(drop=True)
        top_k_pop_items  = item_popularity.head(k_top)
        return top_k_pop_items
    
    def create_rec_df(self, top_k_pop_items: pd.DataFrame, events_train: pd.DataFrame) -> pd.DataFrame:
        #оформляем датасет в требуемый формат и сохраняем
        users = []
        recs = []
        scores = []
        for user in events_train['user_id'].unique():
            users.extend([user] * len(top_k_pop_items))
            recs.extend(top_k_pop_items['item_id'].values.tolist())
            scores.extend((top_k_pop_items['count_users'] / top_k_pop_items['count_users'].max()).values.tolist())

        df_top_recs = pd.DataFrame({'user_id': users,
                                    'item_id': recs,
                                    'score': scores})          
        return df_top_recs
            