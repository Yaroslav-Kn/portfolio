import pandas as pd
from datetime import datetime
import os
from datetime import timedelta

class Preprocessor:
    def __init__(self, path_data_floder):
        self.path_data_floder = path_data_floder
        self.new_events = False
        # проевряем есть ли такой файл с событиями, если нет, то у нас точно новые дданные
        if not os.path.exists(f'{path_data_floder}/preprocessed/events.csv'):
            self.new_events = True
            os.makedirs(f'{path_data_floder}/preprocessed', exist_ok=True)

    @staticmethod
    def get_events_before(df: pd.DataFrame, column_action: str, column_value: str) -> pd.DataFrame:
        list_columns_action = df[column_action].unique()

        # ставим флаг, что событие произошло
        df['flag'] = 1
        # разворачиваем таблицу в широкий вариант
        pivot_events = df.pivot_table(index=['visitorid', column_value, 'timestamp'],
                                    columns=column_action,
                                    values='flag',
                                    fill_value=0).reset_index(drop=False)
        
        # группируем и рассчитвыавем кумулитивную сумму
        pivot_events[list_columns_action] = pivot_events.groupby(['visitorid', column_value])[list_columns_action].cumsum()
        pivot_events[list_columns_action] = pivot_events[list_columns_action].astype(int)

        # переименновываем столбцы, удаляем флаг и объединяем датасеты
        pivot_events = pivot_events.rename(columns={col: f'{col}_{column_value}_before' for col in list_columns_action})    
        df = df.drop('flag', axis=1)
        df = df.merge(pivot_events, on=['visitorid', column_value, 'timestamp'], how='left')
        return df
    
    def get_items(self) -> pd.DataFrame:
        list_files = os.listdir(f'{self.path_data_floder}/raw')
        list_dataframe_items = []
        for file in list_files:
            if 'item_properties' in file:
                list_dataframe_items.append(pd.read_csv(f'{self.path_data_floder}/raw/{file}'))
        items = pd.concat(list_dataframe_items, axis=0).sort_values(by=['itemid', 'property', 'timestamp']).reset_index(drop=True)
        del list_dataframe_items

        # выбираем последнее вхождение для каждого признака, и разворачиваем таблицу
        items = items.drop_duplicates(subset=['itemid', 'property'], keep='last')
        items = items.pivot(index='itemid', 
                    columns='property', 
                    values='value').reset_index(drop=False)
        
        # выбираем только корректные признаки
        correct_columns = [
            'itemid',
            '6',
            '678',
            '839',
            'categoryid'
        ]
        items = items[correct_columns]

        # присоединяем дерево признаков
        category_tree = pd.read_csv(f'{self.path_data_floder}/raw/category_tree.csv')
        items['categoryid'] = items['categoryid'].astype(int)
        items = items.merge(category_tree, on='categoryid', how='left')
        return items

    def remove_outliers(self, events):
        # чистим выбросы
        user_events = pd.pivot_table(events, 
                            index=['visitorid', 'date'], 
                            columns=['event'], 
                            values='itemid', 
                            aggfunc='count').reset_index(drop=False)
        user_events = user_events.groupby('visitorid').agg(
            view=('view', 'mean'),
            addtocart=('addtocart', 'mean'),
            transaction=('transaction', 'mean')
        )
        user_events = user_events.fillna(0)
        user_events = user_events.loc [(user_events['view'] <= 10) &
                                (user_events['addtocart'] <= 5) &
                                (user_events['transaction'] <= 5)]

        user_list = user_events.index.to_list()
        events = events.loc[events['visitorid'].isin(user_list)]
        return events

    def get_events(self, items: pd.DataFrame) -> pd.DataFrame:
        events = pd.read_csv(f'{self.path_data_floder}/raw/events.csv')
        events = events.sort_values(['visitorid', 'timestamp', 'itemid'])
        events['date'] = events['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000).date())
        
        events = self.remove_outliers(events)
        events = self.get_events_before(events, 'event', 'itemid')

        # Присоединяем признаки товаров и получаем историю взаимодействий с категорией товара или родительской категорией.
        events = events.merge(items, on='itemid', how='left')
        events = self.get_events_before(events, 'event', 'categoryid')
        events = self.get_events_before(events, 'event', 'parentid')

        events = events.drop(['6', '678', '839', 'categoryid', 'parentid'], axis=1)

        return events
    
    def get_items_history(self) -> pd.DataFrame:
        list_files = os.listdir(f'{self.path_data_floder}/raw')
        list_dataframe_items = []
        for file in list_files:
            if 'item_properties' in file:
                list_dataframe_items.append(pd.read_csv(f'{self.path_data_floder}/raw/{file}'))
        items_history = pd.concat(list_dataframe_items, axis=0).sort_values(by=['itemid', 'property', 'timestamp']).reset_index(drop=True)
        del list_dataframe_items
        items_history = items_history[items_history['property'] == 'available']
        items_history = items_history.drop('property', axis=1)
        items_history = items_history.rename(columns={'value': 'available'})
        items_history['date'] = items_history['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000).date())

        return items_history

    def train_test_events(self, events: pd.DataFrame, n_test_day: int = 28) -> tuple[pd.DataFrame, pd.DataFrame]:
        train_test_global_time_split_date = max(pd.to_datetime(events['date'])) - timedelta(days=n_test_day)
        train_test_global_time_split_idx = pd.to_datetime(events["date"]) < train_test_global_time_split_date
        events_train = events[train_test_global_time_split_idx].reset_index(drop=True)
        events_test = events[~train_test_global_time_split_idx].reset_index(drop=True)
        return events_train, events_test

    def preprocess_files(self):
        print('Начата загрузка товаров')
        items = self.get_items()
        print('Начата загрузка действий пользователей')
        events = self.get_events(items)
        print('Начата загрузка истории товаров')
        items_history = self.get_items_history()

        events = events.rename(columns={'visitorid': 'user_id',
                            'itemid': 'item_id'})
        items_history = items_history.rename(columns={'itemid': 'item_id'})
        items = items.rename(columns={'itemid': 'item_id'})

        # если уже есть предообработанные файлы проверяем были ли какие-то новые события
        if os.path.exists(f'{self.path_data_floder}/preprocessed/events.csv'):
            old_events = pd.read_csv(f'{self.path_data_floder}/preprocessed/events.csv')
            if len(old_events) == len(events):
                self.new_events = not (events[['user_id', 'item_id', 'timestamp', 'event']] == old_events[['user_id', 'item_id', 'timestamp', 'event']]).all().all()
            else:
                self.new_events = True 
            del old_events

        # если события обновились, тогда делим события на две части (для ранжирующей модели) и сохраним новые датасеты
        if self.new_events:
            events_train, events_test = self.train_test_events(events)
            events_train.to_csv(f'{self.path_data_floder}/preprocessed/events_train.csv', index=False)
            events_test.to_csv(f'{self.path_data_floder}/preprocessed/events_test.csv', index=False)
            events.to_csv(f'{self.path_data_floder}/preprocessed/events.csv', index=False)
            items.to_csv(f'{self.path_data_floder}/preprocessed/items.csv', index=False)
            items_history.to_csv(f'{self.path_data_floder}/preprocessed/items_history.csv', index=False)
