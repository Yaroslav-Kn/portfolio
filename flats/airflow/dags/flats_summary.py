import pendulum
from airflow.decorators import dag, task
from telegram_sender.messeges import send_telegram_success_message, send_telegram_failure_message 

@dag(
    dag_id='flats_summary',
    schedule='@once',
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    catchup=False,
    tags=['ETL', 'flats'],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_flats_summary_dataset():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, String, Float, Boolean, inspect, BigInteger
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()   
        metadata = MetaData()
        flats_summary = Table(
            'flats_summary',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer, unique=True),
            Column('floor', Integer),
            Column('is_apartment', Boolean),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('build_year', Integer),
            Column('building_type_int', String),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            Column('relative_floor', Float),
            Column('target', BigInteger),
        )

        if not inspect(conn).has_table(flats_summary.name):
            metadata.create_all(conn)

    @task()
    def extract(**kwargs):
        query = f'''
            SELECT 
            fl.id as flat_id,
            fl.floor,
            fl.is_apartment,
            fl.kitchen_area,
            fl.living_area,
            fl.rooms,
            fl.studio,
            fl.total_area,
            bld.build_year,
            bld.building_type_int,
            bld.latitude,
            bld.longitude,
            bld.ceiling_height,
            bld.flats_count,
            bld.floors_total,
            bld.has_elevator,
            fl.price as target
            FROM flats as fl
            LEFT JOIN buildings as bld ON fl.building_id = bld.id
        '''
        hook = PostgresHook('destination_db')
        with hook.get_conn() as conn:
            data = pd.read_sql(query, conn)
        return data
    
    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        from utils import remove_duplicates, del_outliers, get_relative_flat, del_small_target
        data['building_type_int'] = data['building_type_int'].astype(str)
        data = del_small_target(data, 5e5)
        data = remove_duplicates(data)
        data = del_outliers(data, ['flat_id', 'flats_count'])
        data = get_relative_flat(data)
        data = data.drop('flats_count', axis=1)
        return data
    
    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table='flats_summary',
            replace=True,
            target_fields=data.columns.to_list(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
        )
    
    data = extract()
    transformed_data = transform(data)
    create_table() >> load(transformed_data)

prepare_flats_summary_dataset()
