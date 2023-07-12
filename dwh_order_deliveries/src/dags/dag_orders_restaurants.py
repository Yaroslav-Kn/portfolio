import logging

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.variable import Variable
from lib import ConnectionBuilder, MongoConnect

from orders_restaurants.ddl.schema_init import SchemaDdl

from orders_restaurants.stg.bonus_dag.ranks_loader import RankLoader
from orders_restaurants.stg.bonus_dag.users_loader import UserLoader
from orders_restaurants.stg.bonus_dag.outbox_loader import OutboxLoader
from orders_restaurants.stg.order_system.order_loader import OrderLoader
from orders_restaurants.stg.order_system.restaurant_loader import RestaurantLoader
from orders_restaurants.stg.order_system.user_loader import UserOrderLoader
from orders_restaurants.stg.order_system.pg_saver_mongo import PgSaver
from orders_restaurants.stg.order_system.reader_mongo import ReaderMongo

from orders_restaurants.dds.user_loader import DdsUserLoader
from orders_restaurants.dds.restaurant_loader import DdsRestaurantLoader
from orders_restaurants.dds.timestamp_loader import DdsTimestampLoader
from orders_restaurants.dds.product_loader import DdsProductLoader
from orders_restaurants.dds.order_loader import DdsOrderLoader
from orders_restaurants.dds.fct_product_sales_loader import DdsFctProduct_Sales_Loader

from orders_restaurants.cdm.settlement_report_loader import SettlementReportLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  
    tags=['stg', 'dds', 'cdm', 'orders_restaurants'],
    is_paused_upon_creation=True
)
def dag_orders_restaurants():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Задаём путь до каталога со скриптами инициализации таблиц.
    ddl_path_stg = "/lessons/dags/orders_restaurants/ddl/scripts_ddl"

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Получаем переменные из Airflow для подключения к Mongo-DB и инициализируем подключение.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

    # Объявляем таски
    # таск создания таблиц
    @task(task_id='schema_init')
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path_stg)    
    
    #таски для stg слоя
    # таски для получения данных из postgres
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные. Далее с postgres аналогично.

    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    @task(task_id="outboxs_load")
    def load_outboxs():
        rest_loader = OutboxLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_outboxs() 

    # таски для работы с Mongo-DB
    @task(task_id="orders_load")
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()
        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = ReaderMongo(mongo_connect)
        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy() # Запускаем копирование данных. Далее для других данных из Mongo-DB аналогично

    @task(task_id="restaurants_load")
    def load_restaurants():
        pg_saver = PgSaver()
        collection_reader = ReaderMongo(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task()
    def load_user_order(task_id="user_order_load"):
        pg_saver = PgSaver()
        collection_reader = ReaderMongo(mongo_connect)
        loader = UserOrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()  

    # таски для dds слоя
    @task(task_id="dds_users_load")
    def dds_load_users():
        user_loader = DdsUserLoader(dwh_pg_connect, log)
        user_loader.load_users() 

    @task(task_id="dds_restuarants_load")
    def dds_load_restaurants():
        rest_loader = DdsRestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="dds_timestamps_load")
    def dds_load_timestamps():
        timestamp_loader = DdsTimestampLoader(dwh_pg_connect, log)
        timestamp_loader.load_timestamps()

    @task(task_id="dds_products_load")
    def dds_load_products():
        product_loader = DdsProductLoader(dwh_pg_connect, log)
        product_loader.load_products()

    @task(task_id="dds_orders_load")
    def dds_load_orders():
        order_loader = DdsOrderLoader(dwh_pg_connect, log)
        order_loader.load_orders()

    @task(task_id="dds_fct_product_sales_load")
    def dds_load_fct_product_sales():
        fct_product_sales_loader = DdsFctProduct_Sales_Loader(dwh_pg_connect, log)
        fct_product_sales_loader.load_insert_fct_product_sales() 
   
    # таски дял cdm слоя
    @task(task_id="settlement_reportLoader_load")
    def load_settlement_reportLoader():
        settlement_reportLoader_loader = SettlementReportLoader(dwh_pg_connect, log)
        settlement_reportLoader_loader.load_settlementreport() 

    # Иницализируем схемы и таблицы    
    init_schema = schema_init()
   
    # Создаём таск группы по слоям
    #stg слой
    @task_group
    def filling_stg_layer(group_id="stg_filling"):
        users_loader = load_users()    
        ranks_loader = load_ranks()    
        order_loader = load_orders()
        restaurant_loader = load_restaurants()
        order_user_loader = load_user_order()
        outboxs_dict = load_outboxs()

        [users_loader, ranks_loader, outboxs_dict, order_loader, restaurant_loader, order_user_loader]
    
    #dds слой
    @task_group
    def filling_dds_layer(group_id="dds_filling"):
        dds_user_load = dds_load_users()
        dds_restaurant_load = dds_load_restaurants()
        dds_timestamp_load = dds_load_timestamps()
        dds_product_load = dds_load_products()
        dds_order_load = dds_load_orders()
        dds_fct_product_sales_load = dds_load_fct_product_sales()

        [dds_user_load, dds_restaurant_load, dds_timestamp_load] >> dds_product_load >> dds_order_load >> dds_fct_product_sales_load

    
        #cdm слой (делаем отдельную task_group для единообразия дага)
    @task_group
    def filling_cdm_layer(group_id="cdm_filling"):
        settlement_reportLoader_load = load_settlement_reportLoader()

        settlement_reportLoader_load
    
    
    # Инициализируем таски групп
    stg_filling = filling_stg_layer()
    dds_filling = filling_dds_layer()
    cdm_filling = filling_cdm_layer()

    # Задаем последовательность выполнения тасков. 
    init_schema  >> stg_filling >> dds_filling >> cdm_filling


# Вызываем функцию, описывающую даг.
full_dag_orders = dag_orders_restaurants()  
