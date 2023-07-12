import logging

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.variable import Variable

from lib import ConnectionBuilder

from deliveries.ddl.schema_init import SchemaDdl
from deliveries.stg.api_loader import ApiObjLoader

from deliveries.dds.courier_loader import DdsCourierLoader
from deliveries.dds.timestamp_order_deliv_loader import DdsTimestampsOrderDelivLoader
from deliveries.dds.rate_loader import DdsRateLoader
from deliveries.dds.fct_order_deliveries_loader import DdsFctOrderDeliveriesLoader

from deliveries.cdm.courier_ledger import CourierLedgerReportLoader



log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 0 * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg', 'dds', 'cdm', 'loader', 'deliveries'],
    is_paused_upon_creation=True 
)
def dag_deliveries():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow для подключения к API.
    url = Variable.get('URL_API')
    headers = {
    "X-API-KEY": Variable.get('X-API-KEY'),
    "X-Nickname": Variable.get('X-NICNAME'),
    "X-Cohort": Variable.get('X-COHORT')
    }

    # Задаём путь до каталога со скриптами инициализации таблиц слоя.
    ddl_path_stg = "/lessons/dags/deliveries/ddl/scripts_ddl"

    # Задаём стандартные параметры для фильтра api
    dict_filter = {'sort_field': '_id',
                'sort_direction': 'asc',
                'limit': 50,
                'offset': 0}

    @task(task_id='schema_init')
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path_stg)    
    
    # таски для stg слоя
    # выгрузка данных по API
    @task(task_id="loader_couriers")
    def loader_couriers():
        # Задаём параметры для выгрузки информации о курьерах по API
        table_name = 'api_couriers'
        method_url = '/couriers'        
        # создаем экземпляр класса, в котором реализована логика.        
        rest_loader = ApiObjLoader(dwh_pg_connect, table_name, url, method_url, headers, dict_filter, log)
        rest_loader.load_objs()  # Вызываем функцию, которая перельет данные. Далее загрузки по API аналогично.
        
    @task(task_id="loader_deliveries")
    def loader_deliveries():
        table_name = 'api_deliveries'
        method_url = '/deliveries'              
        rest_loader = ApiObjLoader(dwh_pg_connect, table_name, url, method_url, headers, dict_filter, log)
        rest_loader.load_objs() 

    # таски для dds слоя (переносим данные из stg)
    @task(task_id="dds_couriers_load")
    def load_dds_couriers():
        courier_loader = DdsCourierLoader(dwh_pg_connect, log)
        courier_loader.load_users() 

    @task(task_id="dds_timestamps_order_delivs_load")
    def load_dds_timestamps_order_delivs():
        courier_loader = DdsTimestampsOrderDelivLoader(dwh_pg_connect, log)
        courier_loader.load_timestamps_order_delivs()

    @task(task_id="dds_rates")
    def load_rates():
        rate_loader = DdsRateLoader(dwh_pg_connect, log)
        rate_loader.load_rates() 

    @task(task_id="dds_fct_ord_deliv")
    def load_fct_ord_deliv():
        fct_ord_deliv_loader = DdsFctOrderDeliveriesLoader(dwh_pg_connect, log)
        fct_ord_deliv_loader.load_fct_ord_deliv() 

    # таск для cdm слоя
    # формируем витрину для куреров за прошлый месяц
    @task(task_id="cdm_courier_ledger")
    def load_cdm_courier_ledger():
        cdm_courier_ledger_loader = CourierLedgerReportLoader(dwh_pg_connect, log)
        cdm_courier_ledger_loader.load_courier_ledger() 

    # Иницализируем схемы и таблицы    
    init_schema = schema_init()
   
    #Создаём таск группы по слоям
    #stg слой
    @task_group
    def filling_stg_layer(group_id="stg_filling"):
        couriers_loader = loader_couriers()
        deliveries_loader = loader_deliveries()

        [couriers_loader, deliveries_loader]
    
    #dds слой
    @task_group
    def filling_dds_layer(group_id="dds_filling"):
        dds_couriers_load = load_dds_couriers()
        dds_timestamps_order_delivs_load = load_dds_timestamps_order_delivs()
        dds_rates_load = load_rates()        
        fct_ord_deliv_load = load_fct_ord_deliv()

        [dds_couriers_load, dds_timestamps_order_delivs_load, dds_rates_load] >> fct_ord_deliv_load

    
        #cdm слой (делаем отдельную task_group для единообразия дага )
    @task_group
    def filling_cdm_layer(group_id="cdm_filling"):
        cdm_courier_ledger_load = load_cdm_courier_ledger()

        cdm_courier_ledger_load
    
    
    # Инициализируем таск группы
    stg_filling = filling_stg_layer()
    dds_filling = filling_dds_layer()
    cdm_filling = filling_cdm_layer()

    # Задаем последовательность выполнения тасков. 
    init_schema  >> stg_filling >> dds_filling >> cdm_filling


# Вызываем функцию, описывающую даг.
dag_deliv = dag_deliveries()  
