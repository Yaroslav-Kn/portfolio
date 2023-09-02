from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from datetime import datetime, timedelta
import pytz
import sys


def is_hdfs_path_exists(path: str, spark: SparkSession) -> bool:
    #функция для проверки существования пути
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))


def input_paths(start_date: str, end_date: str, path_data: str, spark: SparkSession) -> list:
    #функция формирующая  список существующих путей между начальной и конечной датой
    list_date = []
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    date_i = start_date
    while date_i <= end_date:
        date = datetime.strftime(date_i, '%Y-%m-%d')
        path_i = path_data + f'/date={date}'
        if is_hdfs_path_exists(path_i, spark):
            list_date.append(path_i)        
        date_i = date_i + timedelta(days=1)
    return list_date


def get_distance(df: DataFrame, lat1: str, lat2: str, lon1: str, lon2: str) -> DataFrame:
    #функция возвращает ближайший по дистанции объект по столбцу
    df = (df.withColumn('distance', F.lit(2) * F.asin(F.sqrt(F.pow(F.sin((F.col(lat1) - F.col(lat2))/F.lit(2)), F.lit(2)) 
                                                    + F.cos(F.col(lat1)) * F.cos(F.col(lat2))
                                                    * F.pow(F.sin((F.col(lon1) - F.col(lon2))/F.lit(2)), F.lit(2)))))
      )
  
    return df

def get_min_obj_dist(df: DataFrame, lat1: str, lat2: str, lon1: str, lon2: str, col: str) -> DataFrame:
    #функция возвращает ближайший по дистанции объект по столбцу    
    df = get_distance(df, lat1, lat2, lon1, lon2)
    df = df.join(df.select(col, 'distance').groupBy(col).agg(F.min('distance')), col)
    # фильтруем датасет, ноходим минимальную дистанцию для каждого пользователя
    df = (df.where(F.col('distance') == F.col('min(distance)')))
    return df


def change_dec_sep(df: DataFrame, column: str, old_sep: str = ',', new_sep: str = '.') -> DataFrame:
    # функция для замены десятичного разделителя
    df = df.withColumn(column, F.regexp_replace(column, old_sep, new_sep))
    df = df.withColumn(column, df[column].cast("float"))
    return df


def get_center_with_timezone(df_center: DataFrame) -> DataFrame:
    # функция добавляет столбец с timezone в справочник (timezone будем считать по ближайшему городу c timezone)
    df_center_time_zone = (df_center.withColumn('timezone', F.concat_ws('/', F.lit('Australia'), F.col('city')))
                           .where(F.col('timezone').isin(pytz.all_timezones)))
    df_center = df_center.crossJoin(df_center_time_zone.selectExpr(['timezone', 'lat as lat2', 'lng as lng2']))
    df_center = get_min_obj_dist(df_center, 'lat', 'lat2', 'lng', 'lng2', 'city').select('city', 'lat', 'lng', 'timezone')
    return df_center


def get_city(spark: SparkSession, 
             df: DataFrame,  
             path_to_catolog_geo: str, 
             lat1: str, 
             lon1: str) -> DataFrame:
    #функция для получения ближайшего города
    df_center = spark.read.csv(path_to_catolog_geo, 
                               sep=';', 
                               inferSchema=True,
                               header=True) 
    # меняем десятичный разделитель c ',' на '.'
    df_center = change_dec_sep(df_center, 'lat')
    df_center = change_dec_sep(df_center, 'lng')
    #получаем timezone
    df_center = get_center_with_timezone(df_center)
    
    #получаем ближайший город и сохраняем данные в кэш
    df = df.crossJoin(df_center.selectExpr(['city as zone_id', 'lat as lat_city', 'lng as lng_city', 'timezone']))
    df = get_min_obj_dist(df, lat1, 'lat_city', lon1, 'lng_city', 'user_for_contact')
        
    df = df.drop('lat_city', 'lng_city', 'distance', 'min(distance)')
    return df


def get_old_contact(spark: SparkSession, 
                    date: str, 
                    path_to_geo_events: str) -> DataFrame:
    #функци для получения старых контактов (дф с шифраторами пар пользователей, которые контактировали друг с другом)
    START_TIME = '2022-01-01'
    list_path_event = input_paths(START_TIME, date, path_to_geo_events, spark)
    df = (spark.read
               .option("basePath", path_to_geo_events)
               .parquet(*list_path_event)
                .where('event_type=="message" and event.message_to is not null'))
    #добавим столбцы с пользователями при этом, user_1 всегда будет иметь меньший порядок, чем user_2
    df = df.withColumn('user_1', F.least(F.col('event.message_from'),F.col('event.message_to')))    
    df = df.withColumn('user_2', F.greatest(F.col('event.message_from'),F.col('event.message_to')))
    df = df.withColumn('user_contact', F.concat_ws('_', F.col('user_1'), F.col('user_2')))
    return df.select('user_contact').distinct()


def get_user_in_chanel(spark: SparkSession, 
                        date: str, 
                        path_to_geo_events: str) -> DataFrame:
    #функция для получения пользователей в одном канале
    START_TIME = '2022-01-01'
    list_path_event = input_paths(START_TIME, date, path_to_geo_events, spark)
    df = (spark.read
               .option("basePath", path_to_geo_events)
               .parquet(*list_path_event)
               .where(F.col('event.channel_id').isNotNull())
               .select('event.message_from', 'event.channel_id')
               .withColumnRenamed('message_from', 'user_1')
               .withColumnRenamed('channel_id', 'channel_id_1'))
    # найдём все варианты пересечений пользователей
    df = df.crossJoin(df.selectExpr(['user_1 as user_2', 'channel_id_1 as channel_id_2']))
    # оставим только тех, кто в одном канале
    df.where('"channel_id_1"=="channel_id_2" and "user_1"!="user_2"')
    # Создадим сурагатный ключ (11 наименьший из пары)
    df = df.withColumn('user_11', F.least(F.col('user_1'),F.col('user_2')))    
    df = df.withColumn('user_22', F.greatest(F.col('user_1'),F.col('user_2')))
    df = df.withColumn('user_for_contact', F.concat_ws('_', F.col('user_11'), F.col('user_22')))
    return df.select('user_for_contact').distinct()


def rec_user(spark: SparkSession,
             date: str,
             path_to_geo_events: str,
             path_to_catolog_geo: str,
             output_path: str):
    #функция для формирования витрины
    df = (spark.read.parquet(f'{path_to_geo_events}/date={date}')
          .where('event_type=="message"'))

    #добавим столбец с пользователями объединим кроссджойн, чтобы перебрать все варианты за день
    df_user = (df.withColumn('user_1', F.col('event.message_from'))
               .selectExpr(['user_1', 'lat as lat_1', 'lon as lon_1']))
    df_user = df_user.crossJoin(df_user.selectExpr(['user_1 as user_2', 'lat_1 as lat_2', 'lon_1 as lon_2']))
    # получим расстояние между пользователями
    df_user = (get_distance(df_user, 'lat_1', 'lat_1', 'lon_1', 'lon_2')
               .select('user_1', 'user_2', 'distance',  'lat_1', 'lon_1'))
    # отфильтруем данные уберём ссылку на самого пользователя и пользователей с расстоянием больше 1км
    df_user = df_user.where((F.col('user_1')!=F.col('user_2')) & (F.col('distance')<=1))
    #сделаем столбец с шифром в котором будут указаны оба пользователя, 
    #при этом пользователь с меньшим id будет стоять первым, это поможет проще разобраться с дубликатами
    df_user = (df_user.withColumn('user_for_contact',
                                  F.concat_ws('_', F.least(F.col('user_1'),F.col('user_2')), 
                                                  F.greatest(F.col('user_1'),F.col('user_2')))))
    # для каждой пары пользователей оставим только первое вхождение
    window = Window().partitionBy('user_for_contact').orderBy('user_1')
    df_user = (df_user.withColumn('rank', F.rank().over(window))
              .where(F.col('rank')==1))
    # получим список старых контактов
    df_old_contact = get_old_contact(spark, date, path_to_geo_events)
    df_user = df_user.join(df_old_contact, 
                           df_user.user_for_contact == df_old_contact.user_contact,
                          'left_anti')  
    
    #получим список пользователей в одной группе
    df_user_in_one_channel = get_user_in_chanel(spark, date, path_to_geo_events)
    df_user = df_user.join(df_user_in_one_channel, ['user_for_contact'], 'inner') 
    #получим ближайший город и таймзоны
    df_user = get_city(spark, df_user, path_to_catolog_geo, 'lat_1', 'lon_1')
    # выберем только нужные столбцы
    df_user = df_user.selectExpr(['user_1 as user_left', 'user_2 as user_right', 'zone_id', 'timezone' ])
    df_user = df_user.distinct()
    #добавим дату формирования витрины и локальное время
    df_user = df_user.withColumn('processed_dttm', F.current_timestamp())
    df_user = df_user.withColumn('local_time', F.from_utc_timestamp(F.col("processed_dttm"),F.col('timezone')))     
    df_user = df_user.drop('timezone')
    df_user.write.mode("overwrite").parquet(f"{output_path}/date={date}")


def main():
        
    date = sys.argv[1]
    path_to_geo_events = sys.argv[2]
    path_to_catolog_geo = sys.argv[3]
    output_path = sys.argv[4]  

    spark = (SparkSession
             .builder
             .master('yarn')
             .config("spark.executor.memory", "1g")
             .config("spark.executor.cores", 2)
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.executor.instances", "1")
             .config("spark.dynamicAllocation.minExecutors", "1")
             .config("spark.dynamicAllocation.maxExecutors", "7")
             .appName(f"RecUserJob-{date}")
             .getOrCreate())

    rec_user(spark, date, path_to_geo_events, path_to_catolog_geo, output_path)


if __name__ == '__main__':
    main()
