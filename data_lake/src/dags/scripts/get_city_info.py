from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys


def is_hdfs_path_exists(path: str, spark: SparkSession):
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


def get_start_end_date(end_date: str, deep: str = 'week') -> tuple:
    #функция для получения start_date (за неделю и месяц) и списка путей
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    if deep == 'week':
        start_date = end_date - timedelta(weeks=1)
    elif deep == 'month':
        start_date = end_date + relativedelta(months=-1)
    else:
        return []
    end_date = datetime.strftime(end_date, '%Y-%m-%d')
    start_date = datetime.strftime(start_date + timedelta(days=1), '%Y-%m-%d')
    return start_date, end_date

def change_dec_sep(df: DataFrame, column: str, old_sep: str = ',', new_sep: str = '.') -> DataFrame:
    #функция для замены old_sep на new_sep
    df = df.withColumn(column, F.regexp_replace(column, old_sep, new_sep))
    df = df.withColumn(column, df[column].cast("float"))
    return df


def get_min_obj_dist(df: DataFrame, lat1: str, lat2: str, lon1: str, lon2: str, col: str) -> DataFrame:
    #функция возвращает ближайший по дистанции объект по столбцу
    df = (df.withColumn('distance', F.lit(2) * F.asin(F.sqrt(F.pow(F.sin((F.col(lat1) - F.col(lat2))/F.lit(2)), F.lit(2)) 
                                                    + F.cos(F.col(lat1)) * F.cos(F.col(lat2))
                                                    * F.pow(F.sin((F.col(lon1) - F.col(lon2))/F.lit(2)), F.lit(2)))))
      )
    df = df.join(df.select(col, 'distance').groupBy(col).agg(F.min('distance')), col)
    # фильтруем датасет, ноходим минимальную дистанцию для каждого пользователя
    df = (df.where(F.col('distance') == F.col('min(distance)')))    
    return df


def get_stat(df: DataFrame) -> DataFrame:
    #функция для расчёта статистик по датасету
    df.cache()
    window = Window().partitionBy('zone_id')      
    df_message = df.where(F.col('event_type')=='message')
    df_message = (df_message.withColumn('count_message', F.count('event').over(window))
                  .select('zone_id', 'count_message')
                  .distinct())
    
    df_subscription = df.where(F.col('event_type')=='subscription')
    df_subscription = (df_subscription.withColumn('count_subscription', F.count('event').over(window))
                      .select('zone_id', 'count_subscription')
                      .distinct())
    
    df_reaction = df.where(F.col('event_type')=='reaction')
    df_reaction = (df_reaction.withColumn('count_reaction', F.count('event').over(window))
                  .select('zone_id', 'count_reaction')
                  .distinct())    
    df_user = df.where(F.col('rank')==1)
    df_user = (df_user.withColumn('count_rank', F.count('event').over(window))
                  .select('zone_id', 'count_rank')
                  .distinct())  
    df.unpersist() 
    df_message = (df_message.join(df_subscription,['zone_id'], 'outer')
                            .join(df_reaction, ['zone_id'], 'outer')
                            .join(df_user, ['zone_id'], 'outer'))
    df_message = df_message.where(F.col('zone_id').isNotNull())
    return df_message


def get_df_info(spark: SparkSession,
                date: str, 
                path_to_geo_events: str, 
                path_to_catolog_geo: str,
                path_to_user_registrated: str,
                deep: str):
    # функция для получения датасета со статистиками на заданную глубину
    
    # получаем датасет на заданную глубину
    start_date, end_date = get_start_end_date(date, deep)
    list_path_event = input_paths(start_date, end_date, path_to_geo_events, spark)
    df = (spark.read
               .option("basePath", path_to_geo_events)
               .parquet(*list_path_event))
    # открываем датасет с центрами городов
    df_center = spark.read.csv(path_to_catolog_geo, 
                               sep=';', 
                               inferSchema=True,
                               header=True) 
    # меняем десятичный разделитель c ',' на '.'
    df_center = change_dec_sep(df_center, 'lat')
    df_center = change_dec_sep(df_center, 'lng')
    
    #получаем ближайший город и сохраняем данные в кэш
    df = df.crossJoin(df_center.selectExpr(['city as zone_id', 'lat as lat_city', 'lng as lng_city']))
    df = get_min_obj_dist(df, 'lat', 'lat_city', 'lon', 'lng_city', 'event')
    df = df.drop('lat', 'lat_city', 'lon', 'lng_city', 'distance', 'min(distance)')
    
    #получаем датасет по новым пользователям
    list_path_reg = input_paths(start_date, end_date, path_to_user_registrated, spark)
    df_reg = (spark.read
                   .option("basePath", path_to_user_registrated)
                   .parquet(*list_path_reg))
    df = df.join(df_reg, ['event'], 'outer')
    #получаем информацию о количествах действий пользователя
    df = get_stat(df)      
    df = (df.withColumnRenamed('count_message', f'{deep}_message')
            .withColumnRenamed('count_reaction', f'{deep}_reaction')
            .withColumnRenamed('count_subscription', f'{deep}_subscription')
            .withColumnRenamed('count_rank', f'{deep}_user'))
    df = df.withColumn(deep, F.lit(f'{start_date} - {end_date}'))
    return df


def get_city_info(spark: SparkSession, 
                  date: str, 
                  path_to_geo_events: str, 
                  path_to_catolog_geo: str,
                  path_to_user_registrated: str,
                  output_path: str):
      
    # получаем информацию по неделям
    df_week = get_df_info(spark, date, path_to_geo_events, path_to_catolog_geo, path_to_user_registrated, 'week')
    
    # получаем информацию по месяцам
    df_month = get_df_info(spark, date, path_to_geo_events, path_to_catolog_geo, path_to_user_registrated, 'month')
                
    df_week = df_week.join(df_month, ['zone_id'], 'outer')
    df_week.write.mode("overwrite").parquet(f"{output_path}/date={date}")  


def main():
    date = sys.argv[1]
    path_to_geo_events = sys.argv[2]
    path_to_catolog_geo = sys.argv[3]
    path_to_user_registrated = sys.argv[4]
    output_path = sys.argv[5]

    spark = (SparkSession
             .builder
             .master('yarn')
             .config("spark.executor.memory", "1g")
             .config("spark.executor.cores", 2)
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.executor.instances", "1")
             .config("spark.dynamicAllocation.minExecutors", "1")
             .config("spark.dynamicAllocation.maxExecutors", "7")
             .appName(f"GetCityInfoJob-{date}")
             .getOrCreate())
    get_city_info(spark, 
                  date,
                  path_to_geo_events,
                  path_to_catolog_geo,
                  path_to_user_registrated,
                  output_path)


if __name__ == '__main__':
    main()
