from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from datetime import datetime, timedelta
import sys


def is_hdfs_path_exists(path: str, spark: SparkSession):
    #функция для проверки наличия пути
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


def get_home_city(spark: SparkSession, path_to_actual_city: str, date: str) -> DataFrame:
    #функция для нахождения домашнего города
    START_DATE = '2022-01-01'
    list_path = input_paths(START_DATE, date, path_to_actual_city, spark)
    df = spark.read.parquet(*list_path)

    #находим максимальную дату для каждого пользователя
    window = Window.partitionBy('user_id')
    df = df.withColumn('max_date', F.max('date').over(window))
    
    #находим предыдущий город для каждого пользователя
    window = Window.partitionBy('user_id').orderBy('date')
    df = df.withColumn("city_lag_1day",F.lag("act_city", 1).over(window))
    # Выбираем строки где изменился город или последнюю строку в базе 
    # и добавляем в такие строки дату когда из данного города уехал из текущего города
    df_change_city = (df.where((F.col('act_city')!=F.col('city_lag_1day')) | 
                               (F.col('date')==F.col('max_date')) |
                               (F.col('city_lag_1day').isNull()))
                      .withColumn('end_date_in_city', F.lead('date').over(window)))
    
    #рассчитываем разницу в днях между приездом и отъездом из города
    df_change_city = df_change_city.withColumn('diff_day', F.datediff('end_date_in_city', 'date'))
    #выбираем города, в которых пользователь был непрерывно больше 27 дней
    df_change_city = df_change_city.where(F.col('diff_day') >= 27)
    
    # находим последнюю дату из всех городов, где дольше всего был пользователь больше 27 дней
    window = Window.partitionBy('user_id')
    df_change_city = df_change_city.withColumn('last_date', F.max('end_date_in_city').over(window))
    df_change_city = df_change_city.where(F.col('last_date') == F.col('end_date_in_city'))
    return df_change_city.selectExpr(["user_id", "act_city as home_city"])


def get_travel(spark: SparkSession, path_to_actual_city: str, date: str) -> DataFrame:
    # функция для нахождения  информации о количестве путешествий и списке путешествей
    START_DATE = '2022-01-01'
    list_path = input_paths(START_DATE, date, path_to_actual_city, spark)
    df = spark.read.parquet(*list_path)
    
     #находим предыдущий город для каждого пользователя
    window = Window.partitionBy('user_id').orderBy('date')
    df = df.withColumn("city_lag_1day",F.lag("act_city", 1).over(window))
    # Выбираем строки где изменился город
    df_change_city = (df.where((F.col('act_city')!=F.col('city_lag_1day')) | 
                               (F.col('city_lag_1day').isNull())))
    window = Window.partitionBy('user_id')
    
    # получаем количество путешествий
    df_change_city = df_change_city.withColumn("travel_count",F.count("act_city").over(window))    
    df_change_city.cache()
    # Создаём список путешествий
    df_travel_array = (df_change_city.groupby("user_id")
                       .agg(F.concat_ws(" ", F.collect_list(df_change_city.act_city)))
                      .withColumnRenamed('concat_ws( , collect_list(act_city))', 'travel_array'))   
    # Оставляем уникальные записи в датасете с количеством путешествий
    df_change_city = df_change_city.select('user_id', 'travel_count').distinct()

    df_change_city.unpersist()
    return df_change_city.join(df_travel_array, ['user_id'])


def get_user_travel(spark: SparkSession, date: str, path_to_actual_city: str, output_path: str):
    df = (spark.read.parquet(f'{path_to_actual_city}/date={date}')
         .select('user_id', 'act_city', 'local_time'))
    
    df_home = get_home_city(spark, path_to_actual_city, date)
    df_travel = get_travel(spark, path_to_actual_city, date)
    
    df = (df.join(df_home, ['user_id'], 'left')
         .join(df_travel, ['user_id'], 'left'))
    df.write .mode("overwrite").parquet(f"{output_path}/date={date}")    


def main():
    date = sys.argv[1]
    path_to_actual_city = sys.argv[2]
    output_path = sys.argv[3]   

    spark = (SparkSession
             .builder
             .master('yarn')
             .config("spark.executor.memory", "1g")
             .config("spark.executor.cores", 2)
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.executor.instances", "1")
             .config("spark.dynamicAllocation.minExecutors", "1")
             .config("spark.dynamicAllocation.maxExecutors", "7")
             .appName(f"GetUserTravelJob-{date}") 
             .getOrCreate())

    get_user_travel(spark, date, path_to_actual_city, output_path)


if __name__ == '__main__':
    main()