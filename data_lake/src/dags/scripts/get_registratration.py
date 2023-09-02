from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
import sys


def get_registratration(spark: SparkSession, 
                        date: str, 
                        path_to_geo_events: str, 
                        path_to_user_registrated: str):
    # открываем данные за определённую дату (нам нужны только данные из столбца event)
    df = spark.read.parquet(f'{path_to_geo_events}/date={date}').select('event')
    df = df.withColumn('user_id', F.col('event.user'))
    # по каждому пользователю делаем ранжирование и оставляем первое вхождение
    window = Window().partitionBy('user_id').orderBy('event.datetime')
    df = df.withColumn('rank', F.rank().over(window))
    df = df.where(F.col('rank') == 1)
    
    #пытаемся открыть датасет с уже зарегистрированными пользователями
    try:
        df_user_reg = spark.read.parquet(path_to_user_registrated)
        # оставляем в df только тех, кто не зарегистрировалсся
        df = df.join(df_user_reg, ['user_id'], 'left_anti')
    #если файл открыть не удалось, значит его нет и это первый датасет и пишем его полностью
    except:
        pass
    # записываем данные о регистрации пользователей    
    df = df.where(F.col('user_id').isNotNull())
    df.write .mode("overwrite").parquet(f"{path_to_user_registrated}/date={date}")


def main():
    date = sys.argv[1]
    path_to_geo_events = sys.argv[2]
    path_to_user_registrated = sys.argv[3]

    spark = (SparkSession
             .builder
             .master('yarn')
             .config("spark.executor.memory", "1g")
             .config("spark.executor.cores", 2)
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.executor.instances", "1")
             .config("spark.dynamicAllocation.minExecutors", "1")
             .config("spark.dynamicAllocation.maxExecutors", "7")
             .appName(f"GetRegistrationJob-{date}") 
             .getOrCreate())
    
    get_registratration(spark, date, path_to_geo_events, path_to_user_registrated)


if __name__ == '__main__':
    main()
