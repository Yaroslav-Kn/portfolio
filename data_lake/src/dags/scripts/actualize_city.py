from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pytz
import sys


def change_dec_sep(df: DataFrame, column: str, old_sep: str = ',', new_sep: str = '.') -> DataFrame:
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


def get_center_with_timezone(df_center: DataFrame) -> DataFrame:
    # функция добавляет столбец с timezone в справочник (timezone будем считать по ближайшему городу c timezone)
    df_center_time_zone = (df_center.withColumn('timezone', F.concat_ws('/', F.lit('Australia'), F.col('city')))
                           .where(F.col('timezone').isin(pytz.all_timezones)))
    df_center = df_center.crossJoin(df_center_time_zone.selectExpr(['timezone', 'lat as lat2', 'lng as lng2']))
    df_center = get_min_obj_dist(df_center, 'lat', 'lat2', 'lng', 'lng2', 'city').select('city', 'lat', 'lng', 'timezone')
    return df_center

def update_city(spark: SparkSession, date: str, path_to_geo_events: str, path_to_catolog_geo: str, output_path: str):
    # открываем датасет с  событиями
    df = spark.read.parquet(f'{path_to_geo_events}/date={date}')
    df = df.select('event.user', 'lat', 'lon', 'event.datetime').where(F.col('lat').isNotNull() & F.col('lon').isNotNull())
    # открываем датасет с центрами городов
    df_center = spark.read.csv(path_to_catolog_geo, 
                               sep=';', 
                               inferSchema=True,
                               header=True) 
    # меняем десятичный разделитель c ',' на '.'
    df_center = change_dec_sep(df_center, 'lat')
    df_center = change_dec_sep(df_center, 'lng')
    # добавим столбец с timezone в справочник
    df_center = get_center_with_timezone(df_center)
    
          
    # находим ближайший город
    df = df.crossJoin(df_center.selectExpr(['city', 'lat as lat_city', 'lng as lng_city', "timezone"]))
    df = (get_min_obj_dist(df, 'lat', 'lat_city', 'lon', 'lng_city', 'user')
          .selectExpr(["user as user_id", "city as act_city", "datetime", "timezone"]))
    df = df.withColumn('date', F.lit(date))
    
    df = df.withColumn('timestamp', F.to_timestamp(F.col('datetime')))    
    df = df.withColumn('local_time', F.from_utc_timestamp(F.col("timestamp"),F.col('timezone')))
    df = df.select('user_id', 'act_city', 'date', 'local_time')
    df.write .mode("overwrite").parquet(f"{output_path}/date={date}")


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
             .appName(f"ActualizeCityJob-{date}") 
             .getOrCreate())
    
    update_city(spark, date, path_to_geo_events, path_to_catolog_geo, output_path)


if __name__ == "__main__":
        main()
