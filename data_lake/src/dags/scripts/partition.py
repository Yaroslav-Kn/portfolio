import sys 
from pyspark.sql import SparkSession

 
def main():
        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]
        type_file = sys.argv[4]

        spark = (SparkSession
                 .builder
                 .config("spark.executor.memory", "2g")
                 .config("spark.executor.cores", 2)
                 .appName(f"EventsPartitioningJob-{date}")                 
                 .getOrCreate())

        if type_file == 'json':
                events = spark.read.json(f"{base_input_path}/date={date}")
        elif type_file == 'parquet':
                events = spark.read.parquet(f"{base_input_path}/date={date}")
        else:
                raise TypeError('read file format not as expected')

        events.write \
              .partitionBy("event_type") \
              .mode("overwrite") \
              .parquet(f"{base_output_path}/date={date}")


if __name__ == "__main__":
        main()