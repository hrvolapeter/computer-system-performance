from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import time

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#----------------------------DF creation START------------------------------
# time_load_start = time.time()

csv_file1 = "./data/chicago_taxi_trips_2016_01.csv"
csv_file2 = "./data/chicago_taxi_trips_2016_02.csv"
csv_file3 = "./data/chicago_taxi_trips_2016_03.csv"
csv_file4 = "./data/chicago_taxi_trips_2016_04.csv"
csv_file5 = "./data/chicago_taxi_trips_2016_05.csv"
csv_file6 = "./data/chicago_taxi_trips_2016_06.csv"
csv_file7 = "./data/chicago_taxi_trips_2016_07.csv"
csv_file8 = "./data/chicago_taxi_trips_2016_08.csv"
csv_file9 = "./data/chicago_taxi_trips_2016_09.csv"
csv_file10 = "./data/chicago_taxi_trips_2016_10.csv"
csv_file11 = "./data/chicago_taxi_trips_2016_11.csv"
csv_file12 = "./data/chicago_taxi_trips_2016_12.csv"

df1 = spark.read.format("csv").option("header", "True").load(csv_file1)
df2 = spark.read.format("csv").option("header", "True").load(csv_file2)
df3 = spark.read.format("csv").option("header", "True").load(csv_file3)
df4 = spark.read.format("csv").option("header", "True").load(csv_file4)
df5 = spark.read.format("csv").option("header", "True").load(csv_file5)
df6 = spark.read.format("csv").option("header", "True").load(csv_file6)
df7 = spark.read.format("csv").option("header", "True").load(csv_file7)
df8 = spark.read.format("csv").option("header", "True").load(csv_file8)
df9 = spark.read.format("csv").option("header", "True").load(csv_file9)
df10 = spark.read.format("csv").option("header", "True").load(csv_file10)
df11 = spark.read.format("csv").option("header", "True").load(csv_file11)
df12 = spark.read.format("csv").option("header", "True").load(csv_file12)

dfUnion = df1.union(df2).union(df3).union(df4).union(df5)                   \
          .union(df6).union(df7).union(df8).union(df9)                      \
          .union(df10).union(df11).union(df12)

# measuring loading time
# time_load_start = time.time()
# dfUnion.count()
# time_load_end = time.time()

dfUnion.createOrReplaceTempView("taxi")

# time_load_end = time.time()
#-----------------------------DF creation END-------------------------------

#-------------------------------Query START---------------------------------
time_cpu_start = time.time()

# showing results with MONTH NAME
# sqlDF = spark.sql(r"SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(trip_start_timestamp, '(-{1}\\d*\\s\\d{2}:\\d{2}:\\d{2})', ''), '2016-12', 'DECEMBER'), '2016-11', 'NOVEMBER'), '2016-10', 'OCTOBER'), '2016-9', 'SEPTEMBER'), '2016-8', 'AUGUST'), '2016-7', 'JULY'), '2016-6', 'JUNE'), '2016-5', 'MAY'), '2016-4', 'APRIL'), '2016-3', 'MARCH'), '2016-2', 'FEBRUARY'), '2016-1', 'JANUARY') AS month, SUM(trip_total) AS sum FROM taxi WHERE payment_type = 'Cash' AND trip_miles > 1.0 AND trip_seconds > 60 GROUP BY month ORDER BY month")

sqlDF = spark.sql(r"SELECT MONTH(REGEXP_REPLACE(trip_start_timestamp, '(\\s\\d{2}:\\d{2}:\\d{2})', '')) AS month, SUM(trip_total) AS sum FROM taxi WHERE payment_type = 'Cash' AND trip_miles > 1.0 AND trip_seconds > 60 GROUP BY month ORDER BY month")

sqlDF.show(12, truncate = False)

time_cpu_end = time.time()
#-------------------------------Query END-----------------------------------

# print("load time: ")
# print(time_load_end - time_load_start)

print("Query Time: ")
print(time_cpu_end - time_cpu_start)
print(" ")
