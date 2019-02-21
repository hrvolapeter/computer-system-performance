from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from datetime import datetime
from pyspark.sql.types import *
import time


sc = SparkContext('local[*]')
spark = SparkSession(sc)

schema = StructType([
    StructField("taxi_id", IntegerType()),
    StructField("trip_start_timestamp", TimestampType()),
    StructField("trip_end_timestamp", TimestampType()),
    StructField("trip_seconds", IntegerType()),
    StructField("trip_miles", FloatType()),
    StructField("pickup_census_tract", IntegerType()),
    StructField("dropoff_census_tract", IntegerType()),
    StructField("pickup_community_area", IntegerType()),
    StructField("dropoff_community_area", IntegerType()),
    StructField("fare", FloatType()),
    StructField("tips", FloatType()),
    StructField("tolls", FloatType()),
    StructField("extras", FloatType()),
    StructField("trip_total", FloatType()),
    StructField("payment_type", StringType()),
    StructField("company", IntegerType()),
    StructField("pickup_latitude", IntegerType()),
    StructField("pickup_longitude", IntegerType()),
    StructField("dropoff_latitude", IntegerType()),
    StructField("dropoff_longitude", IntegerType())
])

def parseData(fields):

    # trip_start = datetime.strptime("2016-1-13 06:15:00", '%Y-%m-%d %H:%M:%S')

    # try:
    #     trip_start = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")
    # except:
    #     trip_start = datetime.strptime("1900-1-1 00:00:00", "%Y-%m-%d %H:%M:%S")
    #     print("--------------PARSE ERROR-----------------")
    #     print(fields)
    #     print("------------------------------------------")

    # try:
    #     trip_end = datetime.strptime(fields[2], "%Y-%m-%d %H:%M:%S")
    # except:
    #     trip_end = datetime.strptime("1900-1-1 00:00:00", "%Y-%m-%d %H:%M:%S")
    #     print("--------------PARSE ERROR-----------------")
    #     print(fields)
    #     print("------------------------------------------")


    try:
        trip_start = fields[1]
    except:
        print("--------------PARSE ERROR-----------------")
        print(fields)
        print("------------------------------------------")
        trip_start = datetime.strptime("1900-1-1 00:00:00", "%Y-%m-%d %H:%M:%S")


    try:
        trip_end = fields[2]
    except:
        print("--------------PARSE ERROR-----------------")
        print(fields)
        print("------------------------------------------")
        trip_end = datetime.strptime("1900-1-1 00:00:00", "%Y-%m-%d %H:%M:%S")


    trip_start_month = fields[1].month
    trip_end_month = fields[2].month


    try:
        trip_seconds = int(fields[3])
    except:
        print("--------------PARSE ERROR-----------------")
        print(fields)
        print("------------------------------------------")
        trip_seconds = 0

    try:
        trip_miles = float(fields[4])
    except:
        trip_miles = 0

    try:
        trip_total = float(fields[13])
    except:
        trip_total = 0

    payment_type = str(fields[14])

    return (trip_end_month, trip_seconds, trip_miles, trip_total, payment_type)


#path = "chicago_taxi_trips_2016_*.csv"
#path = "/opt/chicago-taxi-rides-2016/chicago_taxi_trips_2016_*.csv" # path on the server

# Loading the path of the data from a text file
dataPath = "dataPath.txt"
path = str(sc.textFile("dataPath.txt").first())

time1 = time.time()

# df = spark.read.option("header", "true").csv(path)
df = (spark.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .schema(schema)
            .load(path)

        )


myRdd = df.rdd

paymentData = myRdd.map(parseData).filter(lambda x: x[1] > 60).filter(lambda y: y[2] > 1.0).filter(lambda z: "Cash" in z[4]).groupBy(lambda a: a[0])


# TODO: fucking finish it. it doesn't even get here, it just crashed on the previous step

print(paymentData.first())
# cashonly = paymentData.map(lambda x: x[0])

time2 = time.time()

# total = paymentData.sum()

time3 = time.time()

print("Total payments with cash:")
print(total)


# print("loading time: ")
# print(time2 - time1)
print("\r\n")
print("Calculation time (seconds): ")
print(time3-time2)
