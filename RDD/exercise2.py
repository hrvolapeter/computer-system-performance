from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

import time

sc = SparkContext('local')
spark = SparkSession(sc)

#path = "chicago_taxi_trips_2016_*.csv"
path = "/opt/chicago-taxi-rides-2016/chicago_taxi_trips_2016_*.csv"
# schema = "data_dictionary.csv"


time1 = time.time()

def parseData(fields):
    # zero indexed, as usual :)
    #return (float(fields[13]), fields[14])
    try:
        money = float(fields[13])
    except:
        money = 0
    return (money, str(fields[14]))



df = spark.read.option("header","true").csv(path)
myRdd = df.rdd

paymentData = myRdd.map(parseData).filter(lambda x: "Cash" in x[1])
cashonly = paymentData.map(lambda x: x[0])

time2 = time.time()

cashonly.sum()

time3 = time.time()

print("loading time: ")
print(time2 - time1)
print("\r\n")
print("Calculation time: ")
print(time3-time2)
