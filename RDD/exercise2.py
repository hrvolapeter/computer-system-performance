from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import time

sc = SparkContext('local')
spark = SparkSession(sc)

def parseData(fields):
    # zero indexed, as usual :)
    #return (float(fields[13]), fields[14])
    try:
        money = float(fields[13])
    except:
        money = 0
    return (money, str(fields[14]))


#path = "chicago_taxi_trips_2016_*.csv"
#path = "/opt/chicago-taxi-rides-2016/chicago_taxi_trips_2016_*.csv" # path on the server

# Loading the path of the data from a text file
dataPath = "dataPath.txt"
path = str(sc.textFile("dataPath.txt").first())

time1 = time.time()

df = spark.read.option("header","true").csv(path)
myRdd = df.rdd

paymentData = myRdd.map(parseData).filter(lambda x: "Cash" in x[1])
cashonly = paymentData.map(lambda x: x[0])

time2 = time.time()

total = cashonly.sum()

time3 = time.time()

print("Total payments with cash:")
print(total)


# print("loading time: ")
# print(time2 - time1)
print("\r\n")
print("Calculation time (seconds): ")
print(time3-time2)
