import sys
import pyspark
from pyspark.sql.functions import *
from csv import reader
from pyspark.sql import SparkSession

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("task1-sql.py") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def getInterval(amount):
    amount = float(amount)
    if amount>=0 and amount<=5: return "[0,5]"
    elif amount>5 and amount<=15: return "(5,15]"
    elif amount>15 and amount<=30: return "(15,30]"
    elif amount>30 and amount<=50: return "(30,50]"
    elif amount>50 and amount<=100: return "(50,100]"
    else return"[>100]"

# change column name, correct count
if __name__ == "__main__":
        if len(sys.argv) != 2:
                print(sys.argv)
                print("Please enter 2 arguments")
                exit(-1)
        allTrips = spark.read.option("header","true").csv(sys.argv[1])
        getIntervalUdf = udf(getInterval,StringType())
        allTrips = allTrips.withColumn("amount_range",getIntervalUdf("fare_amount")) 
        table = allTrips.groupby("amount_range").count()
        table.write.save("task2a.out",format="csv")




