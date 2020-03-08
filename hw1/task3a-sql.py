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

if __name__ == "__main__":
        if len(sys.argv) != 2:
                print(sys.argv)
                print("Please enter 2 arguments")
                exit(-1)
        allTrips = spark.read.option("header","true").csv(sys.argv[1])
        allTrips.createOrReplaceTempView("alltrips")
        query = "SELECT Count(*) FROM alltrips WHERE alltrips.fare_amount<0"
        table = spark.sql(query)
        table.write.save("task3a-sql.out",format="csv")
