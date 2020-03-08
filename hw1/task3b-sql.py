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
        query = "SELECT medallion,pickup_datetime FROM (SELECT medallion,pickup_datetime,Count(*) AS c  FROM alltrips GROUP BY medallion, pickup_datetime) WHERE c > 1"
        table = spark.sql(query)
        table.write.save("task3b-sql.out",format="csv")
