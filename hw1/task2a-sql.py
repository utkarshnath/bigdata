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
        query = "SELECT CASE WHEN fare_amount >=0 AND fare_amount <=5 THEN '[0-5]' \
        WHEN fare_amount >5 AND fare_amount <=15 THEN '(5-15]'\
        WHEN fare_amount >15 AND fare_amount <=30 THEN '(15-30]'\
        WHEN fare_amount >30 AND fare_amount <=50 THEN '(30-50]'\
        WHEN fare_amount >50 AND fare_amount <=100 THEN '(50-100]'\
        ELSE '>100' END AS amount_range,count(*) AS num_trips from alltrips GROUP BY amount_range ORDER BY num_trips"
        table = spark.sql(query)
        table.write.save("task2a-sql.out",format="csv")



