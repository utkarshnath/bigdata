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
        alltrips = spark.read.option("header","true").csv(sys.argv[1])
        alltrips.createOrReplaceTempView("alltrips")
        query = "SELECT pickup_datetime as date,ROUND((total_fare_amount+total_surcharge+total_tip_amount),2)as total_revenue, ROUND(total_tolls,2) as total_tolls \
                 from (SELECT sum(fare_amount) as total_fare_amount, sum(surcharge) as total_surcharge,sum(tip_amount) as total_tip_amount, sum(tolls_amount) as total_tolls,\
                 pickup_datetime FROM alltrips GROUP BY pickup_datetime) ORDER BY date"
        table = spark.sql(query)
        table.write.save("task2c-sql.out",format="csv")


~
~
~                                                            
