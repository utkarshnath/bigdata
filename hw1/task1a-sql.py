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
        if len(sys.argv) != 3:
                print(sys.argv)
                print("Please enter 3 arguments")
                exit(-1)
        fares = spark.read.option("header","true").csv(sys.argv[2])
        trips = spark.read.option("header","true").csv(sys.argv[1])
        trips.createOrReplaceTempView("T")
        fares.createOrReplaceTempView("F")
        query = "SELECT T.medallion,T.hack_license,T.vendor_id,T.pickup_datetime,T.rate_code,T.store_and_fwd_flag,T.dropoff_datetime,T.passenger_count,
                 T.trip_time_in_secs,T.trip_distance,T.pickup_longitude,T.pickup_latitude,T.dropoff_longitude,T.dropoff_latitude,
                 F.payment_type,F.fare_amount,F.surcharge,F.mta_tax,F.tip_amount,F.tolls_amount,F.total_amount  
                 FROM T INNER JOIN F ON 
                 T.medallion=F.medallion and T.hack_license=F.hack_license and T.vendor_id=F.vendor_id and T.pickup_datetime=F.pickup_datetime
                 ORDER BY T.medallion,T.hack_license,T.pickup_datetime"
        table = spark.sql(query)
        table.write.save("task1a-sql.out",format="csv")



