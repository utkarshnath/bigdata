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
        df = alltrips.select(alltrips.pickup_datetime,alltrips.fare_amount.cast("float"),
             alltrips.surcharge.cast("float"),alltrips.tip_amount.cast("float"),alltrips.tolls_amount.cast("float"))
        df = df.groupBy('pickup_datetime').sum('fare_amount','surcharge','surcharge','tolls_amount').orderBy(['pickup_datetime'])
        df = df.select(df[0],df[1]+df[2]+df[3],df[4])
        df = df.withColumnRenamed('((fare_amount + surcharge) + tip_amount)','total_revenue')
               .withColumnRenamed('pickup_datetime','date')
               .withColumnRenamed('tolls_amount','total_tolls')
        # Round to 2 digit left
        df.write.save("task2c.out",format="csv")
