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
        fares = spark.read.option("header","true").csv(sys.argv[1])
        licenses = spark.read.option("header","true").csv(sys.argv[2])
        table = fares.join(licenses,['medallion']).orderBy(['medallion', 'hack_license', 'pickup_datetime'])
        table.write.save("task1b.out",format="csv")


