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
        query = "SELECT passenger_count as num_of_passengers,COUNT(*) as num_of_trips FROM alltrips GROUP BY passenger_count ORDER BY num_of_passengers"
        table = spark.sql(query)
        table.write.save("task2b-sql.out",format="csv")


~                                                                                                                                                                                    
~                                                           
