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
        licenses.createOrReplaceTempView("L")
        fares.createOrReplaceTempView("F")
        query = "SELECT F.*,L.name,L.type,L.current_status,L.DMV_license_plate,L.vehicle_VIN_number,L.vehicle_type,L.model_year,L.medallion_type,L.agent_number,
                 L.agent_name,L.agent_telephone_number,L.agent_website,L.agent_address,L.last_updated_date,L.last_updated_time
                 FROM F INNER JOIN L ON 
                 F.medallion=L.medallion
                 ORDER BY F.medallion,F.hack_license,F.pickup_datetime"
        table = spark.sql(query)
        table.write.save("task1b-sql.out",format="csv")



