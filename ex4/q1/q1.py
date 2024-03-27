from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ex4-q1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read data1 from parquet file
data1 = spark.read.option("header", "true").option("inferschema", "true").parquet("file:///home/ahmad/bd-analytics/data/yellow_tripdata_2022-01.parquet")

# Read data2 from a CSV file
data2 = spark.read.option("header", "true").option("inferschema", "true").csv("file:///home/ahmad/bd-analytics/data/taxi_zone_lookup.csv")

# Join data1 with data2 for PULocationID
data_pu = data1.join(data2.withColumnRenamed("LocationID", "PULocationID")
                    .withColumnRenamed("Borough", "PUBorough")
                    .withColumnRenamed("Zone", "PUZone")
                    .withColumnRenamed("service_zone", "PUservice_zone"),
                    "PULocationID")


# Join data1 with data2 for DOLocationID
data_final = data_pu.join(data2.withColumnRenamed("LocationID", "DOLocationID")
                    .withColumnRenamed("Borough", "DOBorough")
                    .withColumnRenamed("Zone", "DOZone")
                    .withColumnRenamed("service_zone", "DOservice_zone"),
                    "DOLocationID")

# Show the schema of the final joined data
data_final.printSchema()

# Show the final joined data
data_final.show()

