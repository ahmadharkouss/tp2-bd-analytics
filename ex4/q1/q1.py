from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("taxi-ex1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read data1 from parquet file
data1 = spark.read.option("header", "true").option("inferschema", "true").parquet("file:///home/ahmad/bd-analytics/data/yellow_tripdata_2022-01.parquet")

# Read data2 from a CSV file
data2 = spark.read.option("header", "true").option("inferschema", "true").csv("file:///home/ahmad/bd-analytics/data/taxi_zone_lookup.csv")



# Join data1 with data2 for PULocationID
data_pu = data1.join(data2, data1["PULocationID"] == data2["LocationID"])

# Join data1 with data2 for DOLocationID
data_do = data1.join(data2, data1["DOLocationID"] == data2["LocationID"])

# Show the schema of the joined data for pick-up
data_pu.printSchema()
data_pu.show()

# Show the schema of the joined data for drop-off
data_do.printSchema()
data_do.show()
