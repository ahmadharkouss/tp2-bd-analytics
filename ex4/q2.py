from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

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


# Calculate the number of trips for each destination and select the top 4 destinations
top_destinations = data_do.groupBy("Zone").agg(count("*").alias("trip_count")) \
                          .orderBy(desc("trip_count")).limit(4)

# Show the top 4 destinations
print("Top 4 destinations:")
top_destinations.show()

# Calculate the total number of trips from the top 5 starting zones
top_starting_zones = data_pu.groupBy("Zone").agg(count("*").alias("trip_count")) \
                             .orderBy(desc("trip_count")).limit(5)

print("Top 5 starting zones:")
# Show the top 5 starting zones
top_starting_zones.show()

